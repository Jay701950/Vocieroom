use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::{Html, Response},
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use uuid::Uuid;

struct Peer {
    name: String,
    avatar: Option<String>,
    tx: UnboundedSender<Message>,
}
struct Room {
    name: String,
    max: usize,
    peers: HashMap<String, Peer>,
}
type Rooms = Arc<Mutex<HashMap<String, Room>>>;

fn short() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

async fn list_rooms(State(rooms): State<Rooms>) -> Json<Value> {
    let rooms = rooms.lock().unwrap();
    Json(Value::Array(
        rooms
            .iter()
            .map(|(id, r)| json!({"id": id, "name": r.name, "max": r.max, "n": r.peers.len()}))
            .collect(),
    ))
}

async fn add_room(State(rooms): State<Rooms>, Json(b): Json<Value>) -> Result<Json<Value>, StatusCode> {
    let name: String = b["name"].as_str().unwrap_or("").trim().chars().take(20).collect();
    let max = b["max"].as_u64().unwrap_or(4).clamp(2, 8) as usize;
    if name.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut rooms = rooms.lock().unwrap();
    if rooms.len() >= 50 {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    let id = short();
    rooms.insert(id.clone(), Room { name, max, peers: HashMap::new() });
    Ok(Json(json!({"id": id})))
}

#[derive(Deserialize)]
struct Q {
    room: String,
}

async fn ws_handler(State(rooms): State<Rooms>, Query(q): Query<Q>, up: WebSocketUpgrade) -> Response {
    up.on_upgrade(move |sock| run(rooms, sock, q.room))
}

async fn run(rooms: Rooms, sock: WebSocket, room: String) {
    let (mut sink, mut stream) = sock.split();
    let (tx, mut rx) = unbounded_channel::<Message>();
    let id = short();

    // 첫 메시지 = 클라이언트가 보내는 {name, avatar} (로그인/프사는 Firebase에서 가져온 값)
    let hello = loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => break serde_json::from_str::<Value>(&t).unwrap_or_default(),
            Some(Ok(_)) => continue,
            _ => return,
        }
    };
    let name: String = hello["name"].as_str().unwrap_or("").trim().chars().take(12).collect();
    let name = if name.is_empty() { "손님".to_string() } else { name };
    let avatar: Option<String> = hello["avatar"]
        .as_str()
        .filter(|a| {
            a.len() <= 100_000
                && a.starts_with("data:image/")
                && a.chars().all(|c| c.is_ascii_alphanumeric() || "+/=:;,".contains(c))
        })
        .map(String::from);

    let joined: Result<Vec<Value>, &str> = {
        let mut rs = rooms.lock().unwrap();
        match rs.get_mut(&room) {
            None => Err("없는 방이야"),
            Some(r) if r.peers.len() >= r.max => Err("방이 꽉 찼어"),
            Some(r) => {
                let peers = r
                    .peers
                    .iter()
                    .map(|(i, p)| json!({"id": i, "name": p.name, "avatar": p.avatar}))
                    .collect();
                let m = json!({"t": "join", "id": id, "name": name, "avatar": avatar}).to_string();
                for p in r.peers.values() {
                    let _ = p.tx.send(Message::Text(m.clone()));
                }
                r.peers.insert(
                    id.clone(),
                    Peer { name: name.clone(), avatar: avatar.clone(), tx: tx.clone() },
                );
                Ok(peers)
            }
        }
    };
    let peers = match joined {
        Ok(p) => p,
        Err(m) => {
            let _ = sink.send(Message::Text(json!({"t": "err", "m": m}).to_string())).await;
            return;
        }
    };
    let _ = tx.send(Message::Text(
        json!({"t": "welcome", "id": id, "name": name, "avatar": avatar, "peers": peers}).to_string(),
    ));

    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(m).await.is_err() {
                break;
            }
        }
    });

    // 시그널(offer/answer/ice)을 대상 사람에게 그대로 전달
    while let Some(Ok(m)) = stream.next().await {
        match m {
            Message::Text(t) => {
                let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                let out = json!({"t": "sig", "from": id, "data": v["data"]}).to_string();
                let to = v["to"].as_str().unwrap_or("");
                if let Some(p) = rooms.lock().unwrap().get(&room).and_then(|r| r.peers.get(to)) {
                    let _ = p.tx.send(Message::Text(out));
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    writer.abort();
    if let Some(r) = rooms.lock().unwrap().get_mut(&room) {
        r.peers.remove(&id);
        let m = json!({"t": "leave", "id": id}).to_string();
        for p in r.peers.values() {
            let _ = p.tx.send(Message::Text(m.clone()));
        }
    }
}

#[tokio::main]
async fn main() {
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        .route("/api/rooms", get(list_rooms).post(add_room))
        .route("/ws", get(ws_handler))
        .with_state(rooms);

    let port = std::env::var("PORT").unwrap_or("3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
