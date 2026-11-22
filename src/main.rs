use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{Html, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use uuid::Uuid;

struct Peer {
    name: String,
    avatar: Option<String>,
    tx: UnboundedSender<Message>,
    muted: bool,
    forced: bool,
    sharing: bool,
    share_off: bool,
    cid: String,
}
struct Room {
    name: String,
    max: usize,
    peers: HashMap<String, Peer>,
    chat: Vec<Value>,
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
    rooms.insert(id.clone(), Room { name, max, peers: HashMap::new(), chat: Vec::new() });
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

    let cid: String = hello["cid"].as_str().unwrap_or("").chars().take(40).collect();
    let joined: Result<(Vec<Value>, Vec<Value>), &str> = {
        let mut rs = rooms.lock().unwrap();
        // 같은 브라우저(cid)의 이전 접속이 남아 있으면 즉시 정리 → 나갔다 바로 다시 들어와도 OK
        if let Some(r) = rs.get_mut(&room) {
            if !cid.is_empty() {
                let old: Vec<String> = r.peers.iter().filter(|(_, p)| p.cid == cid).map(|(i, _)| i.clone()).collect();
                for oid in old {
                    if let Some(p) = r.peers.remove(&oid) {
                        let _ = p.tx.send(Message::Close(None));
                        broadcast(r, &json!({"t": "leave", "id": oid}).to_string());
                    }
                }
            }
        }
        match rs.get_mut(&room) {
            None => Err("없는 방이야"),
            Some(r) if r.peers.values().any(|p| p.name.to_lowercase() == name.to_lowercase()) => {
                Err("같은 이름이 이미 방에 있어요")
            }
            Some(r) if r.peers.len() >= r.max => Err("방이 꽉 찼어"),
            Some(r) => {
                let peers = r
                    .peers
                    .iter()
                    .map(|(i, p)| json!({"id": i, "name": p.name, "avatar": p.avatar, "muted": p.muted, "sharing": p.sharing, "shareOff": p.share_off}))
                    .collect();
                let m = json!({"t": "join", "id": id, "name": name, "avatar": avatar}).to_string();
                for p in r.peers.values() {
                    let _ = p.tx.send(Message::Text(m.clone()));
                }
                r.peers.insert(
                    id.clone(),
                    Peer { name: name.clone(), avatar: avatar.clone(), tx: tx.clone(), muted: false, forced: false, sharing: false, share_off: false, cid: cid.clone() },
                );
                Ok((peers, r.chat.clone()))
            }
        }
    };
    let (peers, chat) = match joined {
        Ok(p) => p,
        Err(m) => {
            let _ = sink.send(Message::Text(json!({"t": "err", "m": m}).to_string())).await;
            return;
        }
    };
    let _ = tx.send(Message::Text(
        json!({"t": "welcome", "id": id, "name": name, "avatar": avatar, "peers": peers, "chat": chat}).to_string(),
    ));

    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(m).await.is_err() {
                break;
            }
        }
    });

    let mut last_chat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap_or_else(Instant::now);
    // 시그널(offer/answer/ice)을 대상 사람에게 그대로 전달
    loop {
        // 60초 동안 아무 신호도 없으면(클라이언트는 25초마다 ping) 끊긴 것으로 보고 정리
        let m = match tokio::time::timeout(Duration::from_secs(60), stream.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        match m {
            Message::Text(t) => {
                let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                // 채팅: 서버 메모리에만 최근 100개 보관, 도배 방지(0.35초)
                if let Some(txt) = v["chat"].as_str() {
                    let txt: String = txt.trim().chars().take(600).collect();
                    if txt.is_empty() || last_chat.elapsed() < Duration::from_millis(350) {
                        continue;
                    }
                    last_chat = Instant::now();
                    if let Some(r) = rooms.lock().unwrap().get_mut(&room) {
                        let who = r.peers.get(&id).map(|p| p.name.clone()).unwrap_or_default();
                        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                        let m = json!({"t": "chat", "id": id, "name": who, "text": txt, "ts": ts});
                        r.chat.push(m.clone());
                        if r.chat.len() > 100 {
                            r.chat.remove(0);
                        }
                        broadcast(r, &m.to_string());
                    }
                    continue;
                }
                // 내가 음소거/해제하면 방 사람들에게 상태 전달
                if let Some(mu) = v["muted"].as_bool() {
                    if let Some(r) = rooms.lock().unwrap().get_mut(&room) {
                        if let Some(p) = r.peers.get_mut(&id) {
                            p.muted = mu;
                        }
                        if let Some(p) = r.peers.get(&id) {
                            let m = state_msg(&id, p);
                            broadcast(r, &m);
                        }
                    }
                    continue;
                }
                // 화면 공유 시작/종료 상태 전달
                if let Some(sh) = v["share"].as_bool() {
                    if let Some(r) = rooms.lock().unwrap().get_mut(&room) {
                        if let Some(p) = r.peers.get_mut(&id) {
                            p.sharing = sh;
                            if !sh {
                                p.share_off = false;
                            }
                        }
                        if let Some(p) = r.peers.get(&id) {
                            let m = state_msg(&id, p);
                            broadcast(r, &m);
                        }
                    }
                    continue;
                }
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
        if r.peers.is_empty() {
            r.chat.clear(); // 방이 비면 채팅 기록 삭제
        }
        let m = json!({"t": "leave", "id": id}).to_string();
        for p in r.peers.values() {
            let _ = p.tx.send(Message::Text(m.clone()));
        }
    }
}

// ---- 관리자 (환경변수 ADMIN_PASSWORD 가 설정돼 있어야 동작) ----
fn is_admin(h: &HeaderMap) -> bool {
    match std::env::var("ADMIN_PASSWORD") {
        Ok(p) if !p.is_empty() => h.get("x-admin").and_then(|v| v.to_str().ok()) == Some(p.as_str()),
        _ => false,
    }
}

fn kick_msg(p: &Peer, m: &str) {
    let _ = p.tx.send(Message::Text(json!({"t": "err", "m": m}).to_string()));
    let _ = p.tx.send(Message::Close(None));
}

async fn admin_rooms(State(rooms): State<Rooms>, h: HeaderMap) -> Result<Json<Value>, StatusCode> {
    if !is_admin(&h) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let rooms = rooms.lock().unwrap();
    let list: Vec<Value> = rooms
        .iter()
        .map(|(id, r)| {
            let peers: Vec<Value> = r.peers.iter().map(|(i, p)| json!({"id": i, "name": p.name, "muted": p.muted, "sharing": p.sharing, "shareOff": p.share_off})).collect();
            json!({"id": id, "name": r.name, "max": r.max, "peers": peers})
        })
        .collect();
    Ok(Json(Value::Array(list)))
}

async fn admin_kick(State(rooms): State<Rooms>, h: HeaderMap, Json(b): Json<Value>) -> StatusCode {
    if !is_admin(&h) {
        return StatusCode::UNAUTHORIZED;
    }
    let room = b["room"].as_str().unwrap_or("");
    let peer = b["peer"].as_str().unwrap_or("");
    if let Some(p) = rooms.lock().unwrap().get(room).and_then(|r| r.peers.get(peer)) {
        kick_msg(p, "관리자에 의해 퇴장됐어요");
    }
    StatusCode::OK
}

async fn admin_delete(State(rooms): State<Rooms>, h: HeaderMap, Json(b): Json<Value>) -> StatusCode {
    if !is_admin(&h) {
        return StatusCode::UNAUTHORIZED;
    }
    if let Some(r) = rooms.lock().unwrap().remove(b["room"].as_str().unwrap_or("")) {
        for p in r.peers.values() {
            kick_msg(p, "방이 삭제됐어요");
        }
    }
    StatusCode::OK
}

fn broadcast(r: &Room, m: &str) {
    for p in r.peers.values() {
        let _ = p.tx.send(Message::Text(m.to_string()));
    }
}

fn state_msg(id: &str, p: &Peer) -> String {
    json!({"t": "state", "id": id, "muted": p.muted, "sharing": p.sharing, "shareOff": p.share_off}).to_string()
}

// 관리자: 음소거 (당사자가 직접 다시 켤 수 있음)
async fn admin_mute(State(rooms): State<Rooms>, h: HeaderMap, Json(b): Json<Value>) -> StatusCode {
    if !is_admin(&h) {
        return StatusCode::UNAUTHORIZED;
    }
    let room = b["room"].as_str().unwrap_or("");
    let peer = b["peer"].as_str().unwrap_or("");
    if let Some(r) = rooms.lock().unwrap().get_mut(room) {
        if let Some(p) = r.peers.get_mut(peer) {
            p.muted = true;
        }
        if let Some(p) = r.peers.get(peer) {
            let mut v: Value = serde_json::from_str(&state_msg(peer, p)).unwrap_or_default();
            v["admin"] = json!(true);
            broadcast(r, &v.to_string());
        }
    }
    StatusCode::OK
}

// 관리자: 화면 공유 끄기(stop) / 공유 소리만 끄기(audio_off) / 다시 켜기(audio_on)
async fn admin_share(State(rooms): State<Rooms>, h: HeaderMap, Json(b): Json<Value>) -> StatusCode {
    if !is_admin(&h) {
        return StatusCode::UNAUTHORIZED;
    }
    let cmd = b["cmd"].as_str().unwrap_or("");
    let room = b["room"].as_str().unwrap_or("");
    let peer = b["peer"].as_str().unwrap_or("");
    if let Some(r) = rooms.lock().unwrap().get_mut(room) {
        if let Some(p) = r.peers.get_mut(peer) {
            match cmd {
                "audio_off" => p.share_off = true,
                "audio_on" => p.share_off = false,
                "stop" => {
                    p.sharing = false;
                    p.share_off = false;
                }
                _ => {}
            }
            let _ = p.tx.send(Message::Text(json!({"t": "adm", "cmd": cmd}).to_string()));
        }
        if let Some(p) = r.peers.get(peer) {
            let m = state_msg(peer, p);
            broadcast(r, &m);
        }
    }
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));
    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        .route("/api/rooms", get(list_rooms).post(add_room))
        .route("/ws", get(ws_handler))
        .route("/admin", get(|| async { Html(include_str!("../static/admin.html")) }))
        .route("/api/admin/rooms", get(admin_rooms))
        .route("/api/admin/kick", post(admin_kick))
        .route("/api/admin/delete", post(admin_delete))
        .route("/api/admin/mute", post(admin_mute))
        .route("/api/admin/share", post(admin_share))
        .with_state(rooms);

    let port = std::env::var("PORT").unwrap_or("3000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
