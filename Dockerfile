FROM rust:1 AS build
WORKDIR /app
# 의존성만 먼저 빌드해서 캐시 (코드만 바꾼 재배포가 빨라짐)
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src
COPY . .
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
COPY --from=build /app/target/release/voiceroom /usr/local/bin/voiceroom
CMD ["voiceroom"]