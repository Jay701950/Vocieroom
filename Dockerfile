FROM rust:1 AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /app/target/release/voiceroom /usr/local/bin/voiceroom
CMD ["voiceroom"]
