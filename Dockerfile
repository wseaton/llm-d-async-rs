FROM docker.io/library/rust:1-bookworm AS builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
    && cargo build --release --locked && rm -rf src
COPY src src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
RUN install -d -o 65532 -g 65532 /var/lib/llm-d-async
COPY --from=builder /src/target/release/llm-d-async /usr/local/bin/llm-d-async
WORKDIR /var/lib/llm-d-async
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/llm-d-async"]
