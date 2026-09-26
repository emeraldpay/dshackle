# syntax=docker/dockerfile:1
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update &&  \
    apt-get install -y protobuf-compiler && \
    apt-get install -y build-essential libssl-dev openssl pkg-config

WORKDIR /src/dshackle

# The sources (including .git, which the build script reads the commit from) are mounted only for the build, so they
# never become a layer. The mount is read-only, so the build goes to a cache dir, and the binary is copied out of it
# in the same step because a cache mount isn't a part of the image.
RUN --mount=type=bind,target=/src/dshackle \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    CARGO_TARGET_DIR=/build/target cargo build --release --locked && \
    cp /build/target/release/dshackle /opt/dshackle

FROM debian:bookworm-slim

RUN apt-get update && apt install -y openssl ca-certificates

COPY --from=builder /opt/dshackle /opt/

# gRPC (2449), JSON RPC proxy (8545), and the internal access port (2448)
EXPOSE 2448 2449 8545

# dshackle looks up dshackle.yaml in the working dir; matches the `-v $(pwd):/etc/dshackle` run example
WORKDIR /etc/dshackle

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENTRYPOINT ["/opt/dshackle"]
