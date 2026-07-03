FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update &&  \
    apt-get install -y protobuf-compiler && \
    apt-get install -y build-essential libssl-dev openssl pkg-config
COPY ./ /src/dshackle

WORKDIR /src/dshackle

RUN cargo fetch --verbose
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt install -y openssl ca-certificates

COPY --from=builder /src/dshackle/target/release/dshackle /opt/

# gRPC (2449), JSON RPC proxy (8545), and the internal access port (2448)
EXPOSE 2448 2449 8545

# dshackle looks up dshackle.yaml in the working dir; matches the `-v $(pwd):/etc/dshackle` run example
WORKDIR /etc/dshackle

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENTRYPOINT ["/opt/dshackle"]
