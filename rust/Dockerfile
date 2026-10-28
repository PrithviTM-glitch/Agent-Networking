# ledgerd only, as a static musl binary on an empty image. Build from this directory:
#   docker build -t agent-bridge-ledgerd:rust .
FROM rust:1.98.1-alpine3.23 AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p agent-bridge-ledgerd

FROM scratch
COPY --from=build /src/target/release/agent-bridge-ledgerd /agent-bridge-ledgerd
CMD ["/agent-bridge-ledgerd"]
