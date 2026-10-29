# ledgerd and historyd, each a static musl binary on an empty image. Build from this directory:
#   docker build --target ledgerd -t agent-bridge-ledgerd:rust .
#   docker build --target historyd -t agent-bridge-historyd:rust .
# Without --target it builds ledgerd, the last stage.
FROM rust:1.98.1-alpine3.23 AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p agent-bridge-ledgerd -p agent-bridge-historyd

FROM scratch AS historyd
COPY --from=build /src/target/release/agent-bridge-historyd /agent-bridge-historyd
CMD ["/agent-bridge-historyd"]

FROM scratch AS ledgerd
COPY --from=build /src/target/release/agent-bridge-ledgerd /agent-bridge-ledgerd
CMD ["/agent-bridge-ledgerd"]
