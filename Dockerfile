# The Python services (historyd, and ledgerd's Python rollback): the package and its Rust extension, without the MCP SDK.
FROM python:3.12.14-alpine3.23 AS build
RUN apk add --no-cache cargo patchelf && pip install --no-cache-dir maturin==1.15.0
COPY pyproject.toml README.md LICENSE /build/
COPY src /build/src
COPY rust /build/rust
RUN cd /build && maturin build --release --out /dist

FROM python:3.12.14-alpine3.23
RUN pip install --no-cache-dir paho-mqtt==2.1.0
COPY --from=build /dist /dist
RUN pip install --no-cache-dir --no-deps /dist/*.whl && rm -rf /dist
CMD ["agent-bridge-ledgerd"]
