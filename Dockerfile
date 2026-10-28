# The Python services (ledgerd, historyd): they need paho-mqtt and the package, not the MCP SDK.
FROM python:3.12.14-alpine3.23
RUN pip install --no-cache-dir paho-mqtt==2.1.0
COPY pyproject.toml README.md LICENSE /build/
COPY src /build/src
RUN pip install --no-cache-dir --no-deps /build && rm -rf /build
CMD ["agent-bridge-ledgerd"]
