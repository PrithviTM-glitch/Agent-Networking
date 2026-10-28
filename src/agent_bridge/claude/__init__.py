"""Everything that knows Claude Code: its session variables and process, the session inbox socket, the session
registry and transcripts, Desktop detection, its hooks, its resume commands and its OpenTelemetry metric names.

The rest of the bridge reaches it only through agent_bridge.host, so a Claude Code change breaks this package and
nothing else. Claude Code's files and sockets are internal and undocumented: everything read here is parsed as
optional, checked before use, and skipped when malformed."""
