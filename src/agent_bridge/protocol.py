"""Presence record field names shared outside the native core, which owns the wire format itself (reserved topics,
presence records and message payloads, parsed as hostile input).
"""

ORIGIN = ("session", "pid", "pid_start")  # write-once: the agent's session and its process, see docs/architecture.md
LINEAGE = ("kind", "parent")  # a subagent's agent: kind "subagent" and the creating session, never its own session
