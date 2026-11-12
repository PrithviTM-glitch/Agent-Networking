"""A local message bus, resource ledger and admin console for Claude Code sessions, over MQTT 5."""


def __getattr__(name):
    """agent_bridge.__version__: the installed package's version, the one the wheel and its native binaries were built
    as (rust/Cargo.toml). Every command's --version and every version this package reports read it here. It is looked
    up on first use, so importing the package does not pay for importlib.metadata."""
    if name != "__version__":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib.metadata import PackageNotFoundError, version

    try:
        found = version("agent-bridge")
    except PackageNotFoundError:  # a source tree that was never installed
        found = "unknown"
    globals()["__version__"] = found
    return found
