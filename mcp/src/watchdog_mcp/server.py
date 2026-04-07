"""Watchdog MCP Server — governance operations for platform admins.

Exposes Watchdog governance state (violations, policies, scans,
exceptions) as MCP tools over SSE. Uses on-behalf-of auth: each
request runs as the calling user's identity. UC grants on the
platform.watchdog schema govern who can query governance data.

For AI capabilities (FMAI, Vector Search, SQL), see ai-devkit-mcp.
"""

import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Request
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool

from watchdog_mcp.config import WatchdogMcpConfig
from watchdog_mcp.tools import governance

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("watchdog_mcp.audit")

config = WatchdogMcpConfig()

ALL_TOOLS: list[Tool] = governance.TOOLS

TOOL_HANDLERS: dict[str, Any] = {}
for tool in governance.TOOLS:
    TOOL_HANDLERS[tool.name] = governance

_session_clients: dict[str, WorkspaceClient] = {}
_session_users: dict[str, str] = {}


def _get_user_client(headers: dict[str, Any]) -> WorkspaceClient:
    """Create a WorkspaceClient with the caller's OAuth token."""
    auth_header = headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        return WorkspaceClient(host=config.host, token=auth_header[len("Bearer "):])

    user_token = headers.get("x-forwarded-access-token", "")
    if user_token:
        return WorkspaceClient(host=config.host, token=user_token)

    logger.debug("No user token — falling back to SP credentials")
    return WorkspaceClient()


def _get_user_identity(headers: dict[str, Any]) -> str:
    return (
        headers.get("x-forwarded-email")
        or headers.get("x-forwarded-user")
        or "anonymous"
    )


def _audit_log(event_type: str, user: str, **kwargs) -> None:
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user": user,
        **kwargs,
    }
    audit_logger.info(json.dumps(event, default=str))


def create_mcp_server(session_id: str) -> Server:
    server = Server(config.server_name)

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return ALL_TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        handler = TOOL_HANDLERS.get(name)
        if not handler:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

        w = _session_clients.get(session_id)
        if not w:
            return [TextContent(type="text", text="Session expired — reconnect")]

        user = _session_users.get(session_id, "unknown")
        args = arguments or {}
        start = time.monotonic()

        try:
            result = await handler.handle(name, args, w, config)
            duration_ms = int((time.monotonic() - start) * 1000)
            _audit_log("tool_invocation", user, tool=name,
                       arguments=args, duration_ms=duration_ms, success=True)
            return result
        except Exception as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            _audit_log("tool_invocation", user, tool=name,
                       arguments=args, duration_ms=duration_ms,
                       success=False, error=str(exc))

            if "PERMISSION_DENIED" in str(exc) or "ACCESS_DENIED" in str(exc):
                return [TextContent(
                    type="text",
                    text="Access denied: your UC permissions do not allow "
                    "querying the Watchdog governance tables. "
                    "Contact your platform admin.",
                )]
            raise

    return server


sse_transport = SseServerTransport("/mcp/messages/")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info(
        "Watchdog MCP v%s starting — on-behalf-of auth, schema=%s.%s",
        config.server_version, config.catalog, config.schema,
    )
    yield
    _session_clients.clear()
    _session_users.clear()
    logger.info("Watchdog MCP server shutting down")


app = FastAPI(
    title="Watchdog MCP",
    version=config.server_version,
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": config.server_version,
        "auth_model": "on-behalf-of",
        "tools": [t.name for t in ALL_TOOLS],
        "schema": f"{config.catalog}.{config.schema}",
        "active_sessions": len(_session_clients),
    }


@app.get("/mcp/sse")
async def sse_endpoint(request: Request):
    headers = dict(request.headers)
    user = _get_user_identity(headers)
    session_id = f"{user}-{id(request)}"

    w = _get_user_client(headers)
    _session_clients[session_id] = w
    _session_users[session_id] = user

    _audit_log("session_start", user, session_id=session_id)
    mcp_server = create_mcp_server(session_id)

    try:
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await mcp_server.run(
                streams[0], streams[1], mcp_server.create_initialization_options()
            )
    finally:
        _session_clients.pop(session_id, None)
        _session_users.pop(session_id, None)
        _audit_log("session_end", user, session_id=session_id)


@app.post("/mcp/messages/")
async def mcp_messages(request: Request):
    await sse_transport.handle_post_message(
        request.scope, request.receive, request._send
    )
