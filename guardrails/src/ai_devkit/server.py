"""AI DevKit Guardrails MCP Server.

Standalone MCP server exposing governance guardrails as tools over SSE.
Uses on-behalf-of auth: each request runs as the calling user's identity.
UC grants govern what metadata the user can see.

Connect from Claude Code or any MCP client:
    https://<app-url>/mcp/sse
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

from ai_devkit.audit import audit_log
from ai_devkit.config import AiDevkitConfig
from ai_devkit.tools import governance

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = AiDevkitConfig()

ALL_TOOLS: list[Tool] = governance.TOOLS

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


def create_mcp_server(session_id: str) -> Server:
    server = Server(config.server_name)

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return ALL_TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        ws = _session_clients.get(session_id)
        user = _session_users.get(session_id, "anonymous")

        if ws is None:
            return [TextContent(type="text", text="Error: no authenticated session")]

        start = time.monotonic()
        try:
            result = await governance.handle(name, arguments, ws, config)
            elapsed = time.monotonic() - start

            audit_log(
                event_type="tool_call",
                user=user,
                tool=name,
                arguments=arguments,
                success=True,
                elapsed_ms=round(elapsed * 1000),
            )
            return result

        except Exception as e:
            elapsed = time.monotonic() - start
            audit_log(
                event_type="tool_call",
                user=user,
                tool=name,
                arguments=arguments,
                success=False,
                error=str(e),
                elapsed_ms=round(elapsed * 1000),
            )
            logger.exception(f"Tool {name} failed")
            return [TextContent(type="text", text=f"Error: {e}")]

    return server


# ── FastAPI app with SSE transport ────────────────────────────────────────────

sse = SseServerTransport("/mcp/messages/")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {config.server_name} v{config.server_version}")
    logger.info(f"Catalog: {config.catalog}, Watchdog: {config.watchdog_schema}")
    logger.info(f"Tools: {len(ALL_TOOLS)}")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="AI DevKit Guardrails MCP",
    version=config.server_version,
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "server": config.server_name,
        "version": config.server_version,
        "tools": len(ALL_TOOLS),
    }


@app.get("/mcp/sse")
async def sse_endpoint(request: Request):
    session_id = f"session-{id(request)}"
    headers = dict(request.headers)

    _session_clients[session_id] = _get_user_client(headers)
    _session_users[session_id] = _get_user_identity(headers)

    server = create_mcp_server(session_id)

    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await server.run(streams[0], streams[1], server.create_initialization_options())

    _session_clients.pop(session_id, None)
    _session_users.pop(session_id, None)


@app.post("/mcp/messages/")
async def sse_post(request: Request):
    await sse.handle_post_message(request.scope, request.receive, request._send)
