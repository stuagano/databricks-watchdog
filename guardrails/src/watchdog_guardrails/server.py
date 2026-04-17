"""AI Guardrails MCP Server.

Standalone MCP server exposing governance guardrails as tools over SSE.
Uses on-behalf-of auth: each request runs as the calling user's identity.
UC grants govern what metadata the user can see.

Connect from Claude Code or any MCP client:
    https://<app-url>/mcp/sse
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Any

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Request
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import TextContent, Tool

from watchdog_guardrails.audit import log_tool_call
from watchdog_guardrails.config import GuardrailsConfig
from watchdog_guardrails.tools import governance

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = GuardrailsConfig()
ALL_TOOLS: list[Tool] = governance.TOOLS

_session_clients: dict[str, WorkspaceClient] = {}
_session_users: dict[str, str] = {}


def _get_user_client(headers: dict[str, Any]) -> WorkspaceClient:
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
        w = _session_clients.get(session_id)
        user = _session_users.get(session_id, "anonymous")

        if w is None:
            return [TextContent(type="text", text="Error: no authenticated session")]

        start = time.monotonic()
        try:
            result = await governance.handle(name, arguments, w, config)
            log_tool_call(user=user, tool=name, arguments=arguments,
                          start_time=start, success=True)
            return result
        except Exception as e:
            log_tool_call(user=user, tool=name, arguments=arguments,
                          start_time=start, success=False, error=str(e))
            logger.exception(f"Tool {name} failed")
            return [TextContent(type="text", text=f"Error: {e}")]

    return server


sse = SseServerTransport("/mcp/messages/")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {config.server_name} v{config.server_version}")
    logger.info(f"Watchdog schema: {config.watchdog_schema}")
    logger.info(f"Tools: {len(ALL_TOOLS)}")
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="Watchdog Guardrails MCP",
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
        "tool_names": [t.name for t in ALL_TOOLS],
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
