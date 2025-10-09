# app/dev_server.py
# Start MCP server in STDIO mode for development and testing

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.mcp_server import mcp

if __name__ == "__main__":
    mcp.run()