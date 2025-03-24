# Materialize mcp-server

Automatically creates MCP resources based on indexes in the database / schema / cluster configured on startup.

```bash
MZ_DSN=psql://user:password@host:6875/database uv run server.py
```