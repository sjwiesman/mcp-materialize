"""
Materialize MCP Server

This script creates an MCP server that dynamically generates resource templates
based on the indexes in your Materialize cluster. For each index found in the
catalog, a resource is registered with a URI template that accepts parameters
for each indexed column. When a cligient later makes a lookup request (e.g. via
a URL like "materialize://customers/123"), the corresponding handler queries
the Materialize view with the provided parameters.
"""

import asyncio
import os
from dataclasses import dataclass
from typing import List

import psycopg
from psycopg.rows import dict_row
from mcp.server.fastmcp import FastMCP

# Materialize connection string
MZ_DSN = os.getenv("MZ_DSN", "postgres://materialize:materialize@localhost:6875/materialize")

# Create an MCP server instance with a descriptive name
mcp = FastMCP("Materialize MCP Server")

@dataclass
class IndexInfo:
    on: str
    keys: List[str]
    desc: str


async def get_indexes() -> List[IndexInfo]:
    async with await psycopg.AsyncConnection.connect(MZ_DSN, row_factory=dict_row) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
SELECT DISTINCT o.name AS on,
       array_agg(
         CASE
           WHEN ic.on_position IS NOT NULL THEN col.name
           ELSE ic.on_expression
         END
       ORDER BY ic.index_position) AS key,
       COALESCE(com.comment, o.name) AS description
FROM mz_indexes i
JOIN mz_clusters c ON i.cluster_id = c.id
JOIN mz_objects o ON i.on_id = o.id
JOIN mz_schemas s ON o.schema_id = s.id
JOIN mz_databases d ON s.database_id = d.id
JOIN mz_index_columns ic ON i.id = ic.index_id
JOIN mz_columns col ON o.id = col.id AND ic.on_position = col.position
LEFT JOIN mz_internal.mz_comments com ON com.id = o.id AND com.object_sub_id IS NULL
WHERE i.id LIKE 'u%'
  AND c.name = current_setting('cluster')
  AND s.name = current_schema()
  AND d.name = current_database()
GROUP BY o.name, com.comment
""")
            rows = await cur.fetchall()
    return [IndexInfo(on=row['on'], keys=row['key'], desc=row['description']) for row in rows]


def generate_tool_handler(view_name: str, index_columns: List[str]):
    param_list = ", ".join(index_columns)
    func_code = f"async def handler({param_list}):\n"
    func_code += "    conditions = []\n"
    func_code += "    values = []\n"
    for col in index_columns:
        func_code += f"    conditions.append(\"{col} = %s\")\n"
        func_code += f"    values.append({col})\n"
    func_code += f"    query = f\"SELECT * FROM {view_name} WHERE \" + \" AND \".join(conditions)\n"
    func_code += "    import psycopg\n"
    func_code += f"    async with await psycopg.AsyncConnection.connect('{MZ_DSN}') as conn:\n"
    func_code += "        async with conn.cursor() as cur:\n"
    func_code += "            await cur.execute(query, values)\n"
    func_code += "            rows = await cur.fetchall()\n"
    func_code += "    return str(rows)\n"
    local_vars = {}
    exec(func_code, globals(), local_vars)
    return local_vars["handler"]


async def register_tools():
    indexes = await get_indexes()
    for index in indexes:
        handler = generate_tool_handler(index.on, index.keys)
        mcp.tool(
            name=f"Lookup {index.on}",
            description=index.desc,
        )(handler)
        print(f"Registered tool: Lookup {index.on}")


async def main():
    await register_tools()
    await mcp.run_stdio_async()

if __name__ == "__main__":
    asyncio.run(main())