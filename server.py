#!/usr/bin/env python
"""
Materialize MCP Server

This script creates an MCP server that dynamically generates resource templates
based on the indexes in your Materialize cluster. For each index found in the
catalog, a resource is registered with a URI template that accepts parameters
for each indexed column. When a client later makes a lookup request (e.g. via
a URL like "materialize://customers/123"), the corresponding handler queries
the Materialize view with the provided parameters.
"""

import asyncio
import os
from dataclasses import dataclass
from typing import List

import asyncpg
from mcp.server.fastmcp import FastMCP

# Materialize connection string (DSN) is retrieved from the environment.
# If not set, it defaults to a local Materialize instance.
MZ_DSN = os.getenv("MZ_DSN", "postgres://materialize:materialize@localhost:6875/materialize")

# Create an MCP server instance with a descriptive name.
mcp = FastMCP("Materialize MCP Server")

@dataclass
class IndexInfo:
    on: str
    keys: List[str]

async def get_indexes() -> List[IndexInfo]:
    """
    Connects to the Materialize cluster using asyncpg and retrieves index metadata.

    It queries a system catalog table that contains:
      - on: the view/table name the index is built on.
      - key: a text[] of the index keys.

    Returns:
        A list of IndexInfo objects containing the index information.
    """
    conn = await asyncpg.connect(MZ_DSN)
    # Query the catalog for indexes. Adjust the query if your catalog differs.
    rows = await conn.fetch("SHOW INDEXES")
    await conn.close()

    indexes = [
        IndexInfo(on=row['on'], keys=row['key'])  # 'key' is auto-converted to List[str]
        for row in rows
    ]
    return indexes

def generate_lookup_handler(view_name: str, index_columns: List[str]):
    """
    Dynamically generates an asynchronous lookup handler with explicit parameters
    matching the index columns. This ensures that the MCP server sees a function whose
    signature exactly matches the URI template parameters.

    Args:
        view_name: The name of the view/table to query.
        index_columns: A list of column names that form the index.

    Returns:
        A coroutine function (handler) that performs the lookup query.
    """
    # Build a comma-separated string of parameters (e.g., "customer_id, order_id")
    param_list = ", ".join(index_columns)
    # Build the function code as a string.
    # This function will:
    #   - Build SQL conditions using the provided parameters.
    #   - Connect to Materialize, run the query, and return the results.
    func_code = f"async def handler({param_list}):\n"
    func_code += "    # Build SQL conditions and corresponding values\n"
    func_code += "    conditions = []\n"
    func_code += "    values = []\n"
    for i, col in enumerate(index_columns):
        func_code += f"    conditions.append(f\"{col} = ${{{i+1}}}\")\n"
        func_code += f"    values.append({col})\n"
    func_code += f"    query = f\"SELECT * FROM {view_name} WHERE \" + \" AND \".join(conditions)\n"
    func_code += "    import asyncpg\n"
    func_code += f"    conn = await asyncpg.connect('{MZ_DSN}')\n"
    func_code += "    rows = await conn.fetch(query, *values)\n"
    func_code += "    await conn.close()\n"
    func_code += "    return str(rows)\n"

    # print('Generated function code:', func_code)

    # Create a dictionary to hold the local variables from exec.
    local_vars = {}
    exec(func_code, globals(), local_vars)
    # Retrieve the dynamically created function.
    handler_func = local_vars["handler"]
    return handler_func

async def register_resources():
    """
    Dynamically registers MCP resource templates based on the indexes in Materialize.

    For each index:
      - A URI template is constructed (e.g., materialize://customers/{customer_id}).
      - A lookup handler is generated with explicit parameters matching the index keys.
      - The resource is registered on the MCP server.
    """
    indexes = await get_indexes()
    for index in indexes:
        # Construct the URI template.
        # For multiple columns, the template will look like: materialize://table/{col1}/{col2}
        template = f"materialize://{index.on}/" + "/".join(f"{{{col}}}" for col in index.keys)
        # Generate a lookup handler with a signature that matches the URI parameters.
        handler = generate_lookup_handler(index.on, index.keys)
        # Register the resource with the MCP server.
        mcp.resource(template)(handler)
        print(f"Registered resource template: {template}")

async def main():
    """
    Main entry point for the MCP server.

    It registers all dynamic resource templates by querying Materialize for index
    information and then starts the MCP server to begin accepting client requests.
    """
    await register_resources()
    await mcp.run_stdio_async()


# Run the main coroutine if this script is executed as the main module.
if __name__ == "__main__":
    asyncio.run(main())
