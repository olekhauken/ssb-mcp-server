import asyncio
import httpx
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
import uvicorn
import itertools
import os
from urllib.parse import urlencode

SSB_BASE = "https://data.ssb.no/api/pxwebapi/v2"

app = Server("ssb-mcp-server")


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="search_ssb",
            description="Søk etter statistikktabeller hos SSB. Bruk norske søkeord.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Søkeord på norsk, f.eks. 'boligpriser Oslo' eller 'arbeidsledighet'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Antall resultater (standard: 10)",
                        "default": 10
                    },
                    "language": {
                        "type": "string",
                        "description": "Språk: 'no' (norsk, standard) eller 'en' (engelsk)",
                        "enum": ["no", "en"],
                        "default": "no"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_table_metadata",
            description="Hent metadata og variabler for en SSB-tabell. Inkluderer kontaktperson, oppdateringstidspunkt og kodelister for aggregering.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "SSB tabell-ID, f.eks. '07459'"
                    },
                    "language": {
                        "type": "string",
                        "description": "Språk: 'no' (norsk, standard) eller 'en' (engelsk)",
                        "enum": ["no", "en"],
                        "default": "no"
                    }
                },
                "required": ["table_id"]
            }
        ),
        Tool(
            name="query_ssb_table",
            description=(
                "Hent data fra en SSB-tabell. Bruk get_table_metadata først for å se tilgjengelige variabler og koder. "
                "Filterverdier: '*' for alle, 'top(N)' for N siste perioder, 'from(YYYY)' for fra og med år, "
                "eller spesifikke koder som ['0301', '1103']."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "SSB tabell-ID"
                    },
                    "filters": {
                        "type": "object",
                        "description": (
                            "Filtre som dict. Eksempler: "
                            "{'Region': '*', 'Tid': 'top(5)'} "
                            "{'Tid': 'from(2020)'} "
                            "{'Region': ['0301', '1103']}"
                        )
                    },
                    "language": {
                        "type": "string",
                        "description": "Språk: 'no' (norsk, standard) eller 'en' (engelsk)",
                        "enum": ["no", "en"],
                        "default": "no"
                    }
                },
                "required": ["table_id", "filters"]
            }
        ),
        Tool(
            name="get_ssb_url",
            description=(
                "Generer en delbar URL til SSB-data. Nyttig for å åpne data i nettleser eller dele med andre. "
                "Støtter csv, xlsx og json-stat2 som format."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "string",
                        "description": "SSB tabell-ID, f.eks. '07221'"
                    },
                    "filters": {
                        "type": "object",
                        "description": "Samme filterformat som query_ssb_table"
                    },
                    "format": {
                        "type": "string",
                        "description": "Utdataformat: 'csv' (standard), 'xlsx' eller 'json-stat2'",
                        "enum": ["csv", "xlsx", "json-stat2"],
                        "default": "csv"
                    },
                    "language": {
                        "type": "string",
                        "description": "Språk: 'no' (norsk, standard) eller 'en' (engelsk)",
                        "enum": ["no", "en"],
                        "default": "no"
                    }
                },
                "required": ["table_id"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    async with httpx.AsyncClient(timeout=30) as client:

        if name == "search_ssb":
            query = arguments["query"]
            limit = arguments.get("limit", 10)
            language = arguments.get("language", "no")
            url = f"{SSB_BASE}/tables"
            params = {"query": query, "lang": language, "pagesize": limit}
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            tables = data.get("tables", [])
            if not tables:
                return [TextContent(type="text", text=f"Ingen tabeller funnet for '{query}'.")]
            lines = [f"Fant {len(tables)} tabell(er) for '{query}':\n"]
            for t in tables:
                table_id = t.get("id", "")
                label = t.get("label", t.get("title", "Uten tittel"))
                updated = t.get("updated", t.get("lastUpdated", ""))
                lines.append(f"- ID: {table_id} | {label}")
                if updated:
                    lines.append(f"  Oppdatert: {updated[:10]}")
            return [TextContent(type="text", text="\n".join(lines))]

        elif name == "get_table_metadata":
            table_id = arguments["table_id"]
            language = arguments.get("language", "no")
            url = f"{SSB_BASE}/tables/{table_id}/metadata"
            params = {"lang": language}
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            title = data.get("label", data.get("title", table_id))
            updated = data.get("updated", "")
            source = data.get("source", "")
            contact = data.get("contact", {})

            lines = [f"Tabell: {table_id} – {title}\n"]

            if updated:
                lines.append(f"Oppdatert: {updated[:10]}")
            if source:
                lines.append(f"Kilde: {source}")
            if contact:
                name_c = contact.get("name", "")
                phone = contact.get("phone", "")
                email = contact.get("email", "")
                contact_parts = [p for p in [name_c, phone, email] if p]
                if contact_parts:
                    lines.append(f"Kontakt: {', '.join(contact_parts)}")
            lines.append("")

            dimension = data.get("dimension", {})
            dim_ids = data.get("id", [])

            for dim_id in dim_ids:
                dim = dimension.get(dim_id, {})
                dim_label = dim.get("label", dim_id)
                category = dim.get("category", {})
                index = category.get("index", {})
                labels = category.get("label", {})
                codelists = dim.get("codelists", [])

                lines.append(f"Variabel: {dim_id} ({dim_label})")
                codes = list(index.keys()) if isinstance(index, dict) else list(labels.keys())
                shown = codes[:8]
                for code in shown:
                    label = labels.get(code, code)
                    lines.append(f"  {code}: {label}")
                if len(codes) > 8:
                    lines.append(f"  ... og {len(codes) - 8} til")
                if codelists:
                    lines.append(f"  Kodelister (aggregering): {', '.join(codelists)}")
                lines.append("")

            return [TextContent(type="text", text="\n".join(lines))]

        elif name == "query_ssb_table":
            table_id = arguments["table_id"]
            filters = arguments["filters"]
            language = arguments.get("language", "no")
            params = {"lang": language, "outputFormat": "json-stat2"}
            for key, val in filters.items():
                params[f"valueCodes[{key}]"] = val
            url = f"{SSB_BASE}/tables/{table_id}/data"
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            dims = data.get("id", [])
            sizes = data.get("size", [])
            dim_info = data.get("dimension", {})
            values = data.get("value", [])

            lines = [f"Data fra tabell {table_id} – {data.get('label', '')}:\n"]

            dim_labels = {}
            for dim in dims:
                cats = dim_info.get(dim, {}).get("category", {})
                dim_labels[dim] = cats.get("label", {})

            if len(values) <= 500:
                ranges = [range(s) for s in sizes]
                for i, combo in enumerate(itertools.product(*ranges)):
                    row_parts = []
                    for d, idx in zip(dims, combo):
                        codes = list(dim_labels[d].keys())
                        label = dim_labels[d].get(codes[idx], str(idx)) if idx < len(codes) else str(idx)
                        row_parts.append(f"{d}: {label}")
                    val = values[i] if i < len(values) else None
                    lines.append(f"{' | '.join(row_parts)} → {val}")
            else:
                lines.append(f"(Datasettet har {len(values)} verdier – viser sammendrag)")
                lines.append(f"Dimensjoner: {', '.join(dims)}")
                lines.append(f"Størrelser: {sizes}")
                lines.append(f"Første 20 verdier: {values[:20]}")

            return [TextContent(type="text", text="\n".join(lines))]

        elif name == "get_ssb_url":
            table_id = arguments["table_id"]
            filters = arguments.get("filters", {})
            fmt = arguments.get("format", "csv")
            language = arguments.get("language", "no")

            base_url = f"{SSB_BASE}/tables/{table_id}/data"
            params = {"lang": language, "outputFormat": fmt}
            for key, val in filters.items():
                params[f"selection[{key}]"] = val

            full_url = f"{base_url}?{urlencode(params)}"
            return [TextContent(type="text", text=f"URL til SSB-data ({fmt}):\n{full_url}")]

        return [TextContent(type="text", text=f"Ukjent verktøy: {name}")]


def create_app():
    sse = SseServerTransport("/messages/")

    async def handle_sse(request: Request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await app.run(
                streams[0], streams[1], app.create_initialization_options()
            )

    starlette_app = Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages/", app=sse.handle_post_message),
        ]
    )
    return starlette_app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(create_app(), host="0.0.0.0", port=port)
