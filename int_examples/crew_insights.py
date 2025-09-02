import argparse
import json
from typing import Dict, Any, Type

import pandas as pd

from crewai import Agent, Task, Crew
from crewai.tools import BaseTool
from pydantic import BaseModel, Field

from mcp_monkdb.mcp_server import run_select_query


def _monkdb_query_impl(sql: str) -> str:
    """Run SELECT via MCP and return JSON string."""
    res = run_select_query(sql)
    if isinstance(res, dict) and res.get("status") == "error":
        return json.dumps({"error": res["message"]})
    return json.dumps(res or [])


class MonkDBQueryInput(BaseModel):
    sql: str = Field(...,
                     description="SQL SELECT query to run against MonkDB via MCP")


class MonkDBQueryTool(BaseTool):
    name: str = "monkdb_query"
    description: str = "Run SELECT statements against MonkDB via MCP and return JSON rows."
    args_schema: Type[BaseModel] = MonkDBQueryInput

    def _run(self, sql: str) -> str:
        return _monkdb_query_impl(sql)


def build_where(filters: Dict[str, Any]) -> str:
    clauses = ["1=1"]
    brands = filters.get("brands")
    if brands:
        safe = ",".join("'" + str(b).replace("'", "''") + "'" for b in brands)
        clauses.append(f"brand IN ({safe})")
    if (m := filters.get("min_discount")) is not None:
        clauses.append(f"discount_percent >= {int(m)}")
    if (r := filters.get("min_rating")) is not None:
        clauses.append(f"rating >= {float(r)}")
    return " AND ".join(clauses)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="analytics_out/insights_pack.json")
    ap.add_argument("--filters-json", default="{}")
    args = ap.parse_args()

    filters = json.loads(args.filters_json)
    where = build_where(filters)

    # The LLM will ONLY be able to call monkdb_query_tool (SELECT-only via MCP).
    prompt = f"""
Using only SELECT queries against trent.products with this WHERE clause:
{where}

1) Return core KPIs (avg price/mrp/discount, no-discount items).
2) Brand concentration top 10 (brand, items, share%).
3) Discount bands distribution.
4) Top 10 discounted items with rating_total > 0.

Respond as EXACT JSON:
{{
  "kpis": {{...}},
  "tables": {{
    "brand_concentration": [...],
    "discount_bands": [...],
    "top_discounted_rated": [...]
  }},
  "bullets": [
    "insight sentence 1 with concrete numbers",
    "insight sentence 2 with concrete numbers",
    "insight sentence 3 with concrete numbers"
  ]
}}
No extra commentary outside JSON.
""".strip()

    # Create the tool instance
    monkdb_query_tool = MonkDBQueryTool()

    agent = Agent(
        role="Insights Analyst",
        goal="Produce crisp, data-backed insights from trent.products via MCP (SELECT-only).",
        backstory="Disciplined analyst that cites numbers; no writes.",
        tools=[monkdb_query_tool],
        allow_delegation=False,
        verbose=True,
    )

    task = Task(description=prompt, agent=agent, expected_output="JSON")
    crew = Crew(agents=[agent], tasks=[task], verbose=True)
    result = crew.kickoff()

    # Robustly get the model output string across CrewAI versions
    out_text = (
        getattr(result, "raw", None)
        or getattr(result, "output", None)
        or (result if isinstance(result, str) else str(result))
    )

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(out_text)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
