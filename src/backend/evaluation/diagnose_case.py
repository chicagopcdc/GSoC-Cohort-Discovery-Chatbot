"""Print one query-builder run with raw model output and validation details."""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path


def _bootstrap() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "backend").is_dir():
            sys.path.insert(0, str(parent / "backend"))
            return parent
    raise SystemExit("could not locate the src/backend package")


_SRC = _bootstrap()
_REPO = _SRC.parent

from services.query_builder_v2 import QueryBuilder


def _find_schema() -> tuple[str, str]:
    d = _REPO / "schema"
    pcdc = os.getenv("PCDC_SCHEMA_PATH") or sorted(glob.glob(str(d / "pcdc-schema-prod-*.json")))[-1]
    gitops = os.getenv("GITOPS_PATH", str(d / "gitops.json"))
    return pcdc, gitops


def _data(name: str):
    p = _SRC / "backend" / "data" / name
    return str(p) if p.exists() else None


def main() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("set OPENAI_API_KEY first")

    query = " ".join(sys.argv[1:]) or "Find male patients from the INRG consortium."
    pcdc, gitops = _find_schema()

    qb = QueryBuilder.from_files(
        pcdc, gitops,
        synonyms_path=_data("synonyms.yaml"),
        numeric_config_path=_data("numeric_fields.yaml"),
    )

    print(f"model: {os.getenv('FILTER_GENERATION_MODEL', '(default)')}")
    print(f"query: {query}\n")

    build = qb.build(query)
    gen = build.generation

    print(f"build.ok:           {build.ok}")
    print(f"build.wire:         {build.wire}")
    print(f"build.errors:       {list(build.errors)}")
    print(f"gen.attempts:       {gen.attempts}")
    print(f"gen.strict_downgraded: {getattr(gen, 'strict_downgraded', '?')}")
    print(f"gen.validation:     {[(i.code, i.message) for i in gen.validation.issues]}")
    print("\n--- raw model outputs (verbatim, per attempt) ---")
    for i, raw in enumerate(gen.raw_outputs, 1):
        print(f"[attempt {i}] {raw!r}")


if __name__ == "__main__":
    main()
