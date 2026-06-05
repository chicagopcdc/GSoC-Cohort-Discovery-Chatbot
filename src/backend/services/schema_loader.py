"""
Schema index for PCDC/Guppy filters.

Loads the PCDC schema and gitops filter config, then exposes field, enum,
type, and nested-path lookups.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Optional, Union


FieldType = Literal["enum", "number", "string", "boolean", "unknown"]


@dataclass(frozen=True)
class FieldSpec:
    """Metadata for one filterable field."""

    name: str
    field_type: FieldType
    enum_values: tuple[str, ...] = ()
    description: str = ""
    parent_path: Optional[str] = None


_TOP_LEVEL_YAMLS = ("subject", "person")

# Default dictionary release. Bump when adopting a new schema; verified
# backward-compatible through pcdc-schema-prod-20260414 (additive changes only).
DEFAULT_PCDC_SCHEMA = "pcdc-schema-prod-20260414.json"
DEFAULT_GITOPS = "gitops.json"


_OverrideValue = Union[tuple[str, str], tuple[str, FieldSpec]]

# Fields that gitops/Guppy exposes differently from the PCDC schema.
_OVERRIDES: dict[tuple[Optional[str], str], _OverrideValue] = {
    # Defined in timing.yaml, but exposed as top-level in gitops.
    (None, "year_at_disease_phase"): ("redirect", "timing"),
    # Anchor field in gitops; same timing.yaml origin, flattened to top-level.
    (None, "disease_phase"): ("redirect", "timing"),

    # Flattened Guppy field.
    (None, "subject_submitter_id"): (
        "inline",
        FieldSpec(
            name="subject_submitter_id",
            field_type="string",
            parent_path=None,
            description=(
                "Subject identifier exposed by Guppy's index "
                "(flattened from subject.submitter_id)."
            ),
        ),
    ),

    # ETL-derived Guppy field.
    ("survival_characteristics", "lkss_obfuscated"): (
        "inline",
        FieldSpec(
            name="lkss_obfuscated",
            field_type="string",
            parent_path="survival_characteristics",
            description=(
                "Privacy-obfuscated last-known survival status. "
                "Guppy-derived field; not in PCDC schema."
            ),
        ),
    ),

    # Guppy availability flag; no schema source.
    (None, "biospecimen_status"): (
        "inline",
        FieldSpec(
            name="biospecimen_status",
            field_type="string",
            parent_path=None,
            description=(
                "Whether a biospecimen is available for the subject. "
                "Guppy-derived; not present in the PCDC schema."
            ),
        ),
    ),
}


def _plural_candidates(stem: str) -> list[str]:
    """Likely plural path names for a schema yaml stem."""

    cands = [stem]
    if stem.endswith("y"):
        cands.append(stem[:-1] + "ies")
    elif not stem.endswith("s"):
        cands.append(stem + "s")
    return cands


def _build_path_to_stem(pcdc: dict) -> dict[str, str]:
    """Map gitops path names to schema yaml stems."""

    path_to_stem: dict[str, str] = {}

    for yaml_name, body in pcdc.items():
        if not yaml_name.endswith(".yaml") or yaml_name.startswith("_"):
            continue

        stem = yaml_name[: -len(".yaml")]

        for link in body.get("links", []) or []:
            if not isinstance(link, dict):
                continue

            backref = link.get("backref")
            if isinstance(backref, str) and backref:
                path_to_stem.setdefault(backref, stem)

    for yaml_name in pcdc:
        if not yaml_name.endswith(".yaml") or yaml_name.startswith("_"):
            continue

        stem = yaml_name[: -len(".yaml")]

        for candidate in _plural_candidates(stem):
            path_to_stem.setdefault(candidate, stem)

    return path_to_stem


def _build_field_spec(
    name: str,
    prop: Union[dict, str],
    parent_path: Optional[str],
) -> FieldSpec:
    """Convert a schema property into a FieldSpec."""

    if not isinstance(prop, dict):
        return FieldSpec(name=name, field_type="unknown", parent_path=parent_path)

    desc = prop.get("description", "") or ""

    enum = prop.get("enum")
    if isinstance(enum, list) and enum:
        return FieldSpec(
            name=name,
            field_type="enum",
            enum_values=tuple(str(value) for value in enum),
            description=desc,
            parent_path=parent_path,
        )

    type_info = prop.get("type")
    primary: Optional[str] = None

    if isinstance(type_info, list) and type_info:
        primary = type_info[0]
    elif isinstance(type_info, str):
        primary = type_info

    if primary in ("number", "integer"):
        return FieldSpec(
            name=name,
            field_type="number",
            description=desc,
            parent_path=parent_path,
        )

    if primary == "string":
        return FieldSpec(
            name=name,
            field_type="string",
            description=desc,
            parent_path=parent_path,
        )

    if primary == "boolean":
        return FieldSpec(
            name=name,
            field_type="boolean",
            description=desc,
            parent_path=parent_path,
        )

    return FieldSpec(
        name=name,
        field_type="unknown",
        description=desc,
        parent_path=parent_path,
    )


def _find_filterable_fields(gitops: dict) -> list[tuple[Optional[str], str]]:
    """Return filterable fields listed anywhere in gitops."""

    out: list[tuple[Optional[str], str]] = []

    def recurse(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "fields" and isinstance(value, list):
                    for entry in value:
                        if not isinstance(entry, str):
                            continue

                        if "." in entry:
                            path, name = entry.split(".", 1)
                            out.append((path, name))
                        else:
                            out.append((None, entry))
                elif key =="anchor" and isinstance(value, dict):
                    field = value.get("field")
                    if isinstance(field, str) and field:
                         if "." in field:
                              path, name = field.split(".", 1)
                              out.append((path, name))
                         else:
                                out.append((None, field))
                    recurse(value)
                                
                                
                
                else:
                    recurse(value)

        elif isinstance(node, list):
            for item in node:
                recurse(item)

    recurse(gitops)

    seen = set()
    deduped: list[tuple[Optional[str], str]] = []

    for pair in out:
        if pair not in seen:
            seen.add(pair)
            deduped.append(pair)

    return deduped


def _apply_override(
    key: tuple[Optional[str], str],
    pcdc: dict,
) -> Optional[FieldSpec]:
    """Resolve a known gitops/Guppy exception."""

    override = _OVERRIDES.get(key)
    if override is None:
        return None

    parent_path, field_name = key
    kind, payload = override

    if kind == "inline":
        return payload  # type: ignore[return-value]

    if kind == "redirect":
        yaml_stem = payload  # type: ignore[assignment]
        props = pcdc.get(f"{yaml_stem}.yaml", {}).get("properties", {})

        if field_name in props:
            return _build_field_spec(field_name, props[field_name], parent_path)

    return None


class SchemaIndex:
    """Read-only index built from PCDC schema and gitops."""

    def __init__(
        self,
        fields_by_key: dict[tuple[Optional[str], str], FieldSpec],
        unresolved: Optional[list[tuple[Optional[str], str]]] = None,
    ):
        self._fields = dict(fields_by_key)
        self._unresolved = list(unresolved or [])

        self._by_name: dict[str, list[FieldSpec]] = defaultdict(list)
        self._by_path: dict[Optional[str], list[FieldSpec]] = defaultdict(list)
        self._by_value: dict[str, list[str]] = defaultdict(list)

        for spec in fields_by_key.values():
            self._by_name[spec.name].append(spec)
            self._by_path[spec.parent_path].append(spec)

            for value in spec.enum_values:
                if spec.name not in self._by_value[value]:
                    self._by_value[value].append(spec.name)

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
    ) -> "SchemaIndex":
        """Load schema files and build lookup indexes."""

        with open(pcdc_path, encoding="utf-8") as f:
            pcdc = json.load(f)

        with open(gitops_path, encoding="utf-8") as f:
            gitops = json.load(f)

        path_to_stem = _build_path_to_stem(pcdc)
        filterable = _find_filterable_fields(gitops)

        fields_by_key: dict[tuple[Optional[str], str], FieldSpec] = {}
        unresolved: list[tuple[Optional[str], str]] = []

        for key in filterable:
            parent_path, field_name = key

            spec = _apply_override(key, pcdc)

            if spec is None:
                if parent_path is None:
                    for top_level in _TOP_LEVEL_YAMLS:
                        props = pcdc.get(f"{top_level}.yaml", {}).get("properties", {})
                        if field_name in props:
                            spec = _build_field_spec(field_name, props[field_name], None)
                            break
                else:
                    stem = path_to_stem.get(parent_path)
                    if stem is not None:
                        props = pcdc.get(f"{stem}.yaml", {}).get("properties", {})
                        if field_name in props:
                            spec = _build_field_spec(
                                field_name,
                                props[field_name],
                                parent_path,
                            )

            if spec is None:
                unresolved.append(key)
                spec = FieldSpec(
                    name=field_name,
                    field_type="unknown",
                    parent_path=parent_path,
                )

            fields_by_key[key] = spec

        return cls(fields_by_key, unresolved=unresolved)

    def get_field(
        self,
        name: str,
        path: Optional[str] = None,
    ) -> Optional[FieldSpec]:
        """Exact lookup. Ambiguous names return None unless path is provided."""

        if path is not None:
            return self._fields.get((path, name))

        candidates = self._by_name.get(name, [])
        if len(candidates) == 1:
            return candidates[0]

        return None

    def get_fields(self, name: str) -> list[FieldSpec]:
        return list(self._by_name.get(name, []))

    def is_known_field(self, name: str) -> bool:
        return name in self._by_name

    def all_fields(self) -> Iterable[FieldSpec]:
        return list(self._fields.values())

    def enum_values(
        self,
        field_name: str,
        path: Optional[str] = None,
    ) -> tuple[str, ...]:
        """Return enum values for a field."""

        if path is not None:
            spec = self._fields.get((path, field_name))
            return spec.enum_values if spec else ()

        seen: list[str] = []

        for spec in self._by_name.get(field_name, []):
            for value in spec.enum_values:
                if value not in seen:
                    seen.append(value)

        return tuple(seen)

    def is_valid_value(
        self,
        field_name: str,
        value: str,
        path: Optional[str] = None,
    ) -> bool:
        return value in self.enum_values(field_name, path=path)

    def fields_containing_value(self, value: str) -> list[str]:
        """Return field names where this value appears as an enum."""

        return list(self._by_value.get(value, []))

    def paths_of(self, field_name: str) -> list[Optional[str]]:
        return [spec.parent_path for spec in self._by_name.get(field_name, [])]

    def fields_under_path(self, path: str) -> list[FieldSpec]:
        return list(self._by_path.get(path, []))

    def top_level_fields(self) -> list[FieldSpec]:
        return list(self._by_path.get(None, []))

    def all_paths(self) -> list[str]:
        return [path for path in self._by_path if path is not None]

    @property
    def unresolved(self) -> list[tuple[Optional[str], str]]:
        return list(self._unresolved)


def _cli() -> None:
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog="python -m services.schema_loader")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_desc = sub.add_parser("describe", help="describe a field by name")
    p_desc.add_argument("name")
    p_desc.add_argument("--path", default=None)

    sub.add_parser("paths", help="list nested paths")

    p_find = sub.add_parser("find-value", help="find fields containing an enum value")
    p_find.add_argument("value")

    sub.add_parser("stats", help="print schema index stats")

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    idx = SchemaIndex.from_files(
        repo_root / "schema" / DEFAULT_PCDC_SCHEMA,
        repo_root / "schema" / DEFAULT_GITOPS,
    )

    if args.cmd == "describe":
        specs = (
            [idx.get_field(args.name, path=args.path)]
            if args.path
            else idx.get_fields(args.name)
        )
        specs = [spec for spec in specs if spec is not None]

        if not specs:
            target = f"{args.name!r}"
            if args.path:
                target += f" under path {args.path!r}"
            print(f"no field matches {target}")
            sys.exit(1)

        for index, spec in enumerate(specs):
            if index > 0:
                print()

            print(f"name:        {spec.name}")
            print(f"type:        {spec.field_type}")
            print(f"parent_path: {spec.parent_path if spec.parent_path else '(top-level)'}")

            if spec.enum_values:
                print(f"enum ({len(spec.enum_values)}):")
                for value in spec.enum_values:
                    print(f"  - {value}")

            if spec.description:
                print(f"description: {spec.description[:300]}")

    elif args.cmd == "paths":
        paths = idx.all_paths()

        if not paths:
            print("(no nested paths)")
            return

        for path in sorted(paths):
            count = len(idx.fields_under_path(path))
            print(f"  {path:40s}  {count} fields")

        print(f"\n+ {len(idx.top_level_fields())} top-level fields")

    elif args.cmd == "find-value":
        hits = idx.fields_containing_value(args.value)

        if not hits:
            print(f"no field has {args.value!r} as an enum value")
            sys.exit(1)

        print(f"{args.value!r} appears in:")

        for name in hits:
            for path in idx.paths_of(name):
                print(f"  - {name}  (path: {path or 'top-level'})")

    elif args.cmd == "stats":
        all_fields = list(idx.all_fields())
        by_type: dict[str, int] = defaultdict(int)

        for spec in all_fields:
            by_type[spec.field_type] += 1

        print(f"total fields:      {len(all_fields)}")

        for field_type in ("enum", "number", "string", "boolean", "unknown"):
            print(f"  {field_type:10s}{by_type.get(field_type, 0):>6}")

        print(f"nested paths:      {len(idx.all_paths())}")
        print(f"top-level fields:  {len(idx.top_level_fields())}")
        print(f"unresolved:        {len(idx.unresolved)}")

        if idx.unresolved:
            print("\n  (showing first 10)")
            for parent, name in idx.unresolved[:10]:
                path = parent if parent else "top-level"
                print(f"    - {path}.{name}")

if __name__ == "__main__":
    _cli()