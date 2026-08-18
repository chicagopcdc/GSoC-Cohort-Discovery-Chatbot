"""Tool 2: lookup for filterable schema fields, paths, and enum values."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Union

from services.schema_loader import FieldSpec, SchemaIndex


DEFAULT_PREVIEW_LIMIT = 40

LIST_FIELDS = "list_fields"
VALUES = "values"
DESCRIBE = "describe"
FIND_VALUE = "find_value"
LIST_TABLES = "list_tables"
QUERY_TYPES = (LIST_FIELDS, VALUES, DESCRIBE, FIND_VALUE, LIST_TABLES)


@dataclass
class ExploreResult:
    kind: str
    text: str
    data: dict


class SchemaExplorer:
    def __init__(self, schema: SchemaIndex, *, preview_limit: int = DEFAULT_PREVIEW_LIMIT):
        if preview_limit < 1:
            raise ValueError(f"preview_limit must be >= 1, got {preview_limit}")
        self.schema = schema
        self.preview_limit = preview_limit

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        *,
        preview_limit: int = DEFAULT_PREVIEW_LIMIT,
    ) -> "SchemaExplorer":
        return cls(SchemaIndex.from_files(pcdc_path, gitops_path), preview_limit=preview_limit)

    def answer(self, query_type: str, *, field: Optional[str] = None,
               path: Optional[str] = None, value: Optional[str] = None) -> ExploreResult:
        query_type = (query_type or "").strip()
        if query_type == LIST_FIELDS:
            return self.list_fields(path)
        if query_type == VALUES:
            return self.field_values(field, path)
        if query_type == DESCRIBE:
            return self.describe_field(field, path)
        if query_type == FIND_VALUE:
            return self.find_value(value)
        if query_type == LIST_TABLES:
            return self.list_paths()
        return self._error("unknown_query_type",
                           f"unknown query_type {query_type!r}; expected one of {QUERY_TYPES}")

    def list_fields(self, path: Optional[str] = None) -> ExploreResult:
        """List fields under a path, or top-level subject fields."""
        if path is not None:
            path = path.strip()
            if not path:
                return self._error("empty_path", "path must not be empty")
            if path not in set(self.schema.all_paths()):
                return self._error("unknown_path", f"no filterable table named {path!r}")
            specs = sorted(self.schema.fields_under_path(path), key=lambda s: s.name)
            where = path
        else:
            specs = sorted(self.schema.top_level_fields(), key=lambda s: s.name)
            where = "subject (top-level)"

        lines = [f"Table: {where}", f"Fields ({len(specs)}):"]
        for s in specs:
            lines.append("- " + self._field_line(s))
        return ExploreResult(
            kind="fields",
            text="\n".join(lines),
            data={"path": path, "field_count": len(specs),
                  "fields": [self._field_brief(s) for s in specs]},
        )

    def field_values(self, field: Optional[str], path: Optional[str] = None) -> ExploreResult:
        specs, err = self._resolve(field, path)
        if err is not None:
            return err
        field = field.strip()

        if len(specs) > 1:
            specs = sorted(specs, key=lambda s: (s.parent_path or ""))
            where = ", ".join(s.parent_path or "subject" for s in specs)
            return ExploreResult(
                kind="values",
                text=f"Field {field!r} exists under multiple tables ({where}); "
                     f"specify a path to see its values.",
                data={"field": field, "ambiguous": True,
                      "placements": [self._values_entry(s) for s in specs]},
            )

        spec = specs[0]
        if spec.field_type != "enum":
            return ExploreResult(
                kind="values",
                text=f"{field} is a {spec.field_type} field; it has no enum value list.",
                data={"field": field, "path": spec.parent_path,
                      "field_type": spec.field_type, "enum_total": 0},
            )

        prev = self._enum_preview(spec.enum_values)
        more = f" (+{prev['enum_more_count']} more)" if prev["enum_more_count"] else ""
        return ExploreResult(
            kind="values",
            text=f"{field} ({len(spec.enum_values)} values): "
                 f"{', '.join(prev['enum_preview'])}{more}",
            data={"field": field, "path": spec.parent_path, "field_type": "enum", **prev},
        )

    def describe_field(self, field: Optional[str], path: Optional[str] = None) -> ExploreResult:
        specs, err = self._resolve(field, path)
        if err is not None:
            return err
        field = field.strip()
        specs = sorted(specs, key=lambda s: (s.parent_path or ""))

        blocks = []
        for s in specs:
            where = s.parent_path or "subject (top-level)"
            block = [f"{s.name}  (under {where})", f"  type: {s.field_type}"]
            if s.field_type == "enum" and s.enum_values:
                prev = self._enum_preview(s.enum_values)
                more = f" (+{prev['enum_more_count']} more)" if prev["enum_more_count"] else ""
                block.append(f"  values: {', '.join(prev['enum_preview'])}{more}")
            if s.description:
                block.append(f"  description: {s.description[:300]}")
            blocks.append("\n".join(block))

        return ExploreResult(
            kind="describe",
            text="\n\n".join(blocks),
            data={"field": field, "placements": [self._describe_entry(s) for s in specs]},
        )

    def find_value(self, value: Optional[str]) -> ExploreResult:
        if not value or not str(value).strip():
            return self._error("empty_value", "value must not be empty")
        value = str(value).strip()

        names = sorted(self.schema.fields_containing_value(value))
        entries = []
        for name in names:
            for spec in sorted(self.schema.get_fields(name), key=lambda s: (s.parent_path or "")):
                if value in spec.enum_values:
                    entries.append({"field": spec.name, "path": spec.parent_path})

        if not entries:
            return self._error("unknown_value", f"no filterable field has {value!r} as a value")

        lines = [f"{value!r} is a value of:"]
        for e in entries:
            lines.append(f"- {e['field']}  (under {e['path'] or 'subject'})")
        return ExploreResult(kind="value_fields", text="\n".join(lines),
                             data={"value": value, "fields": entries})

    def list_paths(self) -> ExploreResult:
        paths = sorted(self.schema.all_paths())
        path_data = [{"path": p, "field_count": len(self.schema.fields_under_path(p))} for p in paths]
        top_count = len(self.schema.top_level_fields())

        lines = [f"Nested tables ({len(paths)}):"]
        for pd in path_data:
            lines.append(f"- {pd['path']}  ({pd['field_count']} fields)")
        lines.append(f"\n+ {top_count} top-level (subject) fields")

        return ExploreResult(
            kind="paths",
            text="\n".join(lines),
            data={"path_count": len(paths), "paths": path_data,
                  "top_level_field_count": top_count},
        )

    def _resolve(self, field: Optional[str], path: Optional[str]):
        if not field or not field.strip():
            return [], self._error("empty_field", "field must not be empty")
        field = field.strip()
        specs = self.schema.get_fields(field)
        if not specs:
            return [], self._error("unknown_field", f"no filterable field named {field!r}")
        if path is not None:
            path = path.strip()
            if not path:
                return [], self._error("empty_path", "path must not be empty")
            here = [s for s in specs if s.parent_path == path]
            if not here:
                under = sorted(p or "subject" for p in self.schema.paths_of(field))
                return [], self._error(
                    "wrong_path",
                    f"field {field!r} is not under {path!r}; it lives under {under}",
                    field_paths=under,
                )
            return here, None
        return specs, None

    def _field_brief(self, spec: FieldSpec) -> dict:
        d = {"name": spec.name, "field_type": spec.field_type, "path": spec.parent_path}
        if spec.field_type == "enum":
            d.update(self._enum_preview(spec.enum_values))
        return d

    def _field_line(self, spec: FieldSpec) -> str:
        if spec.field_type == "enum" and spec.enum_values:
            prev = self._enum_preview(spec.enum_values)
            more = f" (+{prev['enum_more_count']} more)" if prev["enum_more_count"] else ""
            return f"{spec.name} (enum): {', '.join(prev['enum_preview'])}{more}"
        return f"{spec.name} ({spec.field_type})"

    def _values_entry(self, spec: FieldSpec) -> dict:
        d = {"field": spec.name, "path": spec.parent_path, "field_type": spec.field_type}
        if spec.field_type == "enum":
            d.update(self._enum_preview(spec.enum_values))
        else:
            d["enum_total"] = 0
        return d

    def _describe_entry(self, spec: FieldSpec) -> dict:
        d = {"name": spec.name, "path": spec.parent_path,
             "field_type": spec.field_type, "description": spec.description}
        if spec.field_type == "enum":
            d.update(self._enum_preview(spec.enum_values))
        return d

    def _enum_preview(self, values: Iterable[str]) -> dict:
        vals = [str(v) for v in values]
        shown = vals[: self.preview_limit]
        return {"enum_preview": shown, "enum_total": len(vals),
                "enum_more_count": max(0, len(vals) - len(shown))}

    def _error(self, code: str, message: str, **extra) -> ExploreResult:
        paths = sorted(self.schema.all_paths())
        fields = sorted({s.name for s in self.schema.all_fields()})
        data = {
            "code": code,
            "message": message,
            "available_paths": paths[: self.preview_limit],
            "available_paths_total": len(paths),
            "available_fields": fields[: self.preview_limit],
            "available_fields_total": len(fields),
        }
        data.update(extra)
        return ExploreResult(kind="error", text=message, data=data)
