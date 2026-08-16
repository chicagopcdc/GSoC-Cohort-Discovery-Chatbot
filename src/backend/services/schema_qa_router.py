"""Small rule-based router for simple schema questions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from services.schema_explorer import (
    DESCRIBE,
    FIND_VALUE,
    LIST_FIELDS,
    LIST_TABLES,
    VALUES,
    SchemaExplorer,
)


_WORD_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class SchemaQAAnswer:
    query_type: str
    args: dict
    kind: str
    text: str
    data: dict


def _norm(text: str) -> str:
    return _WORD_RE.sub(" ", text.lower()).strip()


def _contains_phrase(haystack: str, needle: str) -> bool:
    return f" {needle} " in f" {haystack} "


class SchemaQARouter:
    def __init__(self, explorer: SchemaExplorer):
        self.explorer = explorer
        schema = explorer.schema
        self._fields = self._index({spec.name for spec in schema.all_fields()})
        self._paths = self._index(schema.all_paths())
        values = set()
        for spec in schema.all_fields():
            values.update(spec.enum_values)
        self._values = self._index(values)

    def answer(self, message: str) -> Optional[SchemaQAAnswer]:
        text = (message or "").strip()
        if not text:
            return None
        norm = _norm(text)
        if not self._looks_like_schema_question(norm):
            return None

        value = self._find_value(norm)
        if value and self._asks_value_owner(norm):
            return self._run(FIND_VALUE, value=value)

        if self._asks_tables(norm):
            return self._run(LIST_TABLES)

        field = self._find_field(norm)
        path = self._find_path(norm)
        if field and self._asks_values(norm):
            return self._run(VALUES, field=field, path=path)

        if field and self._asks_description(norm):
            return self._run(DESCRIBE, field=field, path=path)

        if self._asks_fields(norm):
            return self._run(LIST_FIELDS, path=path)

        return None

    def _run(self, query_type: str, **kwargs) -> SchemaQAAnswer:
        args = {k: v for k, v in kwargs.items() if v is not None}
        result = self.explorer.answer(query_type, **args)
        return SchemaQAAnswer(
            query_type=query_type,
            args=args,
            kind=result.kind,
            text=result.text,
            data=result.data,
        )

    def _index(self, values) -> list[tuple[str, str]]:
        pairs = []
        seen = set()
        for value in values:
            if not value:
                continue
            key = _norm(str(value))
            if not key or key in seen:
                continue
            pairs.append((key, str(value)))
            seen.add(key)
        return sorted(pairs, key=lambda x: (-len(x[0]), x[0]))

    def _find_field(self, norm: str) -> Optional[str]:
        return self._find_from_index(norm, self._fields)

    def _find_path(self, norm: str) -> Optional[str]:
        if _contains_phrase(norm, "top level") or _contains_phrase(norm, "subject"):
            return None
        return self._find_from_index(norm, self._paths)

    def _find_value(self, norm: str) -> Optional[str]:
        return self._find_from_index(norm, self._values)

    def _find_from_index(self, norm: str, index: list[tuple[str, str]]) -> Optional[str]:
        for key, value in index:
            if _contains_phrase(norm, key):
                return value
        return None

    def _looks_like_schema_question(self, norm: str) -> bool:
        cues = (
            "schema", "field", "fields", "value", "values", "allowed", "allow",
            "enum", "option", "options", "table", "tables", "path", "paths",
            "describe", "description", "type", "types", "under", "contains",
            "contain", "belongs", "belong",
        )
        return any(_contains_phrase(norm, cue) for cue in cues)

    def _asks_value_owner(self, norm: str) -> bool:
        owner_cues = (
            "which field", "what field", "which fields", "what fields",
            "contains", "contain", "belongs", "belong", "where",
        )
        return any(_contains_phrase(norm, cue) for cue in owner_cues)

    def _asks_tables(self, norm: str) -> bool:
        table_cues = ("table", "tables", "path", "paths", "nested table", "nested tables")
        return any(_contains_phrase(norm, cue) for cue in table_cues) and (
            _contains_phrase(norm, "list")
            or _contains_phrase(norm, "show")
            or _contains_phrase(norm, "what")
            or _contains_phrase(norm, "which")
            or _contains_phrase(norm, "available")
        )

    def _asks_fields(self, norm: str) -> bool:
        return (
            _contains_phrase(norm, "fields")
            or _contains_phrase(norm, "columns")
            or _contains_phrase(norm, "properties")
        ) and (
            _contains_phrase(norm, "list")
            or _contains_phrase(norm, "show")
            or _contains_phrase(norm, "what")
            or _contains_phrase(norm, "which")
            or _contains_phrase(norm, "available")
            or _contains_phrase(norm, "under")
        )

    def _asks_values(self, norm: str) -> bool:
        return any(
            _contains_phrase(norm, cue)
            for cue in ("value", "values", "allowed", "allow", "enum", "option", "options")
        )

    def _asks_description(self, norm: str) -> bool:
        return any(
            _contains_phrase(norm, cue)
            for cue in ("describe", "description", "type", "types", "numeric", "categorical", "text")
        )
