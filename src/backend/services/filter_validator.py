"""
Schema-aware validation for generated Guppy filters.

The Pydantic models validate the filter shape first. This module then checks
whether the referenced fields, paths, enum values, and operators are valid for
the active schema. Validation errors are returned as structured issues so they
can be reused by the query-builder, benchmarks, or debugging tools.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pydantic import ValidationError as PydanticValidationError

from models.filters import (
    AndClause,
    GraphQLFilter,
    GTClause,
    GTEClause,
    InClause,
    LTClause,
    LTEClause,
    NestedClause,
    OrClause,
)
from services.schema_loader import FieldSpec, SchemaIndex


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    field: Optional[str] = None
    path: Optional[str] = None
    value: Optional[str] = None


@dataclass
class ValidationResult:
    issues: list[ValidationIssue]

    @property
    def ok(self) -> bool:
        return not self.issues

    def codes(self) -> list[str]:
        return [i.code for i in self.issues]


# Stable issue codes so the benchmark can count them across runs
CODE_UNKNOWN_FIELD = "unknown_field"
CODE_WRONG_PATH = "wrong_path"
CODE_INVALID_ENUM = "invalid_enum_value"
CODE_TYPE_MISMATCH = "type_mismatch"
CODE_UNKNOWN_PATH = "unknown_path"
CODE_NESTED_IN_NESTED = "nested_in_nested"
CODE_STRUCTURAL = "structural"


_RANGE_ATTR = {
    GTEClause: "GTE",
    LTEClause: "LTE",
    GTClause: "GT",
    LTClause: "LT",
}


def validate_filter(gf: GraphQLFilter, schema: SchemaIndex) -> ValidationResult:
    """Validate a structurally-valid filter against the schema."""
    issues: list[ValidationIssue] = []
    _walk(gf.root, None, schema, issues)
    return ValidationResult(issues)


def validate_dict(obj: dict, schema: SchemaIndex) -> ValidationResult:
    """Run both layers: Pydantic (structure) then schema (semantics). A
    structural failure comes back as one issue so callers always get the same
    result type."""
    try:
        gf = GraphQLFilter.model_validate(obj)
    except PydanticValidationError as e:
        return ValidationResult([ValidationIssue(CODE_STRUCTURAL, str(e))])
    return validate_filter(gf, schema)


def _walk(clause, path: Optional[str], schema: SchemaIndex, issues: list) -> None:
    if isinstance(clause, InClause):
        _check_in(clause, path, schema, issues)
    elif isinstance(clause, tuple(_RANGE_ATTR)):
        _check_range(clause, path, schema, issues)
    elif isinstance(clause, AndClause):
        for child in clause.AND:
            _walk(child, path, schema, issues)
    elif isinstance(clause, OrClause):
        for child in clause.OR:
            _walk(child, path, schema, issues)
    elif isinstance(clause, NestedClause):
        _check_nested(clause, path, schema, issues)


def _resolve(field: str, path: Optional[str], schema: SchemaIndex,
             issues: list) -> Optional[FieldSpec]:
    """Resolve a field for the current path.
    If the field is missing or belongs to another path, record a validation issue
    and return None. Top-level fields are matched by requiring parent_path to be
    None, rather than treating None as an unspecified path.
    """
    specs = schema.get_fields(field)
    if not specs:
        issues.append(ValidationIssue(
            CODE_UNKNOWN_FIELD, f"unknown field {field!r}", field=field, path=path))
        return None

    here = [s for s in specs if s.parent_path == path]
    if not here:
        lives = sorted({s.parent_path or "(top-level)" for s in specs})
        issues.append(ValidationIssue(
            CODE_WRONG_PATH,
            f"field {field!r} is not available at {path or '(top-level)'}; "
            f"it lives under {lives}",
            field=field, path=path))
        return None

    return here[0]


def _check_in(clause: InClause, path, schema, issues) -> None:
    for field, values in clause.IN.items():
        spec = _resolve(field, path, schema, issues)
        if spec is None:
            continue
        # IN on a free-text/number field is unusual but not wrong — we just
        # have no enum to check the values against, so leave them alone.
        if spec.field_type != "enum":
            continue
        for v in values:
            if str(v) not in spec.enum_values:
                issues.append(ValidationIssue(
                    CODE_INVALID_ENUM,
                    f"{v!r} is not a valid value for {field!r}",
                    field=field, path=path, value=str(v)))


def _check_range(clause, path, schema, issues) -> None:
    op = _RANGE_ATTR[type(clause)]
    for field, _value in getattr(clause, op).items():
        spec = _resolve(field, path, schema, issues)
        if spec is None:
            continue
        if spec.field_type not in ("number", "unknown"):
            issues.append(ValidationIssue(
                CODE_TYPE_MISMATCH,
                f"{op} can't be applied to to {spec.field_type} field {field!r}",
                field=field, path=path))


def _check_nested(clause: NestedClause, path, schema, issues) -> None:
    body = clause.nested

    # Guppy descends exactly one level from subject
    if path is not None:
        issues.append(ValidationIssue(
            CODE_NESTED_IN_NESTED,
            f"nested filter on {body.path!r} sits inside another nested filter "
            f"({path!r}); only one level is supported",
            path=body.path))

    if body.path not in schema.all_paths():
        issues.append(ValidationIssue(
            CODE_UNKNOWN_PATH, f"unknown nested path {body.path!r}", path=body.path))
        return

    children = body.AND if body.AND is not None else (body.OR or [])
    for child in children:
        _walk(child, body.path, schema, issues)


def _cli() -> None:
    import argparse
    import json
    import sys

    from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA

    parser = argparse.ArgumentParser(prog="python -m services.filter_validator")
    parser.add_argument("filter_json", help="a GraphQL filter as a JSON string")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[3]
    schema = SchemaIndex.from_files(
        repo_root / "schema" / DEFAULT_PCDC_SCHEMA,
        repo_root / "schema" / DEFAULT_GITOPS,
    )

    try:
        obj = json.loads(args.filter_json)
    except json.JSONDecodeError as e:
        print(f"invalid JSON: {e}")
        sys.exit(2)

    result = validate_dict(obj, schema)
    if result.ok:
        print("ok — no issues")
        return
    for issue in result.issues:
        print(f"[{issue.code}] {issue.message}  (at {issue.path or '(top-level)'})")
    sys.exit(1)


if __name__ == "__main__":
    _cli()