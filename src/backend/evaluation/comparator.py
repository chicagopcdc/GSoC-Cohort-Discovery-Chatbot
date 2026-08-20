"""Compare a generated filter with a reference filter."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

_RANGE_OPS = ("GTE", "LTE", "GT", "LT")
_BOOL_OPS = ("AND", "OR")
_VALUE_OPS = ("IN", "!=")

# One atomic filter clause.
Leaf = Tuple[Optional[str], str, str, FrozenSet[str]]


class ErrorCategory(str, Enum):
    MISSING_FIELD = "missing_field"
    EXTRA_FIELD = "extra_field"
    WRONG_VALUE = "wrong_value"
    WRONG_RANGE = "incorrect_range"
    WRONG_NESTED_PATH = "wrong_nested_path"
    WRONG_STRUCTURE = "wrong_structure"


@dataclass
class ErrorDetail:
    category: ErrorCategory
    field: Optional[str] = None
    detail: str = ""


@dataclass
class Comparison:
    exact_match: bool
    structural_match: bool
    field_accuracy: float
    value_accuracy: Optional[float]
    structure_accuracy: float
    errors: List[ErrorDetail] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "exact_match": self.exact_match,
            "structural_match": self.structural_match,
            "field_accuracy": self.field_accuracy,
            "value_accuracy": self.value_accuracy,
            "structure_accuracy": self.structure_accuracy,
            "errors": [
                {"category": e.category.value, "field": e.field, "detail": e.detail}
                for e in self.errors
            ],
        }


# Semantic comparison ignores ordering and harmless single-child wrappers.
def canonicalize(node: Any) -> Any:
    if not isinstance(node, dict) or len(node) != 1:
        return node
    (op, body), = node.items()

    if op in _BOOL_OPS:
        children: List[Any] = []
        for child in (body if isinstance(body, list) else []):
            cc = canonicalize(child)
            if isinstance(cc, dict) and len(cc) == 1:
                (cop, cbody), = cc.items()
                if cop == op and isinstance(cbody, list):
                    children.extend(cbody)
                    continue
            children.append(cc)
        if len(children) == 1:
            return children[0]
        children.sort(key=_sort_key)
        return {op: children}

    if op == "IN":
        items = list(body.items()) if isinstance(body, dict) else []
        if len(items) == 1:
            fname, vals = items[0]
            vals = sorted(str(v) for v in (vals if isinstance(vals, list) else [vals]))
            return {"IN": {fname: vals}}
        return node

    if op == "!=":
        items = list(body.items()) if isinstance(body, dict) else []
        if len(items) == 1:
            fname, val = items[0]
            return {"!=": {fname: _num(val)}}
        return node

    if op in _RANGE_OPS:
        return {op: body}

    if op == "nested":
        if not isinstance(body, dict):
            return node
        path = body.get("path")
        inner = "AND" if "AND" in body else ("OR" if "OR" in body else None)
        if inner is None:
            return {"nested": {"path": path}}
        return {"nested": {"path": path, "body": canonicalize({inner: body[inner]})}}

    return node


def _sort_key(node: Any) -> str:
    return json.dumps(node, sort_keys=True, ensure_ascii=False)


def equivalent(a: Any, b: Any) -> bool:
    """Return True when two filters describe the same cohort."""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return False
    return canonicalize(a) == canonicalize(b)


# Structural comparison keeps the logical shape but ignores child order.
def structural_form(node: Any) -> Any:
    if not isinstance(node, dict) or len(node) != 1:
        return None
    (op, body), = node.items()

    if op in _BOOL_OPS:
        children = [structural_form(c) for c in (body if isinstance(body, list) else [])]
        children.sort(key=_sort_key)
        return {op: children}

    if op == "nested" and isinstance(body, dict):
        inner = "AND" if isinstance(body.get("AND"), list) else ("OR" if isinstance(body.get("OR"), list) else None)
        if inner is None:
            return {"nested": {"path": body.get("path")}}
        children = [structural_form(c) for c in body[inner]]
        children.sort(key=_sort_key)
        return {"nested": {"path": body.get("path"), inner: children}}

    if op in _VALUE_OPS and isinstance(body, dict):
        return {op: sorted(body.keys())}

    if op in _RANGE_OPS and isinstance(body, dict):
        return {op: sorted(body.keys())}

    return {op: None}


def _num(v: Any) -> str:
    """Stable string for a numeric bound."""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float)):
        f = float(v)
        return str(int(f)) if f.is_integer() else str(f)
    return str(v)


def _leaves(node: Any, path: Optional[str] = None) -> List[Leaf]:
    """Flatten a filter to its atomic clauses."""
    out: List[Leaf] = []
    if not isinstance(node, dict) or len(node) != 1:
        return out
    (op, body), = node.items()

    if op in _BOOL_OPS:
        for child in (body if isinstance(body, list) else []):
            out += _leaves(child, path)
    elif op == "nested" and isinstance(body, dict):
        npath = body.get("path")
        for key in _BOOL_OPS:
            if isinstance(body.get(key), list):
                for child in body[key]:
                    out += _leaves(child, npath)
    elif op == "IN" and isinstance(body, dict):
        for fname, vals in body.items():
            toks = frozenset(str(v) for v in (vals if isinstance(vals, list) else [vals]))
            out.append((path, "IN", fname, toks))
    elif op == "!=" and isinstance(body, dict):
        for fname, v in body.items():
            out.append((path, "!=", fname, frozenset({_num(v)})))
    elif op in _RANGE_OPS and isinstance(body, dict):
        for fname, v in body.items():
            out.append((path, op, fname, frozenset({_num(v)})))
    return out


def _field_names(leaves: List[Leaf]) -> Set[str]:
    return {fname for _, _, fname, _ in leaves}


def _value_map(leaves: List[Leaf]) -> Dict[Tuple[Optional[str], str], Set[str]]:
    """Map each placement to its value tokens."""
    out: Dict[Tuple[Optional[str], str], Set[str]] = {}
    for path, op, fname, toks in leaves:
        bucket = out.setdefault((path, fname), set())
        bucket |= toks if op == "IN" else {f"{op}:{t}" for t in toks}
    return out


def _field_paths(leaves: List[Leaf]) -> Dict[str, Set[Optional[str]]]:
    out: Dict[str, Set[Optional[str]]] = {}
    for path, _, fname, _ in leaves:
        out.setdefault(fname, set()).add(path)
    return out


def _struct_sig(node: Any, path: Optional[str] = None) -> Counter:
    """Multiset of structural tokens with leaf values stripped."""
    sig: Counter = Counter()
    if not isinstance(node, dict) or len(node) != 1:
        return sig
    (op, body), = node.items()

    if op in _BOOL_OPS:
        sig[(path, op)] += 1
        for child in (body if isinstance(body, list) else []):
            sig += _struct_sig(child, path)
    elif op == "nested" and isinstance(body, dict):
        npath = body.get("path")
        sig[(path, "nested", npath)] += 1
        for key in _BOOL_OPS:
            if isinstance(body.get(key), list):
                for child in body[key]:
                    sig += _struct_sig(child, npath)
    elif op in _VALUE_OPS and isinstance(body, dict):
        for fname in body:
            sig[(path, op, fname)] += 1
    elif op in _RANGE_OPS and isinstance(body, dict):
        for fname in body:
            sig[(path, op, fname)] += 1
    return sig


def _jaccard(a: Set[Any], b: Set[Any]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _multiset_jaccard(a: Counter, b: Counter) -> float:
    if not a and not b:
        return 1.0
    inter = sum((a & b).values())
    union = sum((a | b).values())
    return inter / union if union else 1.0


def _flatten_bool(node: Any) -> Any:
    """Normalize redundant boolean wrappers while keeping wire-style nested blocks."""
    if not isinstance(node, dict) or len(node) != 1:
        return node
    (op, body), = node.items()

    if op in _BOOL_OPS:
        out: List[Any] = []
        for child in (body if isinstance(body, list) else []):
            fc = _flatten_bool(child)
            if isinstance(fc, dict) and len(fc) == 1:
                (cop, cbody), = fc.items()
                if cop == op and isinstance(cbody, list):
                    out.extend(cbody)
                    continue
            out.append(fc)
        return out[0] if len(out) == 1 else {op: out}

    if op == "nested" and isinstance(body, dict):
        inner = "AND" if isinstance(body.get("AND"), list) else ("OR" if isinstance(body.get("OR"), list) else None)
        if inner is None:
            return {"nested": {"path": body.get("path")}}
        flat = _flatten_bool({inner: body[inner]})
        if isinstance(flat, dict) and len(flat) == 1 and next(iter(flat)) in _BOOL_OPS:
            fop, fch = next(iter(flat.items()))
            return {"nested": {"path": body.get("path"), fop: fch}}
        return {"nested": {"path": body.get("path"), inner: [flat]}}

    return node


def compare(generated: Optional[dict], expected: Optional[dict]) -> Comparison:
    """Score a generated filter against a reference filter."""
    exp = expected if isinstance(expected, dict) else None
    gen = generated if isinstance(generated, dict) else None

    if exp is None:
        return Comparison(
            False, False, 0.0, None, 0.0,
            [ErrorDetail(ErrorCategory.WRONG_STRUCTURE, None, "no reference filter")],
        )

    exact = gen is not None and canonicalize(gen) == canonicalize(exp)
    structural = gen is not None and structural_form(gen) == structural_form(exp)

    exp_leaves = _leaves(exp)
    gen_leaves = _leaves(gen) if gen is not None else []

    field_acc = _jaccard(_field_names(gen_leaves), _field_names(exp_leaves))
    value_acc = _value_accuracy(_value_map(gen_leaves), _value_map(exp_leaves))
    struct_acc = _multiset_jaccard(
        _struct_sig(_flatten_bool(gen)) if gen is not None else Counter(),
        _struct_sig(_flatten_bool(exp)),
    )

    errors = [] if exact else _classify(gen_leaves, exp_leaves)
    return Comparison(exact, structural, field_acc, value_acc, struct_acc, errors)


def _value_accuracy(gen_vals: Dict, exp_vals: Dict) -> Optional[float]:
    shared = set(gen_vals) & set(exp_vals)
    if not shared:
        return None
    return sum(_jaccard(gen_vals[k], exp_vals[k]) for k in shared) / len(shared)


def _classify(gen_leaves: List[Leaf], exp_leaves: List[Leaf]) -> List[ErrorDetail]:
    errors: List[ErrorDetail] = []
    seen: Set[Tuple[ErrorCategory, Optional[str]]] = set()

    def add(cat: ErrorCategory, fname: Optional[str], detail: str) -> None:
        key = (cat, fname)
        if key not in seen:
            seen.add(key)
            errors.append(ErrorDetail(cat, fname, detail))

    gen_names = _field_names(gen_leaves)
    exp_names = _field_names(exp_leaves)
    for fname in exp_names - gen_names:
        add(ErrorCategory.MISSING_FIELD, fname, f"reference field {fname!r} not produced")
    for fname in gen_names - exp_names:
        add(ErrorCategory.EXTRA_FIELD, fname, f"produced field {fname!r} is not in the reference")

    gen_paths = _field_paths(gen_leaves)
    exp_paths = _field_paths(exp_leaves)
    gen_vals = _value_map(gen_leaves)
    exp_vals = _value_map(exp_leaves)
    is_range = {fname for _, op, fname, _ in exp_leaves if op in _RANGE_OPS}

    for fname in gen_names & exp_names:
        # Same field, wrong nested block.
        if gen_paths[fname].isdisjoint(exp_paths[fname]):
            add(ErrorCategory.WRONG_NESTED_PATH, fname,
                f"{fname!r} under {sorted(map(str, gen_paths[fname]))}, expected {sorted(map(str, exp_paths[fname]))}")
            continue
        for path in gen_paths[fname] & exp_paths[fname]:
            if gen_vals.get((path, fname)) != exp_vals.get((path, fname)):
                if fname in is_range:
                    add(ErrorCategory.WRONG_RANGE, fname, f"numeric bound on {fname!r} differs")
                else:
                    add(ErrorCategory.WRONG_VALUE, fname, f"value set for {fname!r} differs")

    if not errors:
        add(ErrorCategory.WRONG_STRUCTURE, None, "fields match but the AND/OR structure differs")

    return errors
