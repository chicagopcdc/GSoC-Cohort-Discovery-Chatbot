"""
Schema-driven resolver for numeric ("age at ...") fields.

Field identity, stored unit, and routing cues are DERIVED FROM THE SCHEMA and
overlaid with a small config. There is no per-field regex: one general
description parser, plus each field's own path/name, plus a curated alias table.

- identity: a field is an age field iff its name matches `name_pattern`
  (default ^age_at_) AND the schema types it numeric. This excludes look-alikes
  such as "stage", "language", or "coverage" that merely contain "age".
- unit: parsed from the field's description ("... in days ..."), overridable per
  field in config, falling back to a config default when the description is
  silent.
- routing: cues come from the description's "at <context>" tail, the humanized
  parent path, and the field-name suffix; curated aliases add trusted phrases.
  Derived cues must be at least `min_cue_words` long, so routing stays
  high-confidence rather than matching a single generic token.

Everything is data-driven, so the resolver can be unit-tested against a fake
schema and config without touching the real dictionary files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field as _dc_field
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import yaml


@dataclass
class NumericConfig:
    """Tunable policy for numeric-field resolution. Overridable via YAML."""

    name_pattern: str = r"^age_at_"                # schema-bound field identity
    kind: str = "age"
    default_field: str = "age_at_censor_status"    # used when no cue matches
    default_unit: str = "days"                     # used when a description has no unit
    assumed_input_unit: Optional[str] = "years"    # for a bare number ("older than 5")
    base_unit: str = "days"
    factors_to_base: Dict[str, float] = _dc_field(
        default_factory=lambda: {
            "days": 1.0,
            "weeks": 7.0,
            "months": 30.0,
            "years": 365.25,
        }
    )
    unit_overrides: Dict[str, str] = _dc_field(default_factory=dict)   # field -> unit
    cue_aliases: Dict[str, str] = _dc_field(default_factory=dict)      # phrase -> field
    disabled_fields: Tuple[str, ...] = ()
    min_cue_words: int = 2

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "NumericConfig":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        cfg = cls()
        for key, value in data.items():
            if hasattr(cfg, key) and value is not None:
                setattr(cfg, key, value)
        return cfg


@dataclass(frozen=True)
class Resolution:
    field: str
    path: Optional[str]
    unit: Optional[str]
    confidence: str            # high | medium | default


@dataclass(frozen=True)
class BoundNumeric:
    """One numeric comparison, routed to a field and converted to its unit."""

    field: Optional[str]
    path: Optional[str]
    value: float
    unit: Optional[str]
    original_value: Optional[float]
    original_unit: Optional[str]
    converted: bool
    assumed_unit: bool
    confidence: str


_WORD = re.compile(r"[a-z0-9]+")
_UNIT_IN_DESC = re.compile(r"\bin\s+(days|weeks|months|years)\b", re.I)
_AT_CONTEXT = re.compile(r"\bat\s+(?:the\s+)?(.+?)\.?\s*$", re.I)
_STOP = {"the", "of", "a", "an", "time", "at"}


def _toks(s: str) -> list:
    return [m.group() for m in _WORD.finditer(s.lower())]


def _norm(s: str) -> str:
    return " ".join(t for t in _toks(s) if t not in _STOP)


def _singularize(tokens: list) -> list:
    out = []
    for t in tokens:
        if t.endswith("ies"):
            out.append(t[:-3] + "y")
        elif t.endswith("s") and len(t) > 3:
            out.append(t[:-1])
        else:
            out.append(t)
    return out


class NumericFieldResolver:
    def __init__(self, index, cues, config: NumericConfig):
        self._index = index          # (field, path) -> unit
        self._cues = cues            # normalized phrase -> (field, path, confidence)
        self._cfg = config
        self._default = self._resolve_default()
        self._max_words = max((len(p.split()) for p in cues), default=1)

    def _resolve_default(self):
        for (field, path), unit in self._index.items():
            if field == self._cfg.default_field:
                return (field, path, unit)
        return None

    @classmethod
    def from_schema(
        cls,
        schema,
        config: Optional[NumericConfig] = None,
    ) -> "NumericFieldResolver":
        cfg = config or NumericConfig()
        name_rx = re.compile(cfg.name_pattern)

        index: Dict[Tuple[str, Optional[str]], Optional[str]] = {}
        cues: Dict[str, Tuple[str, Optional[str], str]] = {}
        rank = {"high": 2, "medium": 1}

        def add_cue(phrase, field, path, source):
            key = _norm(phrase)
            if not key:
                return
            # Derived cues must be specific; curated aliases are trusted as-is.
            if source != "alias" and len(key.split()) < cfg.min_cue_words:
                return
            conf = "high" if source in ("desc", "alias") else "medium"
            cur = cues.get(key)
            if cur is None or rank[conf] > rank[cur[2]]:
                cues[key] = (field, path, conf)

        for spec in schema.all_fields():
            if spec.field_type not in ("number", "unknown"):
                continue
            if not name_rx.search(spec.name) or spec.name in cfg.disabled_fields:
                continue

            path = spec.parent_path
            desc = spec.description or ""

            # unit: config override -> schema description -> config default
            unit = cfg.unit_overrides.get(spec.name)
            if unit is None:
                m = _UNIT_IN_DESC.search(desc)
                unit = m.group(1).lower() if m else cfg.default_unit
            index[(spec.name, path)] = unit

            # cue 1: description's "at <context>" tail (highest-value, schema-bound)
            at = _AT_CONTEXT.search(desc)
            if at:
                add_cue(at.group(1), spec.name, path, "desc")
            # cue 2: humanized parent path, plus a light singular form
            if path:
                add_cue(" ".join(_toks(path)), spec.name, path, "path")
                add_cue(" ".join(_singularize(_toks(path))), spec.name, path, "path")
            # cue 3: field name minus the age_at_ prefix
            add_cue(" ".join(_toks(name_rx.sub("", spec.name))), spec.name, path, "name")

        # cue 4: curated aliases from config (trusted, any length)
        for phrase, field_name in cfg.cue_aliases.items():
            match = next(((f, p) for (f, p) in index if f == field_name), None)
            if match:
                add_cue(phrase, match[0], match[1], "alias")

        return cls(index, cues, cfg)

    # --- runtime -----------------------------------------------------------
    def unit_of(self, field: str, path: Optional[str]) -> Optional[str]:
        return self._index.get((field, path))

    def route(self, text: str, span: Tuple[int, int]) -> Optional[Resolution]:
        """Pick the field whose cue sits closest to the range, else the default."""
        toks = [(m.group().lower(), m.start()) for m in _WORD.finditer(text)]
        words = [w for w, _ in toks if w not in _STOP]
        offsets = [o for w, o in toks if w not in _STOP]
        mid = (span[0] + span[1]) / 2

        best, best_dist = None, None
        n = len(words)
        for i in range(n):
            for k in range(min(self._max_words, n - i), 0, -1):
                hit = self._cues.get(" ".join(words[i:i + k]))
                if hit is None:
                    continue
                dist = abs(offsets[i] - mid)
                if best_dist is None or dist < best_dist:
                    field, path, conf = hit
                    best = Resolution(field, path, self.unit_of(field, path), conf)
                    best_dist = dist

        if best is not None:
            return best
        if self._default is not None:
            field, path, unit = self._default
            return Resolution(field, path, unit, "default")
        return None

    def convert(self, value: float, from_unit: str, to_unit: str) -> Optional[float]:
        factors = self._cfg.factors_to_base
        if from_unit not in factors or to_unit not in factors:
            return None
        return round(value * factors[from_unit] / factors[to_unit], 2)

    def bind(
        self,
        value: float,
        stated_unit: Optional[str],
        text: str,
        span: Tuple[int, int],
    ) -> BoundNumeric:
        """Route the comparison to a field and convert its value to the field's unit."""
        res = self.route(text, span)
        if res is None:
            return BoundNumeric(None, None, value, stated_unit, None, None, False, False, "none")

        use_unit, assumed = stated_unit, False
        if use_unit is None and self._cfg.assumed_input_unit is not None:
            use_unit, assumed = self._cfg.assumed_input_unit, True

        if use_unit and res.unit:
            converted = self.convert(value, use_unit, res.unit)
            if converted is not None:
                return BoundNumeric(
                    res.field, res.path, converted, res.unit,
                    value, use_unit, True, assumed, res.confidence,
                )

        return BoundNumeric(
            res.field, res.path, value, res.unit,
            None, None, False, False, res.confidence,
        )
