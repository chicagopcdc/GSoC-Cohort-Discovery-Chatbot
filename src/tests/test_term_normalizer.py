import sys
from pathlib import Path


def _find_upwards(relative: str) -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / relative
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"could not find {relative} above {here}")


_SERVICES = _find_upwards("backend/services")
if str(_SERVICES.parent) not in sys.path:
    sys.path.insert(0, str(_SERVICES.parent))

import pytest

from services.schema_loader import (
    DEFAULT_GITOPS,
    DEFAULT_PCDC_SCHEMA,
    FieldSpec,
    SchemaIndex,
)
from services.term_normalizer import (
    FieldPlacement,
    TermNormalizer,
    _canon_unit,
    _negation_before,
    _norm_phrase,
    _tokenize,
    load_synonyms,
)

SCHEMA_DIR = _find_upwards("schema")

# A small synonym table the tests control, so they don't drift with whatever
# the committed synonyms.yaml happens to contain.
_SYNONYMS = {"brain tumor": "Brain", "mets": "Metastatic"}


@pytest.fixture(scope="module")
def idx() -> SchemaIndex:
    return SchemaIndex.from_files(
        SCHEMA_DIR / DEFAULT_PCDC_SCHEMA,
        SCHEMA_DIR / DEFAULT_GITOPS,
    )


@pytest.fixture(scope="module")
def tn(idx) -> TermNormalizer:
    return TermNormalizer(idx, _SYNONYMS)


def _values(result) -> set:
    return {t.value.lower() for t in result.terms}


class TestPhraseRecognition:
    def test_not_reported_is_not_split(self, tn):
        # the legacy code turned this into ["Reported"]; it must survive whole
        result = tn.normalize("INRG patients with Not Reported race")
        assert any(v == "not reported" for v in _values(result))

    def test_consortium_recognized(self, tn):
        result = tn.normalize("patients from the INRG consortium")
        assert "inrg" in _values(result)

    def test_single_word_enum_recognized(self, tn):
        result = tn.normalize("tumors located on skin")
        assert "skin" in _values(result)

    def test_unrelated_words_not_matched(self, tn):
        result = tn.normalize("please show me the patients")
        assert result.terms == []


class TestPunctuation:
    def test_trailing_period(self, tn):
        result = tn.normalize("the tumor is located on the skin.")
        assert "skin" in _values(result)

    def test_trailing_comma(self, tn):
        result = tn.normalize("metastatic, regional tumors")
        vals = _values(result)
        assert "metastatic" in vals
        assert "regional" in vals

    def test_phrase_with_trailing_punct(self, tn):
        result = tn.normalize("race is Not Reported.")
        assert any(v == "not reported" for v in _values(result))


class TestPlacements:
    def test_multi_path_value_keeps_all_paths(self):
        # synthetic schema so the assertion is deterministic
        fields = {
            ("tumor_assessments", "tumor_classification"):
                FieldSpec("tumor_classification", "enum", ("Metastatic", "Primary"), "", "tumor_assessments"),
            ("biopsy_surgical_procedures", "tumor_classification"):
                FieldSpec("tumor_classification", "enum", ("Metastatic",), "", "biopsy_surgical_procedures"),
            (None, "sex"):
                FieldSpec("sex", "enum", ("Male", "Female"), "", None),
        }
        tn = TermNormalizer(SchemaIndex(fields))
        result = tn.normalize("metastatic patients")
        term = next(t for t in result.terms if t.value == "Metastatic")
        paths = {p.path for p in term.placements}
        assert paths == {"tumor_assessments", "biopsy_surgical_procedures"}

    def test_top_level_placement_has_none_path(self):
        fields = {(None, "sex"): FieldSpec("sex", "enum", ("Male", "Female"), "", None)}
        tn = TermNormalizer(SchemaIndex(fields))
        result = tn.normalize("male patients")
        term = next(t for t in result.terms if t.value == "Male")
        assert term.placements == (FieldPlacement("sex", None),)

    def test_unknown_canonical_has_empty_placements(self):
        fields = {(None, "sex"): FieldSpec("sex", "enum", ("Male",), "", None)}
        # synonym points at a value that isn't in the schema
        tn = TermNormalizer(SchemaIndex(fields), {"chemo": "Chemotherapy"})
        result = tn.normalize("chemo patients")
        term = next(t for t in result.terms if t.value == "Chemotherapy")
        assert term.placements == ()



class TestNumericExtraction:
    def test_older_than_with_unit(self, tn):
        result = tn.normalize("patients older than 5 years")
        r = result.ranges[0]
        assert (r.op, r.value, r.unit, r.quantity) == ("gt", 5.0, "years", "age")

    def test_older_than_without_unit_still_age(self, tn):
        # "older than" is inherently an age comparison
        result = tn.normalize("patients older than 5")
        r = result.ranges[0]
        assert r.op == "gt"
        assert r.quantity == "age"

    def test_generic_comparator_no_age_hint(self, tn):
        result = tn.normalize("dose more than 50")
        r = result.ranges[0]
        assert r.op == "gt"
        assert r.quantity is None

    def test_between_expands_to_two_bounds(self, tn):
        result = tn.normalize("between 0 and 18 years of age")
        ops = {(r.op, r.value) for r in result.ranges}
        assert ("gte", 0.0) in ops
        assert ("lte", 18.0) in ops
        assert all(r.quantity == "age" for r in result.ranges)

    def test_symbolic_operator(self, tn):
        result = tn.normalize("value >= 3")
        r = result.ranges[0]
        assert (r.op, r.value) == ("gte", 3.0)


class TestNegation:
    def test_not_older_than_flips_to_lte(self, tn):
        result = tn.normalize("INRG patients NOT older than 5")
        r = next(r for r in result.ranges if r.value == 5.0)
        assert r.op == "lte"          # gt flipped
        assert r.negated is True
        assert any("older than 5" in n.lower() for n in result.negations)

    def test_negated_term(self):
        fields = {
            ("tumor_assessments", "tumor_classification"):
                FieldSpec("tumor_classification", "enum", ("Metastatic",), "", "tumor_assessments"),
        }
        tn = TermNormalizer(SchemaIndex(fields))
        result = tn.normalize("patients not metastatic")
        term = next(t for t in result.terms if t.value == "Metastatic")
        assert term.negated is True

    def test_negation_lookback_window(self, tn):
        # "not" is 4 words before "older" — too far to negate
        result = tn.normalize("not interested in patients older than 5")
        r = next(r for r in result.ranges if r.value == 5.0)
        assert r.op == "gt"
        assert r.negated is False


class TestSynonyms:
    def test_surface_maps_to_canonical(self, tn):
        result = tn.normalize("patients with brain tumor")
        term = next(t for t in result.terms if t.value == "Brain")
        assert any(p.field == "tumor_site" for p in term.placements)

    def test_abbreviation(self, tn):
        result = tn.normalize("mets in the lung")
        assert "Metastatic" in {t.value for t in result.terms}


class TestYamlLoading:
    def test_no_yes_not_coerced_to_bool(self, tmp_path):
        p = tmp_path / "syn.yaml"
        p.write_text("no bulk: No\nhas bulk: Yes\nbrain tumor: Brain\n", encoding="utf-8")
        syn = load_synonyms(p)
        assert syn["no bulk"] == "No"        # not "False"
        assert syn["has bulk"] == "Yes"      # not "True"
        assert syn["brain tumor"] == "Brain"

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_synonyms(tmp_path / "nope.yaml") == {}

    def test_real_synonyms_file_loads(self):
        path = _find_upwards("backend/data") / "synonyms.yaml"
        if path.exists():
            assert isinstance(load_synonyms(path), dict)


class TestTokenize:
    def test_strips_edge_punctuation(self):
        toks = _tokenize("skin.")
        assert toks[0][0] == "skin"

    def test_keeps_internal_punctuation(self):
        toks = _tokenize("stroma-poor")
        assert toks[0][0] == "stroma-poor"

    def test_drops_pure_punctuation_token(self):
        toks = _tokenize("a -- b")
        words = [t[0] for t in toks]
        assert words == ["a", "b"]

    def test_spans_point_into_original(self):
        toks = _tokenize("on skin.")
        word, start, end = toks[1]
        assert (word, start, end) == ("skin", 3, 7)


class TestNormPhrase:
    def test_lowercases_and_trims(self):
        assert _norm_phrase("Not Reported.") == "not reported"


class TestCanonUnit:
    @pytest.mark.parametrize("raw,expected", [
        ("years", "years"), ("yr", "years"), ("month", "months"),
        ("mos", "months"), ("days", "days"), (None, None),
    ])
    def test_canon(self, raw, expected):
        assert _canon_unit(raw) == expected


class TestNegationBefore:
    def test_cue_directly_before(self):
        text = "not older"
        assert _negation_before(text, text.index("older")) is not None

    def test_cue_too_far(self):
        text = "not a b c older"
        assert _negation_before(text, text.index("older")) is None
        
class TestToDict:
    def test_term_span_in_output(self, tn):
        text = "male patients"
        d = tn.normalize(text).to_dict()
        term = next(t for t in d["recognized_terms"] if t["value"] == "Male")
        span = term["span"]
        assert isinstance(span, list) and len(span) == 2
        # span must point back at the matched substring
        assert text[span[0]:span[1]].lower() == "male"

    def test_range_span_in_output(self, tn):
        text = "patients older than 5 years"
        d = tn.normalize(text).to_dict()
        r = d["ranges"][0]
        span = r["span"]
        assert isinstance(span, list) and len(span) == 2
        assert "5" in text[span[0]:span[1]]

    def test_span_is_json_serializable(self, tn):
        import json
        d = tn.normalize("male patients older than 5 years").to_dict()
        json.dumps(d)  # must not raise