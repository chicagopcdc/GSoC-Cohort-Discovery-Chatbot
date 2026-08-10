import json
import sys
from pathlib import Path

import pytest

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.filter_generator import FilterGenerator, GeneratorConfig
from services.filter_validator import CODE_CONFLICTING_IN, validate_dict
from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA, FieldSpec, SchemaIndex
from services.semantic_enricher import (
    SemanticEnricher,
    SemanticMetadata,
    default_semantic_metadata_path,
)
from services.semantic_intent import ClinicalIntent, infer_intent, infer_intent_near
from services.term_normalizer import TermNormalizer


def _generic_schema():
    return SchemaIndex({
        ("sample_assessments", "sample_site"): FieldSpec(
            "sample_site",
            "enum",
            ("Skin", "Liver"),
            parent_path="sample_assessments",
        ),
        ("sample_assessments", "sample_kind"): FieldSpec(
            "sample_kind",
            "enum",
            ("Metastatic", "Primary"),
            parent_path="sample_assessments",
        ),
        ("sample_assessments", "sample_status"): FieldSpec(
            "sample_status",
            "enum",
            ("Positive", "Negative", "Unknown"),
            parent_path="sample_assessments",
        ),
        ("other_assessments", "other_site"): FieldSpec(
            "other_site",
            "enum",
            ("Skin", "Liver"),
            parent_path="other_assessments",
        ),
        ("other_assessments", "other_status"): FieldSpec(
            "other_status",
            "enum",
            ("Positive", "Negative", "Unknown"),
            parent_path="other_assessments",
        ),
        (None, "consortium"): FieldSpec(
            "consortium",
            "enum",
            ("INRG", "NODAL"),
            parent_path=None,
        ),
    })


def _generic_enricher():
    schema = _generic_schema()
    metadata = SemanticMetadata.from_dict(
        {
            "semantic_entities": {
                "sample_assessments": {
                    "entity": "sample_assessment",
                    "descriptor_fields": ["sample_site", "sample_kind"],
                    "assertion": {
                        "field": "sample_status",
                        "positive_values": ["Positive"],
                        "negative_values": ["Negative"],
                        "unknown_values": ["Unknown"],
                    },
                }
            }
        },
        schema,
    )
    return SemanticEnricher(metadata)


def _nested_children(wire, path="sample_assessments"):
    if "nested" in wire and wire["nested"]["path"] == path:
        return wire["nested"].get("AND", [])
    for child in wire.get("AND", []):
        if "nested" in child and child["nested"]["path"] == path:
            return child["nested"].get("AND", [])
    raise AssertionError(f"no nested block for {path}")


def _has_in(children, field, value):
    return any(child.get("IN", {}).get(field) == [value] for child in children)


class TestIntentInference:
    @pytest.mark.parametrize(
        ("text", "intent"),
        [
            ("patients with metastatic tumors", ClinicalIntent.POSITIVE_EXISTENCE),
            ("patients who have metastatic tumors", ClinicalIntent.POSITIVE_EXISTENCE),
            ("patients without metastatic tumors", ClinicalIntent.NEGATIVE_EXISTENCE),
            ("patients assessed for metastatic tumors", ClinicalIntent.RECORD_EXISTS),
            ("tumor assessments have been classified as metastatic", ClinicalIntent.RECORD_EXISTS),
            ("metastatic tumor assessments regardless of state", ClinicalIntent.ANY_STATE),
            ("molecular abnormalities with presence or absence", ClinicalIntent.ANY_STATE),
            ("patients with unknown metastatic tumor status", ClinicalIntent.UNKNOWN_STATE),
            ("patients who do not exhibit 17q gain", ClinicalIntent.NEGATIVE_EXISTENCE),
            (
                "patients who underwent molecular analysis and exhibit an absence of MYCN",
                ClinicalIntent.NEGATIVE_EXISTENCE,
            ),
            ("patients with the presence of 11q deletion", ClinicalIntent.POSITIVE_EXISTENCE),
            ("patients who exhibit 11q deletion", ClinicalIntent.POSITIVE_EXISTENCE),
            ("metastatic tumor status", ClinicalIntent.STATE_UNSPECIFIED),
        ],
    )
    def test_common_intents(self, text, intent):
        assert infer_intent(text).intent == intent

    def test_strong_text_cue_overrides_model_unspecified(self):
        got = infer_intent(
            "patients without metastatic tumors",
            model_intent="state_unspecified",
        )

        assert got.intent == ClinicalIntent.NEGATIVE_EXISTENCE
        assert got.source == "heuristic"

    def test_nearest_cue_wins_for_each_half_of_a_mixed_query(self):
        """A query can assert one finding and deny another in one sentence."""
        text = "INRG patients with metastatic tumors who do not exhibit MYCN amplification"
        positive_span = (text.index("metastatic"), text.index("metastatic") + 10)
        negative_span = (text.index("MYCN"), text.index("MYCN") + 4)

        assert infer_intent_near(text, positive_span).intent == (
            ClinicalIntent.POSITIVE_EXISTENCE
        )
        assert infer_intent_near(text, negative_span).intent == (
            ClinicalIntent.NEGATIVE_EXISTENCE
        )
        # Read as one sentence it can only be one of the two, which is the bug.
        assert infer_intent(text).intent == ClinicalIntent.NEGATIVE_EXISTENCE

    def test_span_with_no_nearby_cue_falls_back_to_the_whole_query(self):
        text = "patients without metastatic tumors" + " padding" * 20 + " skin"
        far = (len(text) - 4, len(text))

        assert infer_intent_near(text, far).intent == ClinicalIntent.NEGATIVE_EXISTENCE


class TestGenericSemanticEnrichment:
    def test_positive_intent_adds_positive_assertion(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_site": ["Skin"]}},
            ]}},
            "patients with skin sample findings",
        )

        assert res.ok
        assert _has_in(_nested_children(res.wire), "sample_status", "Positive")

    def test_negative_intent_adds_negative_assertion(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_kind": ["Metastatic"]}},
            ]}},
            "patients without metastatic sample findings",
        )

        assert res.ok
        assert _has_in(_nested_children(res.wire), "sample_status", "Negative")

    def test_unknown_intent_adds_unknown_assertion(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_kind": ["Metastatic"]}},
            ]}},
            "patients with unknown metastatic sample status",
        )

        assert res.ok
        assert _has_in(_nested_children(res.wire), "sample_status", "Unknown")

    def test_record_exists_does_not_add_assertion(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_site": ["Skin"]}},
            ]}},
            "patients assessed for skin sample findings",
        )

        assert res.ok
        assert not any("sample_status" in child.get("IN", {}) for child in _nested_children(res.wire))

    def test_any_state_does_not_add_assertion(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_site": ["Skin"]}},
            ]}},
            "skin sample findings regardless of state",
        )

        assert res.ok
        assert not any("sample_status" in child.get("IN", {}) for child in _nested_children(res.wire))

    def test_explicit_state_is_not_overwritten(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_site": ["Skin"]}},
                {"IN": {"sample_status": ["Negative"]}},
            ]}},
            "patients with absent skin sample findings",
        )

        assert res.ok
        children = _nested_children(res.wire)
        assert _has_in(children, "sample_status", "Negative")
        assert not _has_in(children, "sample_status", "Positive")

    def test_conflicting_explicit_state_is_kept_and_reported(self):
        """A stated state beats one guessed from nearby wording.

        "exhibit an 11q deletion, and the result is absent" puts a positive cue
        right next to a negative finding; rejecting the filter there costs the
        user a cohort the query described perfectly clearly.
        """
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_site": ["Skin"]}},
                {"IN": {"sample_status": ["Negative"]}},
            ]}},
            "patients with skin sample findings",
        )

        assert res.ok
        assert _has_in(_nested_children(res.wire), "sample_status", "Negative")
        assert res.warnings and "sample_status" in res.warnings[0]

    def test_ambiguous_status_request_warns_but_still_builds(self):
        """An open-ended state is reported, not fatal.

        Refusing to build leaves the user with no cohort at all; the filter
        without an assertion is the record-existence reading, which is the
        safest of the candidates.
        """
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"IN": {"sample_site": ["Skin"]}},
            ]}},
            "skin sample status",
        )

        assert res.ok
        assert res.warnings and "sample_assessments" in res.warnings[0]
        # No assertion invented for a state the query never gave.
        children = _nested_children(res.wire)
        assert not any("sample_status" in c.get("IN", {}) for c in children)

    def test_missing_metadata_does_not_guess(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "other_assessments", "AND": [
                {"IN": {"other_site": ["Skin"]}},
            ]}},
            "patients with skin other assessment findings",
        )

        assert res.ok
        assert res.wire == {"nested": {"path": "other_assessments", "AND": [
            {"IN": {"other_site": ["Skin"]}},
        ]}}

    def test_negative_descriptor_not_equal_is_normalized_to_target_plus_absent(self):
        res = _generic_enricher().enrich(
            {"nested": {"path": "sample_assessments", "AND": [
                {"!=": {"sample_kind": "Metastatic"}},
            ]}},
            "patients without metastatic sample findings",
        )

        assert res.ok
        children = _nested_children(res.wire)
        assert _has_in(children, "sample_kind", "Metastatic")
        assert _has_in(children, "sample_status", "Negative")

    def test_same_path_nested_blocks_are_merged_before_assertion(self):
        res = _generic_enricher().enrich(
            {"AND": [
                {"nested": {"path": "sample_assessments", "AND": [
                    {"IN": {"sample_kind": ["Metastatic"]}},
                ]}},
                {"nested": {"path": "sample_assessments", "AND": [
                    {"IN": {"sample_site": ["Skin"]}},
                ]}},
            ]},
            "patients with metastatic sample findings on skin",
        )

        assert res.ok
        assert len([c for c in res.wire["AND"] if "nested" in c]) == 1
        children = _nested_children(res.wire)
        assert _has_in(children, "sample_kind", "Metastatic")
        assert _has_in(children, "sample_site", "Skin")
        assert _has_in(children, "sample_status", "Positive")

    def test_different_paths_are_not_merged(self):
        res = _generic_enricher().enrich(
            {"AND": [
                {"nested": {"path": "sample_assessments", "AND": [
                    {"IN": {"sample_site": ["Skin"]}},
                ]}},
                {"nested": {"path": "other_assessments", "AND": [
                    {"IN": {"other_site": ["Skin"]}},
                ]}},
            ]},
            "patients with skin sample and other assessment findings",
        )

        assert res.ok
        assert len([c for c in res.wire["AND"] if "nested" in c]) == 2

    def test_same_field_blocks_are_not_merged(self):
        """Two blocks constraining the same field are two records, not one.

        Merging them would AND both values against a single child document,
        which can never match -- "a skin sample and a bone sample" would come
        back empty instead of returning the patients who have both.
        """
        res = _generic_enricher().enrich(
            {"AND": [
                {"nested": {"path": "sample_assessments", "AND": [
                    {"IN": {"sample_site": ["Skin"]}},
                ]}},
                {"nested": {"path": "sample_assessments", "AND": [
                    {"IN": {"sample_site": ["Bone"]}},
                ]}},
            ]},
            "patients with a skin sample and a bone sample",
        )

        assert res.ok
        assert len([c for c in res.wire["AND"] if "nested" in c]) == 2


class TestPcdcTumorAssessmentRegression:
    @pytest.fixture(scope="class")
    def schema(self):
        repo = Path(__file__).resolve().parents[2]
        return SchemaIndex.from_files(
            repo / "schema" / DEFAULT_PCDC_SCHEMA,
            repo / "schema" / DEFAULT_GITOPS,
        )

    @pytest.fixture(scope="class")
    def enricher(self, schema):
        return SemanticEnricher.from_yaml(default_semantic_metadata_path(), schema)

    def test_mixed_polarity_resolves_each_path_separately(self, schema, enricher):
        """One assertion, one denial, one sentence.

        Against live PCDC data the correct cohort is 4930 subjects; applying a
        single query-wide intent to both blocks returned 6872.
        """
        text = (
            "INRG patients with metastatic tumors who do not exhibit "
            "MYCN amplification"
        )
        wire = {"AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_classification": ["Metastatic"]}},
            ]}},
            {"nested": {"path": "molecular_analysis", "AND": [
                {"IN": {"molecular_abnormality": ["MYCN Amplification"]}},
            ]}},
        ]}

        res = enricher.enrich(wire, text, nq=TermNormalizer(schema).normalize(text))

        assert res.ok
        assert _has_in(
            _nested_children(res.wire, "tumor_assessments"), "tumor_state", "Present"
        )
        assert _has_in(
            _nested_children(res.wire, "molecular_analysis"),
            "molecular_abnormality_result",
            "Absent",
        )

    def test_two_records_on_one_path_are_kept_apart(self, schema, enricher):
        """"A bone tumour and a bone marrow tumour" is 4150 subjects.

        Folding the two blocks together asks for one record carrying both sites,
        which no record can, so the cohort collapses to 0.
        """
        text = "INRG patients who have a bone tumor and a bone marrow tumor"
        wire = {"AND": [
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_site": ["Bone"]}},
                {"IN": {"tumor_state": ["Present"]}},
            ]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_site": ["Bone Marrow"]}},
                {"IN": {"tumor_state": ["Present"]}},
            ]}},
        ]}

        res = enricher.enrich(wire, text, nq=TermNormalizer(schema).normalize(text))

        assert res.ok
        assert len([c for c in res.wire["AND"] if "nested" in c]) == 2
        assert validate_dict(res.wire, schema).ok

    def test_positive_tumor_location_adds_present_in_same_block(self, schema, enricher):
        wire = {"AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_classification": ["Metastatic"]}},
                {"IN": {"tumor_site": ["Skin"]}},
            ]}},
        ]}

        res = enricher.enrich(
            wire,
            "Find INRG patients with metastatic tumors located on the skin.",
        )

        assert res.ok
        children = _nested_children(res.wire, "tumor_assessments")
        assert _has_in(children, "tumor_classification", "Metastatic")
        assert _has_in(children, "tumor_site", "Skin")
        assert _has_in(children, "tumor_state", "Present")
        assert validate_dict(res.wire, schema).ok

    def test_assessment_record_query_does_not_add_present(self, schema, enricher):
        wire = {"AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_classification": ["Metastatic"]}},
                {"IN": {"tumor_site": ["Skin"]}},
            ]}},
        ]}

        res = enricher.enrich(
            wire,
            "Find INRG patients assessed for metastatic tumors at the skin site.",
        )

        assert res.ok
        children = _nested_children(res.wire, "tumor_assessments")
        assert not any("tumor_state" in child.get("IN", {}) for child in children)
        assert validate_dict(res.wire, schema).ok

    def test_without_tumor_uses_real_negative_value(self, schema, enricher):
        wire = {"AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"!=": {"tumor_classification": "Metastatic"}},
                {"IN": {"tumor_site": ["Skin"]}},
            ]}},
        ]}

        res = enricher.enrich(
            wire,
            "Find INRG patients without metastatic tumors at the skin site.",
        )

        assert res.ok
        children = _nested_children(res.wire, "tumor_assessments")
        assert _has_in(children, "tumor_classification", "Metastatic")
        assert _has_in(children, "tumor_state", "Absent")
        assert validate_dict(res.wire, schema).ok

    def test_positive_molecular_finding_adds_present_result(self, schema, enricher):
        wire = {"nested": {"path": "molecular_analysis", "AND": [
            {"IN": {"molecular_abnormality": ["MYCN Amplification"]}},
        ]}}

        res = enricher.enrich(
            wire,
            "patients with the presence of MYCN amplification",
        )

        assert res.ok
        children = _nested_children(res.wire, "molecular_analysis")
        assert _has_in(children, "molecular_abnormality", "MYCN Amplification")
        assert _has_in(children, "molecular_abnormality_result", "Present")
        assert validate_dict(res.wire, schema).ok

    def test_negative_molecular_finding_uses_absent_result(self, schema, enricher):
        wire = {"nested": {"path": "molecular_analysis", "AND": [
            {"!=": {"molecular_abnormality": "17q gain"}},
        ]}}

        res = enricher.enrich(
            wire,
            "patients who do not exhibit 17q gain",
        )

        assert res.ok
        children = _nested_children(res.wire, "molecular_analysis")
        assert _has_in(children, "molecular_abnormality", "17q gain")
        assert _has_in(children, "molecular_abnormality_result", "Absent")
        assert validate_dict(res.wire, schema).ok

    def test_negative_molecular_result_not_present_becomes_absent(self, schema, enricher):
        wire = {"nested": {"path": "molecular_analysis", "AND": [
            {"IN": {"molecular_abnormality": ["MYCN Amplification"]}},
            {"!=": {"molecular_abnormality_result": "Present"}},
        ]}}

        res = enricher.enrich(
            wire,
            "participants exhibit an absence of MYCN Amplification",
        )

        assert res.ok
        children = _nested_children(res.wire, "molecular_analysis")
        assert _has_in(children, "molecular_abnormality_result", "Absent")
        assert not any("!=" in child for child in children)
        assert validate_dict(res.wire, schema).ok

    def test_molecular_any_state_adds_both_known_results(self, schema, enricher):
        wire = {"nested": {"path": "molecular_analysis", "AND": [
            {"IN": {"molecular_abnormality": ["11q deletion"]}},
        ]}}

        res = enricher.enrich(
            wire,
            "11q deletion with presence or absence considered",
        )

        assert res.ok
        children = _nested_children(res.wire, "molecular_analysis")
        assert any(
            child.get("IN", {}).get("molecular_abnormality_result")
            == ["Absent", "Present"]
            for child in children
        )
        assert validate_dict(res.wire, schema).ok

    def test_tumor_assessment_phrase_does_not_override_molecular_presence(self):
        got = infer_intent(
            "age at tumor assessment with the presence of an 11q deletion"
        )

        assert got.intent == ClinicalIntent.POSITIVE_EXISTENCE

    def test_filter_generator_runs_enrichment_before_validation(self, schema):
        class FakeRetriever:
            def retrieve(self, nq, top_k):
                return []

        def chat(_messages, _response_format):
            return json.dumps({
                "intent": "positive_existence",
                "state_explicit": False,
                "ambiguity": [],
                "filter": {
                    "op": "AND",
                    "clauses": [
                        {"op": "IN", "field": "consortium", "values": ["INRG"]},
                        {
                            "op": "nested",
                            "path": "tumor_assessments",
                            "body": {
                                "op": "AND",
                                "clauses": [
                                    {
                                        "op": "IN",
                                        "field": "tumor_classification",
                                        "values": ["Metastatic"],
                                    },
                                    {
                                        "op": "IN",
                                        "field": "tumor_site",
                                        "values": ["Skin"],
                                    },
                                ],
                            },
                        },
                    ],
                },
            })

        generator = FilterGenerator(
            schema,
            TermNormalizer(schema),
            FakeRetriever(),
            config=GeneratorConfig(max_attempts=1),
            chat_fn=chat,
            semantic_enricher=SemanticEnricher.from_yaml(
                default_semantic_metadata_path(),
                schema,
            ),
        )

        res = generator.generate(
            "Find INRG patients with metastatic tumors located on the skin."
        )

        assert res.ok
        assert res.semantic_intent == "positive_existence"
        assert _has_in(_nested_children(res.wire, "tumor_assessments"), "tumor_state", "Present")

    def test_generator_restores_nonnegated_recognized_enum(self, schema):
        class FakeRetriever:
            def retrieve(self, nq, top_k):
                return []

        def chat(_messages, _response_format):
            return json.dumps({
                "intent": "state_unspecified",
                "state_explicit": False,
                "ambiguity": [],
                "filter": {
                    "op": "!=",
                    "field": "ethnicity",
                    "value": "Hispanic or Latino",
                },
            })

        generator = FilterGenerator(
            schema,
            TermNormalizer(schema),
            FakeRetriever(),
            config=GeneratorConfig(max_attempts=1),
            chat_fn=chat,
        )

        res = generator.generate(
            "male participants who are not Hispanic or Latino"
        )

        assert res.ok
        assert res.wire == {"IN": {"ethnicity": ["Not Hispanic or Latino"]}}

    def test_generator_accepts_missing_optional_model_metadata(self, schema):
        class FakeRetriever:
            def retrieve(self, nq, top_k):
                return []

        def chat(_messages, _response_format):
            return json.dumps({
                "filter": {
                    "op": "IN",
                    "field": "sex",
                    "values": ["Male"],
                },
            })

        generator = FilterGenerator(
            schema,
            TermNormalizer(schema),
            FakeRetriever(),
            config=GeneratorConfig(max_attempts=1),
            chat_fn=chat,
        )

        res = generator.generate("male participants")

        assert res.ok
        assert res.wire == {"IN": {"sex": ["Male"]}}

    def test_generator_merges_disjunctive_same_field_and(self, schema):
        class FakeRetriever:
            def retrieve(self, nq, top_k):
                return []

        def chat(_messages, _response_format):
            return json.dumps({
                "intent": "positive_existence",
                "state_explicit": False,
                "ambiguity": [],
                "filter": {
                    "op": "nested",
                    "path": "molecular_analysis",
                    "body": {
                        "op": "AND",
                        "clauses": [
                            {
                                "op": "IN",
                                "field": "molecular_abnormality",
                                "values": ["11q deletion"],
                            },
                            {
                                "op": "IN",
                                "field": "molecular_abnormality",
                                "values": ["1p deletion"],
                            },
                        ],
                    },
                },
            })

        generator = FilterGenerator(
            schema,
            TermNormalizer(schema),
            FakeRetriever(),
            config=GeneratorConfig(max_attempts=1),
            chat_fn=chat,
            semantic_enricher=SemanticEnricher.from_yaml(
                default_semantic_metadata_path(),
                schema,
            ),
        )

        res = generator.generate(
            "patients who exhibit either an 11q deletion or a 1p deletion"
        )

        assert res.ok
        children = _nested_children(res.wire, "molecular_analysis")
        assert any(
            child.get("IN", {}).get("molecular_abnormality")
            == ["11q deletion", "1p deletion"]
            for child in children
        )
        assert _has_in(children, "molecular_abnormality_result", "Present")

    def test_conjunctive_same_field_and_is_not_merged(self, schema):
        """An "and" between the two values is a conflict, not a choice.

        The merge is judged on the span the values occupy, so an unrelated "or"
        elsewhere in the sentence must not turn "bone and bone marrow" into
        "bone or bone marrow" -- two sibling nested blocks are how "both" is
        expressed, and folding them changes the cohort.
        """
        class FakeRetriever:
            def retrieve(self, nq, top_k):
                return []

        def chat(_messages, _response_format):
            return json.dumps({
                "intent": "positive_existence",
                "state_explicit": False,
                "ambiguity": [],
                "filter": {
                    "op": "nested",
                    "path": "tumor_assessments",
                    "body": {
                        "op": "AND",
                        "clauses": [
                            {"op": "IN", "field": "tumor_site", "values": ["Bone"]},
                            {"op": "IN", "field": "tumor_site", "values": ["Bone Marrow"]},
                        ],
                    },
                },
            })

        generator = FilterGenerator(
            schema,
            TermNormalizer(schema),
            FakeRetriever(),
            config=GeneratorConfig(max_attempts=1),
            chat_fn=chat,
            semantic_enricher=SemanticEnricher.from_yaml(
                default_semantic_metadata_path(),
                schema,
            ),
        )

        res = generator.generate(
            "patients whose tumor was found in bone and bone marrow, male or female"
        )

        # Left unmerged, so the validator gets to reject the impossible AND
        # instead of it being silently rewritten into a disjunction.
        assert not res.ok
        assert any(issue.code == CODE_CONFLICTING_IN for issue in res.validation.issues)

    def test_generator_propagates_one_phase_to_each_compatible_nested_block(self, schema):
        class FakeRetriever:
            def retrieve(self, nq, top_k):
                return []

        def chat(_messages, _response_format):
            return json.dumps({
                "intent": "positive_existence",
                "state_explicit": False,
                "ambiguity": [],
                "filter": {
                    "op": "AND",
                    "clauses": [
                        {
                            "op": "nested",
                            "path": "stagings",
                            "body": {
                                "op": "AND",
                                "clauses": [{
                                    "op": "IN",
                                    "field": "disease_phase",
                                    "values": ["Initial Diagnosis"],
                                }],
                            },
                        },
                        {
                            "op": "nested",
                            "path": "molecular_analysis",
                            "body": {
                                "op": "AND",
                                "clauses": [{
                                    "op": "IN",
                                    "field": "molecular_abnormality",
                                    "values": ["11q deletion"],
                                }],
                            },
                        },
                        {
                            "op": "nested",
                            "path": "tumor_assessments",
                            "body": {
                                "op": "AND",
                                "clauses": [{
                                    "op": "IN",
                                    "field": "tumor_classification",
                                    "values": ["Metastatic"],
                                }],
                            },
                        },
                    ],
                },
            })

        generator = FilterGenerator(
            schema,
            TermNormalizer(schema),
            FakeRetriever(),
            config=GeneratorConfig(max_attempts=1),
            chat_fn=chat,
            semantic_enricher=SemanticEnricher.from_yaml(
                default_semantic_metadata_path(),
                schema,
            ),
        )

        res = generator.generate(
            "patients with metastatic tumors and 11q deletion at Initial Diagnosis"
        )

        assert res.ok
        for path in ("stagings", "molecular_analysis", "tumor_assessments"):
            children = _nested_children(res.wire, path)
            assert _has_in(children, "disease_phase", "Initial Diagnosis")
