import hashlib
import re

import numpy as np
import pytest

from services.schema_loader import FieldSpec, SchemaIndex
from services.term_normalizer import NormalizedQuery, RecognizedTerm, FieldPlacement
from services.candidate_retriever import CandidateRetriever


def _bucket(word: str, dim: int) -> int:
    return int(hashlib.sha1(word.encode("utf-8")).hexdigest(), 16) % dim


def _bow_embedder(dim: int = 512):
    """Deterministic bag-of-words: word overlap drives similarity. Enough to
    prove ranking without calling OpenAI."""

    def embed(texts):
        out = []
        for text in texts:
            vec = [0.0] * dim
            for word in re.findall(r"[a-z0-9]+", text.lower()):
                vec[_bucket(word, dim)] += 1.0
            out.append(vec)
        return out

    return embed


@pytest.fixture
def schema() -> SchemaIndex:
    fields = {
        (None, "sex"): FieldSpec("sex", "enum", ("Male", "Female"),
                                 "Biological sex of the subject", None),
        (None, "consortium"): FieldSpec("consortium", "enum", ("INRG", "NODAL"),
                                        "Research consortium membership", None),
        (None, "race"): FieldSpec("race", "enum", ("White", "Not Reported"),
                                  "Race of the subject", None),
        (None, "age_at_enrollment"): FieldSpec("age_at_enrollment", "number", (),
                                               "Age in days at enrollment", None),
    }
    return SchemaIndex(fields)


@pytest.fixture
def retriever(schema):
    return CandidateRetriever(schema, embed_fn=_bow_embedder())


class TestRanking:
    def test_value_word_finds_its_field(self, retriever):
        top = retriever.retrieve("find male patients", top_k=2)
        assert top[0].field == "sex"

    def test_consortium_name_ranks_consortium(self, retriever):
        top = retriever.retrieve("patients in the INRG consortium", top_k=2)
        assert top[0].field == "consortium"

    def test_top_k_caps_embedding_picks(self, retriever):
        out = retriever.retrieve("anything", top_k=1, include_placed=False)
        assert len(out) == 1


class TestValidation:
    def test_top_k_must_be_positive(self, retriever):
        with pytest.raises(ValueError):
            retriever.retrieve("anything", top_k=0)
        with pytest.raises(ValueError):
            retriever.retrieve("anything", top_k=-1)

    def test_embedder_count_mismatch_is_caught(self, schema):
        def _short(texts):
            return [[1.0, 0.0]]  # one vector no matter how many texts

        r = CandidateRetriever(schema, embed_fn=_short)
        with pytest.raises(RuntimeError):
            r.build()


class TestPlacements:
    def test_placed_field_always_present_at_full_score(self, retriever):
        nq = NormalizedQuery(
            text="find female patients",
            terms=[RecognizedTerm(
                value="Female",
                placements=(FieldPlacement(field="sex", path=None),),
                span=(5, 11),
            )],
            ranges=[],
            negations=[],
        )
        out = retriever.retrieve(nq, top_k=1)  # even with top_k=1 it must survive
        sex = [c for c in out if c.field == "sex"]
        assert sex and sex[0].score == 1.0


class TestCache:
    def test_corpus_embedded_once_then_loaded_from_disk(self, schema, tmp_path):
        r1 = CandidateRetriever(schema, embed_fn=_bow_embedder(),
                                cache_dir=tmp_path, cache_namespace="bow-test")
        r1.build()
        assert len(list(tmp_path.glob("field_embeddings.*.npy"))) == 1

        def _must_not_embed(texts):
            raise AssertionError("corpus should have loaded from cache")

        r2 = CandidateRetriever(schema, embed_fn=_must_not_embed,
                                cache_dir=tmp_path, cache_namespace="bow-test")
        r2._ensure_matrix()  # would raise if it re-embedded
        assert r2._matrix is not None

    def test_corrupt_cache_is_rebuilt(self, schema, tmp_path):
        r = CandidateRetriever(schema, embed_fn=_bow_embedder(),
                               cache_dir=tmp_path, cache_namespace="bow-test")
        bad = r._cache_file()
        bad.parent.mkdir(parents=True, exist_ok=True)
        np.save(bad, np.zeros((1, 1), dtype=np.float32))  # wrong shape
        r._ensure_matrix()  # detect mismatch and rebuild
        assert r._matrix.shape[0] == len(r._docs)