"""
Find schema fields that are likely relevant to a user query.

Each schema field is turned into a small text document and embedded once.
Queries are embedded at runtime and matched against those field vectors.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Tuple, Union

import numpy as np

from services.schema_loader import FieldSpec, SchemaIndex
from services.term_normalizer import NormalizedQuery


DEFAULT_EMBED_MODEL = "text-embedding-3-small"

_MAX_ENUM_IN_DOC = 60
_EMBED_BATCH = 256

EmbedFn = Callable[[Sequence[str]], List[List[float]]]


@dataclass(frozen=True)
class FieldCandidate:
    """A schema field returned by retrieval."""

    field: str
    path: Optional[str]
    field_type: str
    description: str
    enum_values: tuple[str, ...]
    score: float

    def as_dict(self) -> dict:
        return {
            "field": self.field,
            "path": self.path,
            "field_type": self.field_type,
            "description": self.description,
            "enum_values": list(self.enum_values),
            "score": round(self.score, 4),
        }


def _l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """Normalize rows so dot product can be used as cosine similarity."""
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


class CandidateRetriever:
    def __init__(
        self,
        schema: SchemaIndex,
        *,
        embed_fn: Optional[EmbedFn] = None,
        client=None,
        model: str = DEFAULT_EMBED_MODEL,
        cache_dir: Optional[Union[str, Path]] = None,
        cache_namespace: Optional[str] = None,
        max_enum_values: int = _MAX_ENUM_IN_DOC,
    ):
        self.schema = schema
        self.model = model
        self.max_enum_values = max_enum_values

        self._embed_fn = embed_fn
        self._client = client
        self._cache_dir = Path(cache_dir) if cache_dir else None

        # Keep cache files separate across embedding backends.
        self._cache_namespace = cache_namespace or model

        self._specs: List[FieldSpec] = list(schema.all_fields())
        self._docs: List[str] = [self._doc_text(s) for s in self._specs]
        self._by_key = {(s.parent_path, s.name): s for s in self._specs}
        self._matrix: Optional[np.ndarray] = None

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        **kwargs,
    ) -> "CandidateRetriever":
        schema = SchemaIndex.from_files(pcdc_path, gitops_path)
        return cls(schema, **kwargs)

    def build(self) -> None:
        """Build field embeddings before the first query."""
        self._ensure_matrix()

    def _doc_text(self, spec: FieldSpec) -> str:
        """Build the text representation used for embedding one field."""
        head_parts: List[str] = []
        if spec.parent_path:
            head_parts.append(spec.parent_path.replace("_", " "))
        head_parts.append(spec.name.replace("_", " "))

        pieces = [" ".join(head_parts)]
        if spec.description:
            pieces.append(spec.description)
        if spec.enum_values:
            shown = spec.enum_values[: self.max_enum_values]
            pieces.append("values: " + ", ".join(shown))
        return ". ".join(pieces)

    def _embed(self, texts: Sequence[str]) -> List[List[float]]:
        if self._embed_fn is not None:
            return self._embed_fn(texts)

        client = self._get_client()
        vectors: List[List[float]] = []
        for start in range(0, len(texts), _EMBED_BATCH):
            batch = list(texts[start : start + _EMBED_BATCH])
            resp = client.embeddings.create(model=self.model, input=batch)
            vectors.extend(item.embedding for item in resp.data)
        return vectors

    def _embed_matrix(self, texts: Sequence[str]) -> np.ndarray:
        """Embed text and return a normalized matrix."""
        raw = self._embed(texts)
        if len(raw) != len(texts):
            raise RuntimeError(
                f"embedder returned {len(raw)} vectors for {len(texts)} texts"
            )

        matrix = np.asarray(raw, dtype=np.float32)
        if matrix.ndim != 2 or matrix.shape[0] != len(texts):
            raise RuntimeError(
                f"expected a 2-D ({len(texts)}, dim) embedding matrix, "
                f"got shape {matrix.shape}"
            )

        return _l2_normalize(matrix)

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI

            self._client = OpenAI()
        return self._client

    def _cache_file(self) -> Optional[Path]:
        if self._cache_dir is None:
            return None

        # Include the namespace, model, and field docs in the cache key.
        digest = hashlib.sha256()
        digest.update(self._cache_namespace.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(self.model.encode("utf-8"))

        for doc in self._docs:
            digest.update(b"\x00")
            digest.update(doc.encode("utf-8"))

        return self._cache_dir / f"field_embeddings.{digest.hexdigest()[:16]}.npy"

    def _ensure_matrix(self) -> None:
        if self._matrix is not None:
            return

        cache = self._cache_file()
        if cache is not None and cache.exists():
            loaded = np.load(cache)

            # Ignore a bad cache file and rebuild it.
            if loaded.ndim == 2 and loaded.shape[0] == len(self._docs):
                self._matrix = loaded
                return

        self._matrix = self._embed_matrix(self._docs)

        if cache is not None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            np.save(cache, self._matrix)

    def retrieve(
        self,
        query: Union[str, NormalizedQuery],
        *,
        top_k: int = 12,
        include_placed: bool = True,
    ) -> List[FieldCandidate]:
        """Return the top schema fields for a query."""
        if top_k < 1:
            raise ValueError(f"top_k must be >= 1, got {top_k}")

        self._ensure_matrix()
        query_text, placed_keys = self._unpack(query, include_placed)

        qvec = self._embed_matrix([query_text])[0]
        scores = self._matrix @ qvec

        ranked: dict[Tuple[Optional[str], str], float] = {}
        for idx in np.argsort(-scores)[:top_k]:
            spec = self._specs[int(idx)]
            ranked[(spec.parent_path, spec.name)] = float(scores[idx])

        # Keep exact matches from the normalizer at the top.
        for key in placed_keys:
            ranked[key] = max(ranked.get(key, 0.0), 1.0)

        candidates = [self._candidate(key, score) for key, score in ranked.items()]
        candidates.sort(key=lambda c: c.score, reverse=True)
        return candidates

    def _unpack(
        self,
        query: Union[str, NormalizedQuery],
        include_placed: bool,
    ) -> Tuple[str, set]:
        if isinstance(query, NormalizedQuery):
            bits = [query.text]
            bits.extend(term.value for term in query.terms)
            text = " ".join(bit for bit in bits if bit)

            placed: set = set()
            if include_placed:
                for term in query.terms:
                    for placement in term.placements:
                        key = (placement.path, placement.field)
                        if key in self._by_key:
                            placed.add(key)

            return text, placed

        return str(query), set()

    def _candidate(
        self, key: Tuple[Optional[str], str], score: float
    ) -> FieldCandidate:
        spec = self._by_key[key]
        return FieldCandidate(
            field=spec.name,
            path=spec.parent_path,
            field_type=spec.field_type,
            description=spec.description,
            enum_values=spec.enum_values,
            score=score,
        )

