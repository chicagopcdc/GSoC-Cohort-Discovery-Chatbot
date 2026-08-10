"""Tool 1: grounded answers from a small local knowledge base."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union


_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_MAX_SNIPPET_CHARS = 900

_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "can", "do", "does",
    "for", "from", "how", "i", "in", "is", "it", "me", "of", "on", "or",
    "our", "please", "tell", "that", "the", "this", "to", "use", "what",
    "when", "where", "which", "who", "why", "with", "you", "your",
}

_QUESTION_CUES = {
    "what", "which", "who", "where", "why", "how", "explain", "describe",
    "tell", "list", "help",
}

_TOPIC_TERMS = {
    "pcdc", "d4cg", "portal", "chatbot", "assistant", "guppy", "gen3",
    "credential", "credentials", "access", "documentation", "docs", "data",
    "dictionary", "dictionaries", "consortium", "consortia", "filter",
    "filters", "cohort", "cohorts", "tool", "tools", "limitation",
    "limitations",
}

_COHORT_BUILD_STARTS = (
    "find patients", "find male patients", "find female patients",
    "find all patients", "find subjects", "find participants",
    "show me patients", "show patients", "return patients",
    "select patients", "get patients",
)


@dataclass(frozen=True)
class KnowledgeSnippet:
    title: str
    heading: str
    text: str
    path: str
    source_urls: Tuple[str, ...] = ()

    @property
    def label(self) -> str:
        return f"{self.title} > {self.heading}" if self.heading else self.title


@dataclass
class KnowledgeAnswer:
    kind: str
    text: str
    sources: List[str] = field(default_factory=list)
    snippets: List[Dict[str, str]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.kind == "answer"

    def as_dict(self) -> dict:
        out = {
            "kind": self.kind,
            "text": self.text,
            "sources": list(self.sources),
            "snippets": list(self.snippets),
        }
        if self.warnings:
            out["warnings"] = list(self.warnings)
        return out


class KnowledgeQA:
    def __init__(
        self,
        snippets: Sequence[KnowledgeSnippet],
        *,
        max_snippets: int = 3,
        min_score: float = 2.0,
    ):
        if max_snippets < 1:
            raise ValueError(f"max_snippets must be >= 1, got {max_snippets}")
        self.snippets = list(snippets)
        self.max_snippets = max_snippets
        self.min_score = min_score
        self._snippet_tokens = [Counter(_tokens(s.text + " " + s.title + " " + s.heading))
                                for s in self.snippets]

    @classmethod
    def from_dir(
        cls,
        directory: Union[str, Path],
        *,
        max_snippets: int = 3,
        min_score: float = 2.0,
    ) -> "KnowledgeQA":
        root = Path(directory)
        snippets: List[KnowledgeSnippet] = []
        if root.exists():
            for path in sorted(root.glob("*.md")):
                snippets.extend(_parse_markdown(path))
        return cls(snippets, max_snippets=max_snippets, min_score=min_score)

    def answer(self, question: str) -> KnowledgeAnswer:
        question = (question or "").strip()
        if not question:
            return KnowledgeAnswer(
                "no_match",
                "Question must not be empty.",
            )
        if not self.snippets:
            return KnowledgeAnswer(
                "no_match",
                "No curated knowledge documents are configured.",
            )

        q_tokens = Counter(_tokens(question))
        ranked = self._rank(q_tokens)
        if not ranked or ranked[0][0] < self.min_score:
            return KnowledgeAnswer(
                "no_match",
                "I do not have enough curated documentation to answer that safely.",
            )

        picked = [snippet for _, snippet in ranked[: self.max_snippets]]
        parts: List[str] = []
        sources: List[str] = []
        snippet_dicts: List[Dict[str, str]] = []
        for snippet in picked:
            text = _shorten(_clean_text(snippet.text), _MAX_SNIPPET_CHARS)
            parts.append(text)
            sources.append(snippet.label)
            snippet_dicts.append({
                "source": snippet.label,
                "path": snippet.path,
                "text": text,
            })

        answer = "\n\n".join(parts)
        answer += "\n\nSources: " + "; ".join(dict.fromkeys(sources))
        return KnowledgeAnswer("answer", answer, list(dict.fromkeys(sources)), snippet_dicts)

    def _rank(self, q_tokens: Counter) -> List[Tuple[float, KnowledgeSnippet]]:
        scored: List[Tuple[float, KnowledgeSnippet]] = []
        for snippet, s_tokens in zip(self.snippets, self._snippet_tokens):
            score = _score(q_tokens, s_tokens)
            if score > 0:
                scored.append((score, snippet))
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored

    def looks_like_knowledge_question(self, text: str) -> bool:
        """Conservative check for the API fast path."""
        return _looks_like_knowledge_question(text)


def _parse_markdown(path: Path) -> List[KnowledgeSnippet]:
    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()
    title = path.stem.replace("_", " ").title()
    source_urls: List[str] = []
    for line in lines:
        if line.startswith("# "):
            title = line[2:].strip()
        elif line.strip().startswith("- http"):
            source_urls.append(line.strip()[2:])

    chunks: List[KnowledgeSnippet] = []
    heading = ""
    buf: List[str] = []

    def flush() -> None:
        text = "\n".join(buf).strip()
        if text:
            chunks.append(KnowledgeSnippet(
                title=title,
                heading=heading,
                text=text,
                path=str(path),
                source_urls=tuple(source_urls),
            ))

    for line in lines:
        if line.startswith("# "):
            continue
        if line.startswith("## "):
            flush()
            heading = line[3:].strip()
            buf = []
            continue
        if line.startswith("Source URLs:") or line.startswith("Last reviewed:"):
            continue
        if line.strip().startswith("- http"):
            continue
        buf.append(line)
    flush()
    return chunks


def _tokens(text: str) -> List[str]:
    return [t for t in (m.group(0).lower() for m in _TOKEN_RE.finditer(text))
            if t not in _STOPWORDS and len(t) > 1]


def _score(query: Counter, snippet: Counter) -> float:
    score = 0.0
    for token, q_count in query.items():
        if token in snippet:
            score += min(q_count, snippet[token])
            score += min(snippet[token], 5) * 0.25
            if token in _TOPIC_TERMS:
                score += 1.5
    return score


def _is_cohort_build(text: str) -> bool:
    lower = text.lower().strip()
    return any(lower.startswith(prefix) for prefix in _COHORT_BUILD_STARTS)


def _looks_like_knowledge_question(text: str) -> bool:
    if _is_cohort_build(text):
        return False

    lower = text.lower().strip()
    raw_tokens = [m.group(0).lower() for m in _TOKEN_RE.finditer(text)]
    tokens = _tokens(text)
    if not tokens:
        return False
    token_set = set(tokens)
    has_topic = bool(token_set & _TOPIC_TERMS)
    first = raw_tokens[0] if raw_tokens else ""
    has_question_cue = first in _QUESTION_CUES or any(
        lower.startswith(prefix)
        for prefix in (
            "what is", "what are", "what does", "which ", "how do", "how can",
            "can this", "find documentation", "find docs", "find information",
        )
    )
    return (has_topic and has_question_cue) or ("pcdc" in token_set and len(tokens) <= 5)


def _clean_text(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    return " ".join(line for line in lines if line)


def _shorten(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0].rstrip()
    return cut + "..."


__all__ = ["KnowledgeQA", "KnowledgeAnswer", "KnowledgeSnippet"]
