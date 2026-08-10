import sys
from pathlib import Path


_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.knowledge_qa import KnowledgeQA, KnowledgeSnippet


_DOCS = _BACKEND / "data" / "knowledge"


class TestCuratedKnowledgeBase:
    def test_answers_pcdc_overview_with_sources(self):
        qa = KnowledgeQA.from_dir(_DOCS)
        answer = qa.answer("What is PCDC?")

        assert answer.ok
        assert "Pediatric Cancer Data Commons" in answer.text
        assert answer.sources
        assert "Sources:" in answer.text

    def test_answers_available_consortia_from_local_schema_note(self):
        qa = KnowledgeQA.from_dir(_DOCS)
        answer = qa.answer("Which consortia are available?")

        assert answer.ok
        assert "INRG" in answer.text
        assert "NODAL" in answer.text
        assert any("Locally filterable consortium values" in s for s in answer.sources)

    def test_find_documentation_phrasing_is_answered_when_covered(self):
        qa = KnowledgeQA.from_dir(_DOCS)
        answer = qa.answer("Find documentation about Guppy.")

        assert answer.ok
        assert "Guppy" in answer.text
        assert answer.sources

    def test_uncovered_topic_is_not_guessed(self):
        qa = KnowledgeQA.from_dir(_DOCS)
        answer = qa.answer("What is the capital of France?")

        assert not answer.ok
        assert answer.kind == "no_match"

    def test_empty_directory_is_safe(self, tmp_path):
        qa = KnowledgeQA.from_dir(tmp_path)
        answer = qa.answer("What is PCDC?")

        assert not answer.ok
        assert "No curated knowledge documents" in answer.text

    def test_yes_no_phrasing_is_answered_when_covered(self):
        qa = KnowledgeQA.from_dir(_DOCS)
        for q in ("Is my line-level data access restricted?",
                  "Does the chatbot report real counts without credentials?"):
            answer = qa.answer(q)
            assert answer.ok, q
            assert answer.sources


class TestGroundingAndRouting:
    def test_min_score_threshold_refuses_weak_match(self):
        snip = KnowledgeSnippet(title="Doc", heading="H",
                                text="alpha beta gamma delta", path="doc.md")
        qa = KnowledgeQA([snip], min_score=2.0)
        assert qa.answer("What about alpha only").kind == "no_match"

    def test_looks_like_knowledge_question_is_conservative(self):
        qa = KnowledgeQA.from_dir(_DOCS)
        assert qa.looks_like_knowledge_question("What is PCDC?")
        assert not qa.looks_like_knowledge_question("Find male patients from INRG")
        assert qa.looks_like_knowledge_question("Find documentation about Guppy")
        assert not qa.looks_like_knowledge_question("Is my data access restricted?")

    def test_parse_markdown_splits_sections_and_drops_metadata(self, tmp_path):
        (tmp_path / "note.md").write_text(
            "# My Title\n\n"
            "Last reviewed: 2026-01-01\n\n"
            "Source URLs:\n- https://example.org/a\n\n"
            "## Section one\nText for one.\n\n"
            "## Section two\nText for two.\n",
            encoding="utf-8",
        )
        qa = KnowledgeQA.from_dir(tmp_path)
        assert {s.title for s in qa.snippets} == {"My Title"}
        assert sorted(s.heading for s in qa.snippets) == ["Section one", "Section two"]
        assert all("http" not in s.text and "Last reviewed" not in s.text
                   for s in qa.snippets)
        assert any("https://example.org/a" in s.source_urls for s in qa.snippets)
