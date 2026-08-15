import sys
from pathlib import Path


_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from collections import Counter

from services.knowledge_qa import KnowledgeQA, KnowledgeSnippet, _tokens


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
        # The values a filter may actually use, not just the consortium names.
        assert "HIBISCUS" in answer.text
        assert all(s.startswith("PCDC consortia") for s in answer.sources)

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
        qa = KnowledgeQA([snip], min_score=0.9)
        assert qa.answer("alpha epsilon zeta eta").kind == "no_match"

    def test_a_short_question_can_be_answered(self):
        """Scores are a share of the question, not a sum over matched words.

        Under the old sum, a fixed threshold was unreachable for a two-word
        question however well it matched, so short phrasings were refused while
        long ones passed on weaker evidence.
        """
        snip = KnowledgeSnippet(title="Guppy", heading="Endpoint",
                                text="Guppy answers aggregate cohort counts.",
                                path="doc.md")
        qa = KnowledgeQA([snip])

        assert qa.answer("guppy counts").ok

    def test_words_the_corpus_never_saw_lower_the_score(self):
        """Unmatched words are evidence against, which is what refuses off-topic
        questions rather than answering them from whatever words overlap."""
        snip = KnowledgeSnippet(title="Guppy", heading="Endpoint",
                                text="Guppy answers aggregate cohort counts.",
                                path="doc.md")
        qa = KnowledgeQA([snip])

        focused = qa._rank(Counter(_tokens("guppy counts")))[0][0]
        padded = qa._rank(Counter(_tokens("guppy counts sourdough bicycle opera")))[0][0]

        assert padded < focused

    def test_topic_words_do_not_need_a_curated_list(self):
        """Term weight comes from the corpus, so a new topic is searchable at once."""
        snips = [
            KnowledgeSnippet(title="A", heading="Vocabulary",
                             text="Radiomics segmentation is described here.",
                             path="a.md"),
            KnowledgeSnippet(title="B", heading="Other",
                             text="Something entirely unrelated.", path="b.md"),
        ]
        qa = KnowledgeQA(snips)
        answer = qa.answer("What is radiomics segmentation?")

        assert answer.ok
        assert answer.sources[0] == "A > Vocabulary"

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

    def test_non_url_source_lines_do_not_become_answers(self, tmp_path):
        """Front-matter source entries are provenance, never an answer.

        Only "- http" lines were dropped, so a "- local implementation: ..."
        entry survived as its own chunk and outranked real sections for
        questions about the document's own topic -- asking what the chatbot
        does returned a list of source file paths.
        """
        (tmp_path / "note.md").write_text(
            "# Chatbot capabilities\n\n"
            "Source URLs:\n"
            "- https://example.org/a\n"
            "- local implementation: src/backend/services/agent.py\n\n"
            "## What it does\nIt builds validated cohort filters.\n",
            encoding="utf-8",
        )
        qa = KnowledgeQA.from_dir(tmp_path)

        assert [s.heading for s in qa.snippets] == ["What it does"]
        assert all("local implementation" not in s.text for s in qa.snippets)
