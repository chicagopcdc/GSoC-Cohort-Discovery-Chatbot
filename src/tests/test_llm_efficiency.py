import json
import asyncio
import sys, os

# ensure backend package can be imported when running tests from src/tests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.utils.nested_graphql_helper import (
    resolve_pcdc_ambiguities,
    resolve_gitops_ambiguities,
)


def test_resolve_pcdc_ambiguities_batches_with_single_llm_call():
    calls = []

    class FakeLLM:
        def invoke(self, prompt):
            calls.append(prompt)
            # Extract the ambiguous terms dict from the prompt
            try:
                # Find the JSON after "Ambiguous terms and options:"
                start_marker = "Ambiguous terms and options:"
                start_idx = prompt.find(start_marker)
                if start_idx == -1:
                    raise ValueError("Marker not found")
                start_idx += len(start_marker)
                
                # Find the first complete JSON object
                rest = prompt[start_idx:]
                first_brace = rest.find('{')
                if first_brace == -1:
                    raise ValueError("No JSON object found")
                
                # Count braces to find matching closing brace
                brace_count = 0
                end_idx = -1
                for i, char in enumerate(rest[first_brace:]):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = first_brace + i + 1
                            break
                
                if end_idx == -1:
                    raise ValueError("Could not find matching closing brace")
                
                json_str = rest[first_brace:end_idx]
                data = json.loads(json_str)
            except Exception as e:
                print(f"Error parsing prompt JSON: {e}")
                print(f"Prompt: {prompt}")
                raise
            # pick first candidate for each term
            result = {k: v[0] if isinstance(v, list) and v else '' for k, v in data.items()}
            class R:
                pass
            r = R()
            r.content = json.dumps(result)
            return r

    llm = FakeLLM()
    keywords = ['Cancer', 'Tumor']
    mapping_dict = {'cancer': ['disease', 'condition'], 'tumor': ['growth', 'neoplasm']}
    res = asyncio.run(resolve_pcdc_ambiguities(keywords, mapping_dict, 'some query', llm))
    assert res == {'Cancer': 'disease', 'Tumor': 'growth'}
    assert len(calls) == 1, "LLM should be called only once for batch resolution"


def test_resolve_gitops_ambiguities_batches_with_single_llm_call():
    calls = []

    class FakeLLM:
        def invoke(self, prompt):
            calls.append(prompt)
            try:
                # Find the JSON after "Ambiguous properties and choices:"
                start_marker = "Ambiguous properties and choices:"
                start_idx = prompt.find(start_marker)
                if start_idx == -1:
                    raise ValueError("Marker not found")
                start_idx += len(start_marker)
                
                # Find the first complete JSON object
                rest = prompt[start_idx:]
                first_brace = rest.find('{')
                if first_brace == -1:
                    raise ValueError("No JSON object found")
                
                # Count braces to find matching closing brace
                brace_count = 0
                end_idx = -1
                for i, char in enumerate(rest[first_brace:]):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = first_brace + i + 1
                            break
                
                if end_idx == -1:
                    raise ValueError("Could not find matching closing brace")
                
                json_str = rest[first_brace:end_idx]
                data = json.loads(json_str)
            except Exception as e:
                print(f"Error parsing prompt JSON: {e}")
                print(f"Prompt: {prompt}")
                raise
            result = {k: v[0] if isinstance(v, list) and v else '' for k, v in data.items()}
            class R:
                pass
            r = R()
            r.content = json.dumps(result)
            return r

    llm = FakeLLM()
    props = ['age', 'gender']
    gitops = {'age': ['age_at_diagnosis', 'age_at_event'], 'gender': ['sex', 'gender_identity']}
    res = asyncio.run(resolve_gitops_ambiguities(props, gitops, 'query', llm))
    assert res == {'age': 'age_at_diagnosis', 'gender': 'sex'}
    assert len(calls) == 1, "LLM should be called only once for gitops batch"
