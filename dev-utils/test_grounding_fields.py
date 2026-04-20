"""Direct Gemini API probe — bypass ADK, dump the full grounding_metadata.

Goal: verify whether `grounding_chunks[*].web.uri` is populated by Gemini 3.1
Pro Preview on Vertex and whether Google has exposed any original-URL field
we missed. Also reverse-scan the entire response object for the string
`cxodigitalpulse` — if the real URL is anywhere in the response, we'll find it.
"""
import os
import json
import pprint
import vertexai
from google.genai import types
from google import genai

PROJECT = os.environ.get("PROJECT_ID", "marketresearch-agents")
LOCATION = "global"
MODEL = "gemini-3.1-pro-preview"

TARGET_URL_FRAG = "cxodigitalpulse"  # if real URL is anywhere, this will match

# Targeted query that should trigger grounding on the AGIBOT / cxodigitalpulse
# result we saw in the last live run.
QUERY = (
    "Find news from April 18 2026 about AGIBOT unveiling new embodied AI "
    "robotic platforms under its 'One Robotic Body, Three Intelligences' "
    "framework. Return the source URL and title of the article."
)


def _scan_for_substring(obj, needle, path="root", hits=None):
    """Recursively walk any nested dict/list/pydantic object, collect every
    (path, value) where `needle` appears in the string form of a scalar."""
    if hits is None:
        hits = []
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump(exclude_none=True)
    if isinstance(obj, dict):
        for k, v in obj.items():
            _scan_for_substring(v, needle, f"{path}.{k}", hits)
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            _scan_for_substring(v, needle, f"{path}[{i}]", hits)
    elif isinstance(obj, str):
        if needle in obj:
            hits.append((path, obj[:300]))
    return hits


def main():
    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    resp = client.models.generate_content(
        model=MODEL,
        contents=QUERY,
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
        ),
    )

    print("=" * 80)
    print("MODEL TEXT OUTPUT:")
    print("=" * 80)
    print(resp.text or "<empty>")

    cand = resp.candidates[0]
    gm = cand.grounding_metadata

    print("\n" + "=" * 80)
    print("GROUNDING_METADATA — full dump (pydantic.model_dump):")
    print("=" * 80)
    if gm is None:
        print("<None>")
    else:
        dump = gm.model_dump(exclude_none=True)
        # Don't spam the HTML search_entry_point — summarise it
        if "search_entry_point" in dump and isinstance(dump["search_entry_point"], dict):
            rc = dump["search_entry_point"].get("rendered_content", "")
            dump["search_entry_point"]["rendered_content"] = f"<html, {len(rc)} chars>"
        pprint.pprint(dump, width=160, sort_dicts=False)

    print("\n" + "=" * 80)
    print("GROUNDING_CHUNKS — per-chunk detail:")
    print("=" * 80)
    chunks = getattr(gm, "grounding_chunks", None) or []
    print(f"Total chunks: {len(chunks)}")
    for i, ch in enumerate(chunks):
        web = getattr(ch, "web", None)
        if web is not None:
            print(f"  [{i}] web.domain={web.domain!r}")
            print(f"      web.title={web.title!r}")
            print(f"      web.uri={web.uri!r}")
        else:
            print(f"  [{i}] non-web chunk: {ch}")

    print("\n" + "=" * 80)
    print(f"REVERSE LOOKUP — does '{TARGET_URL_FRAG}' appear anywhere in the response?")
    print("=" * 80)
    hits = _scan_for_substring(resp, TARGET_URL_FRAG)
    if hits:
        for path, val in hits:
            print(f"  HIT at {path}:")
            print(f"    {val}")
    else:
        print("  No occurrences of the real URL fragment anywhere in the response.")
        print("  → Google does NOT expose the original URL in any field.")

    # Bonus: what top-level fields are even populated on this response?
    print("\n" + "=" * 80)
    print("RESPONSE TOP-LEVEL STRUCTURE:")
    print("=" * 80)
    top = resp.model_dump(exclude_none=True)
    for k, v in top.items():
        if isinstance(v, (dict, list)):
            print(f"  {k}: <{type(v).__name__}, len={len(v)}>")
        else:
            print(f"  {k}: {str(v)[:80]}")


if __name__ == "__main__":
    main()
