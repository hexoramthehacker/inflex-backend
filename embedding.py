# embedding.py
# Inflex — EmbeddingService

from __future__ import annotations

import dotenv
import google.generativeai as genai
import numpy as np

ENV = dotenv.dotenv_values(".env")
REWRITE_MODEL = AIzaSyDRAQ8hMhVG3C_6-qnF0f3Bg9vOMfgA9q4#ENV.get("REWRITE_MODEL", "gemini-2.0-flash")
EMBEDDING_MODEL = AIzaSyDRAQ8hMhVG3C_6-qnF0f3Bg9vOMfgA9q4#ENV.get("EMBEDDING_MODEL", "models/gemini-embedding-2-preview")
GEMINI_API_KEY =AIzaSyDRAQ8hMhVG3C_6-qnF0f3Bg9vOMfgA9q4 #ENV.get("GEMINI_API_KEY") or ENV.get("GEMINI_API") or ENV.get("AI_API")

if not GEMINI_API_KEY:
    raise ValueError("Missing GEMINI_API_KEY (or GEMINI_API / AI_API) in .env")

genai.configure(api_key=GEMINI_API_KEY)

REWRITE_PROMPTS = {
    "transcript": """
You are preparing creator content for a brand partnership marketplace called Inflex.
Given the raw transcript below, rewrite it as a clean structured creator profile summary.
Focus on:
- The main topics and themes the creator covers
- The audience this creator speaks to
- The tone and style of the content
- Any niches, industries or products mentioned
Keep it under 300 words. Do not invent information not in the transcript.

Transcript:
{text}
""",
    "description+tags": """
You are preparing creator content for a brand partnership marketplace called Inflex.
Given the video description and tags below, write a concise creator profile summary.
Focus on:
- What topics this creator covers
- Who their audience likely is
- What kind of brands would be a good fit
Keep it under 150 words. Do not invent information not in the description or tags.

Description and Tags:
{text}
""",
    "title_only": """
You are preparing creator content for a brand partnership marketplace called Inflex.
Given only a video title, write one sentence summarising what this creator likely covers.
Do not invent specific details — keep it general and honest.

Title:
{text}
""",
}


class EmbeddingService:
    def __init__(self) -> None:
        self.rewrite_model = genai.GenerativeModel(REWRITE_MODEL)  # type: ignore[arg-type]
        self.embedding_model = EMBEDDING_MODEL

    def rewrite(self, text: str, source: str) -> str:
        prompt_template = REWRITE_PROMPTS.get(source, REWRITE_PROMPTS["title_only"])
        prompt = prompt_template.format(text=text)
        try:
            response = self.rewrite_model.generate_content(prompt)
            rewritten = (response.text or "").strip()
            return rewritten or text
        except Exception:
            return text

    def embed_text(self, text: str, title: str = "") -> list[float]:
        result = genai.embed_content(
            model=self.embedding_model,
            content=text,
            task_type="RETRIEVAL_DOCUMENT",
            title=title or text[:50],
        )
        return result["embedding"]

    def embed_query(self, query: str) -> list[float]:
        result = genai.embed_content(
            model=self.embedding_model,
            content=query,
            task_type="RETRIEVAL_QUERY",
        )
        return result["embedding"]

    def embed_creators(self, embedding_texts: dict[str, dict[str, str | None]]) -> dict[str, dict[str, object]]:
        embedded: dict[str, dict[str, object]] = {}
        for vid_id, data in embedding_texts.items():
            text = data.get("text")
            source = str(data.get("source") or "title_only")
            if not text:
                continue

            clean_text = self.rewrite(str(text), source)
            try:
                vector = self.embed_text(clean_text, title=vid_id)
                embedded[vid_id] = {
                    "vector": vector,
                    "source": source,
                    "rewritten": clean_text,
                    "original": str(text)[:300],
                    "dim": len(vector),
                }
            except Exception:
                continue
        return embedded

    def rank_creators(self, query: str, embedded_creators: dict[str, dict[str, object]], top_k: int = 10) -> list[dict[str, object]]:
        query_vector = self.embed_query(query)
        scores: list[dict[str, object]] = []
        for vid_id, data in embedded_creators.items():
            a = np.array(query_vector)
            b = np.array(data["vector"])
            score = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
            scores.append({
                "video_id": vid_id,
                "score": round(score, 4),
                "source": data["source"],
                "summary": str(data["rewritten"])[:150],
            })
        scores.sort(key=lambda x: x["score"], reverse=True)
        return scores[:top_k]
