from __future__ import annotations

import dotenv
from googleapiclient.discovery import build


class YOUTUBE_SEARCH:
    def __init__(self) -> None:
        env = dotenv.dotenv_values(".env")
        self.api_key = "AIzaSyDRAQ8hMhVG3C_6-qnF0f3Bg9vOMfgA9q4"#env.get("YOUTUBE_API_KEY") or env.get("AI_API") or env.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("Missing YOUTUBE_API_KEY (or AI_API fallback) in .env")
        self.youtube = build("youtube", "v3", developerKey=self.api_key)

    def search(self, query: str, max_results: int = 5) -> list[dict]:
        request = self.youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
        )
        response = request.execute()
        return response["items"]

    def get_id(self, videos: list[dict]) -> list[str]:
        return [item["id"]["videoId"] for item in videos]

    def get_url(self, videos: list[dict]) -> list[str]:
        return [f"https://www.youtube.com/watch?v={item['id']['videoId']}" for item in videos]

    def get_metadata(self, videos: list[dict]) -> dict[str, dict]:
        video_ids = self.get_id(videos)
        metadata: dict[str, dict] = {}
        if not video_ids:
            return metadata

        request = self.youtube.videos().list(
            part="snippet,contentDetails",
            id=",".join(video_ids),
        )
        response = request.execute()

        for item in response.get("items", []):
            vid_id = item["id"]
            snippet = item["snippet"]
            metadata[vid_id] = {
                "title": snippet.get("title", ""),
                "channel": snippet.get("channelTitle", ""),
                "description": snippet.get("description", ""),
                "tags": snippet.get("tags", []),
                "duration": item["contentDetails"].get("duration", ""),
            }
        return metadata

    def get_caption(self, videos: list[dict]) -> dict[str, str | None]:
        from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

        video_ids = self.get_id(videos)
        captions: dict[str, str | None] = {}
        ytt = YouTubeTranscriptApi()

        for vid_id in video_ids:
            try:
                transcript = ytt.fetch(vid_id)
                captions[vid_id] = " ".join([t.text for t in transcript])
            except (NoTranscriptFound, TranscriptsDisabled):
                captions[vid_id] = None
            except Exception:
                captions[vid_id] = None
        return captions

    def get_embedding_text(self, videos: list[dict]) -> dict[str, dict[str, str | None]]:
        captions = self.get_caption(videos)
        metadata = self.get_metadata(videos)
        result: dict[str, dict[str, str | None]] = {}

        for vid_id in self.get_id(videos):
            transcript = captions.get(vid_id)
            meta = metadata.get(vid_id, {})
            if transcript:
                result[vid_id] = {"text": transcript, "source": "transcript"}
            elif meta.get("description") or meta.get("tags"):
                tag_str = ", ".join(meta.get("tags", []))
                text = f"{meta.get('title', '')}\n\n{meta.get('description', '')}\n\nTopics: {tag_str}".strip()
                result[vid_id] = {"text": text, "source": "description+tags"}
            elif meta.get("title"):
                result[vid_id] = {"text": meta["title"], "source": "title_only"}
            else:
                result[vid_id] = {"text": None, "source": "none"}
        return result
