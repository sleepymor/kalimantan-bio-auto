import asyncio
import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import Optional

import httpx
from ddgs import DDGS

from .config import ScraperConfig, get_config


@dataclass
class WebSearchResult:
    url: str
    source_url: str
    title: str = ""
    engine: str = ""


class WebSearchScraper:
    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or get_config()
        self._client: Optional[httpx.AsyncClient] = None
        self._ddgs: Optional[DDGS] = None
        self._brave_api_key = os.getenv("BRAVE_API_KEY")
        self._google_api_key = os.getenv("GOOGLE_API_KEY")
        self._google_cx = os.getenv("GOOGLE_CX")

    def __enter__(self):
        self._ddgs = DDGS()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ddgs = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.request_timeout),
            headers={"User-Agent": self.config.user_agent},
            follow_redirects=True,
        )
        self._ddgs = DDGS()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
        self._ddgs = None

    def _build_search_query(self, scientific_name: str) -> str:
        common_name = scientific_name.replace("_", " ").replace("-", " ")
        return f"{common_name} species wildlife nature photograph"

    def _is_valid_image_url(self, url: str) -> bool:
        if not url:
            return False
        url_lower = url.lower()
        if not any(url_lower.endswith(ext) for ext in self.config.allowed_extensions):
            if not any(ext in url_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                return False
        blocked_domains = [
            "wikimedia.org",
            "wikipedia.org",
            "facebook.com",
            "instagram.com",
            "pinterest.com",
            "twitter.com",
            "x.com",
            "youtube.com",
            "tiktok.com",
        ]
        for domain in blocked_domains:
            if domain in url_lower:
                return False
        return True

    def _extract_direct_image_url(self, result: dict) -> Optional[str]:
        image_url = result.get("image") or result.get("thumbnail") or result.get("url")
        if image_url and self._is_valid_image_url(image_url):
            return image_url
        return None

    def _search_duckduckgo(
        self,
        query: str,
        max_results: int,
        safesearch: str,
    ) -> list[WebSearchResult]:
        if not self._ddgs:
            self._ddgs = DDGS()

        results = []
        try:
            search_results = self._ddgs.images(
                query,
                region="wt-wt",
                safesearch=safesearch,
                max_results=max_results,
                size=None,
                color=None,
                type_image="photo",
                layout=None,
                license_image=None,
            )
            for r in search_results:
                img_url = self._extract_direct_image_url(r)
                if img_url:
                    results.append(
                        WebSearchResult(
                            url=img_url,
                            source_url=r.get("url", ""),
                            title=r.get("title", ""),
                            engine="duckduckgo",
                        )
                    )
        except Exception:
            pass
        return results

    async def _search_brave(
        self,
        query: str,
        max_results: int,
    ) -> list[WebSearchResult]:
        if not self._brave_api_key or not self._client:
            return []

        results = []
        try:
            response = await self._client.get(
                "https://api.search.brave.com/res/v1/images/search",
                headers={"X-Subscription-Token": self._brave_api_key},
                params={
                    "q": query,
                    "count": min(max_results, 20),
                    "safe_search": "strict",
                    "search_lang": "en",
                },
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("results", []):
                img_url = item.get("properties", {}).get("url") or item.get("thumbnail", {}).get("url")
                if img_url and self._is_valid_image_url(img_url):
                    results.append(
                        WebSearchResult(
                            url=img_url,
                            source_url=item.get("source", "") or item.get("url", ""),
                            title=item.get("title", ""),
                            engine="brave",
                        )
                    )
        except Exception:
            pass
        return results

    async def _search_google(
        self,
        query: str,
        max_results: int,
    ) -> list[WebSearchResult]:
        if not self._google_api_key or not self._google_cx or not self._client:
            return []

        results = []
        try:
            response = await self._client.get(
                "https://www.googleapis.com/customsearch/v1",
                params={
                    "key": self._google_api_key,
                    "cx": self._google_cx,
                    "q": query,
                    "searchType": "image",
                    "num": min(max_results, 10),
                    "safe": "active",
                    "imgType": "photo",
                    "fileType": "jpg,png,webp",
                },
            )
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                img_url = item.get("link")
                if img_url and self._is_valid_image_url(img_url):
                    results.append(
                        WebSearchResult(
                            url=img_url,
                            source_url=item.get("image", {}).get("contextLink", ""),
                            title=item.get("title", ""),
                            engine="google",
                        )
                    )
        except Exception:
            pass
        return results

    async def _search_bing_html(
        self,
        query: str,
        max_results: int,
    ) -> list[WebSearchResult]:
        if not self._client:
            return []

        results = []
        try:
            params = {"q": query, "first": 1, "count": max_results}
            headers = {"User-Agent": self.config.user_agent}
            response = await self._client.get(
                "https://www.bing.com/images/search",
                params=params,
                headers=headers,
            )
            response.raise_for_status()

            from lxml import html
            tree = html.fromstring(response.text)

            for img_elem in tree.cssselect("img.mimg")[:max_results]:
                img_url = img_elem.get("src") or img_elem.get("data-src")
                if img_url and self._is_valid_image_url(img_url):
                    results.append(
                        WebSearchResult(
                            url=img_url,
                            source_url="https://www.bing.com/images/search",
                            title=img_elem.get("alt", ""),
                            engine="bing",
                        )
                    )
        except Exception:
            pass
        return results

    def search_images(
        self,
        scientific_name: str,
        max_results: int = 20,
        safesearch: str = "strict",
    ) -> list[WebSearchResult]:
        query = self._build_search_query(scientific_name)
        return self._search_duckduckgo(query, max_results, safesearch)

    async def search_images_async(
        self,
        scientific_name: str,
        max_results: int = 20,
        safesearch: str = "strict",
    ) -> list[WebSearchResult]:
        query = self._build_search_query(scientific_name)
        all_results = []

        ddg_results = self._search_duckduckgo(query, max_results, safesearch)
        all_results.extend(ddg_results)

        if len(all_results) < max_results:
            remaining = max_results - len(all_results)

            if self._brave_api_key:
                brave_results = await self._search_brave(query, remaining)
                all_results.extend(brave_results)

        if len(all_results) < max_results:
            remaining = max_results - len(all_results)

            if self._google_api_key and self._google_cx:
                google_results = await self._search_google(query, remaining)
                all_results.extend(google_results)

        if len(all_results) < max_results:
            remaining = max_results - len(all_results)
            bing_results = await self._search_bing_html(query, remaining)
            all_results.extend(bing_results)

        seen_urls = set()
        unique_results = []
        for r in all_results:
            if r.url not in seen_urls:
                seen_urls.add(r.url)
                unique_results.append(r)

        return unique_results[:max_results]

    async def get_species_images(
        self,
        scientific_name: str,
        max_images: int = 10,
        safesearch: str = "strict",
    ) -> tuple[list[str], list[str], list[str], list[str], list[str], list[int]]:
        results = await self.search_images_async(scientific_name, max_results=max_images * 2, safesearch=safesearch)

        image_urls = []
        source_urls = []
        licenses = []
        attributions = []
        observation_ids = []
        taxon_ids = []

        for i, result in enumerate(results[:max_images]):
            image_urls.append(result.url)
            source_urls.append(result.source_url or f"https://duckduckgo.com/?q={urllib.parse.quote(scientific_name)}")
            licenses.append("unknown")
            attributions.append(f"Web search ({result.engine}): {result.title[:100]}" if result.title else f"Web search ({result.engine})")
            observation_ids.append(f"web_{result.engine}_{i}")
            taxon_ids.append(0)

        return image_urls, source_urls, licenses, attributions, observation_ids, taxon_ids