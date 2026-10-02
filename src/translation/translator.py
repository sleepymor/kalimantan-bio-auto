import os
import logging
import httpx
from typing import Optional, List
from deep_translator import GoogleTranslator
from src.cache.sqlite_cache import SQLiteCache

logger = logging.getLogger("pipeline.translation")

# langpair codes for MyMemory (uses ISO-639-1)
_MYMEMORY_LANG = {"en": "en", "id": "id"}

class Translator:
    def __init__(self, cache: SQLiteCache, engine: str = "google", target_lang: str = "id", source_lang: str = "en"):
        self.cache = cache
        self.engine = engine
        self.target_lang = target_lang
        self.source_lang = source_lang
        self._translator = None
        
        if engine == "google":
            self._translator = GoogleTranslator(source=source_lang, target=target_lang)
        elif engine == "libretranslate":
            url = os.getenv("LIBRETRANSLATE_URL", "http://localhost:5000")
            from deep_translator import LibreTranslator
            self._translator = LibreTranslator(source=source_lang, target=target_lang, base_url=url)
        else:
            raise ValueError(f"Unknown translation engine: {engine}")
    
    def translate(self, text: str, use_cache: bool = True) -> Optional[str]:
        if not text or not text.strip():
            return text
        
        text = text.strip()
        
        if use_cache:
            cached = self.cache.get_translation(text, self.source_lang, self.target_lang)
            if cached:
                return cached
        
        try:
            translated = self._translator.translate(text)
            if translated and use_cache:
                self.cache.set_translation(text, self.source_lang, self.target_lang, translated, self.engine)
            if translated:
                return translated
        except Exception as e:
            logger.warning(f"Translation failed for text: {text[:50]}... Error: {e}")
        
        # Fallback: MyMemory free API (no key needed) when primary engine fails.
        return self._translate_mymemory(text, use_cache)
    
    def _translate_mymemory(self, text: str, use_cache: bool = True) -> Optional[str]:
        src = _MYMEMORY_LANG.get(self.source_lang, self.source_lang)
        tgt = _MYMEMORY_LANG.get(self.target_lang, self.target_lang)
        try:
            # MyMemory handles ~500 chars reliably per request; chunk defensively.
            chunks = [text[i:i + 450] for i in range(0, len(text), 450)] or [text]
            out = []
            with httpx.Client(timeout=20) as client:
                for chunk in chunks:
                    r = client.get(
                        "https://api.mymemory.translated.net/get",
                        params={"q": chunk, "langpair": f"{src}|{tgt}"},
                    )
                    r.raise_for_status()
                    data = r.json().get("responseData", {})
                    part = (data.get("translatedText") or "").strip()
                    if not part or data.get("responseStatus") == 429:
                        return None
                    out.append(part)
            translated = " ".join(out)
            if translated and use_cache:
                self.cache.set_translation(text, self.source_lang, self.target_lang, translated, "mymemory")
            return translated or None
        except Exception as e:
            logger.warning(f"MyMemory fallback failed: {e}")
            return None
    
    def translate_batch(self, texts: List[str], use_cache: bool = True) -> List[Optional[str]]:
        results = []
        for text in texts:
            results.append(self.translate(text, use_cache))
        return results
    
    def translate_with_fallback(self, text: str, fallback_texts: List[str] = None) -> Optional[str]:
        result = self.translate(text)
        if result:
            return result
        
        if fallback_texts:
            for fallback in fallback_texts:
                result = self.translate(fallback)
                if result:
                    return result
        
        return None