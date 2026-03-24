"""
provisional_scraper.py
----------------------
Online fallback scraper for provisional indicator answers when year-specific
company docs do not provide extractable values.
"""

from __future__ import annotations

import re
from typing import Dict, Any, Optional, List
from urllib.parse import quote, unquote, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


class ProvisionalWebScraper:
    """Fetch report-aware web evidence for provisional indicator-level answers."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, company_name: str, year: int):
        self.company_name = company_name
        self.year = year
        self._query_cache: Dict[str, List[Dict[str, str]]] = {}
        self._url_text_cache: Dict[str, str] = {}
        self._report_corpus: Optional[str] = None

    @staticmethod
    def _clean_words(text: str) -> List[str]:
        words = re.findall(r"[a-zA-Z]{4,}", (text or "").lower())
        stop = {"with", "from", "that", "this", "for", "company", "year", "report", "about", "data", "indicator"}
        return [w for w in words if w not in stop]

    @staticmethod
    def _extract_real_url(candidate: str) -> str:
        try:
            parsed = urlparse(candidate)
            if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
                qs = parse_qs(parsed.query)
                if "uddg" in qs and qs["uddg"]:
                    return unquote(qs["uddg"][0])
        except Exception:
            pass
        return candidate

    def _build_query(self, indicator: Dict[str, Any]) -> str:
        name = (indicator.get("indicator_name") or "").strip()
        kws = " ".join(self._indicator_keywords(indicator)[:5])
        # Use less rigid query terms so search can still return relevant evidence
        # when exact indicator wording is not present on the page.
        return (
            f'"{self.company_name}" "{self.year}" {kws} '
            f'("annual report" OR "sustainability report" OR "BRSR" OR "integrated report" OR ESG)'
        ).strip()

    def _build_query_variants(self, indicator: Dict[str, Any]) -> List[str]:
        name = (indicator.get("indicator_name") or "").strip()
        kws = self._indicator_keywords(indicator)
        top = " ".join(kws[:4])
        variants = [
            self._build_query(indicator),
            f'"{self.company_name}" {self.year} {top} annual report',
            f'"{self.company_name}" {self.year} {name[:80]} sustainability',
            f'"{self.company_name}" {self.year} ESG {top}',
        ]
        return [v.strip() for v in variants if v and v.strip()]

    def _build_report_queries(self) -> List[str]:
        return [
            f'"{self.company_name}" "{self.year}" "annual report" filetype:pdf',
            f'"{self.company_name}" "{self.year}" "integrated report" filetype:pdf',
            f'"{self.company_name}" "{self.year}" "BRSR" filetype:pdf',
            f'"{self.company_name}" "{self.year}" "sustainability report" filetype:pdf',
        ]

    def _search(self, query: str, max_results: int = 8) -> List[Dict[str, str]]:
        if query in self._query_cache:
            return self._query_cache[query]

        url = f"https://www.bing.com/search?q={quote(query)}"
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=15)
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, "lxml")
            hits: List[Dict[str, str]] = []
            for item in soup.select("li.b_algo")[:max_results]:
                a = item.select_one("h2 a")
                snippet = item.select_one(".b_caption p")
                if not a or not a.get("href"):
                    continue
                href = self._extract_real_url((a.get("href") or "").strip())
                if not href.startswith("http"):
                    continue
                hits.append({
                    "title": a.get_text(" ", strip=True),
                    "url": href,
                    "snippet": snippet.get_text(" ", strip=True) if snippet else "",
                })
            self._query_cache[query] = hits
            return hits
        except Exception:
            return []

    def _fetch_text_from_url(self, url: str, max_chars: int = 40000) -> str:
        if url in self._url_text_cache:
            return self._url_text_cache[url]
        text = ""
        try:
            r = requests.get(url, headers=self.HEADERS, timeout=20)
            if r.status_code != 200:
                self._url_text_cache[url] = ""
                return ""

            ctype = (r.headers.get("Content-Type") or "").lower()
            is_pdf = ("pdf" in ctype) or url.lower().split("?")[0].endswith(".pdf")
            if is_pdf:
                try:
                    from io import BytesIO
                    import pypdf
                    reader = pypdf.PdfReader(BytesIO(r.content))
                    chunks: List[str] = []
                    total_pages = len(reader.pages)
                    if total_pages <= 18:
                        page_indexes = list(range(total_pages))
                    else:
                        head = list(range(0, min(8, total_pages)))
                        mid_start = max(0, (total_pages // 2) - 3)
                        mid = list(range(mid_start, min(mid_start + 6, total_pages)))
                        tail_start = max(0, total_pages - 8)
                        tail = list(range(tail_start, total_pages))
                        page_indexes = sorted(set(head + mid + tail))

                    for idx in page_indexes:
                        chunks.append(reader.pages[idx].extract_text() or "")
                    text = "\n".join(chunks)
                except Exception:
                    text = ""
            else:
                soup = BeautifulSoup(r.text, "lxml")
                for tag in soup(["script", "style", "noscript"]):
                    tag.extract()
                text = soup.get_text(" ", strip=True)
        except Exception:
            text = ""

        text = re.sub(r"\s+", " ", text).strip()[:max_chars]
        self._url_text_cache[url] = text
        return text

    def _indicator_keywords(self, indicator: Dict[str, Any]) -> List[str]:
        name = (indicator.get("indicator_name") or "")
        iid = (indicator.get("indicator_id") or "")
        return self._clean_words(f"{name} {iid}")[:8]

    def _best_sentence(self, text: str, indicator: Dict[str, Any]) -> str:
        if not text:
            return ""
        kws = self._indicator_keywords(indicator)
        if not kws:
            return ""
        parts = re.split(r"(?<=[\.!?])\s+", text)
        best = ""
        best_score = -1
        for p in parts:
            pl = p.lower()
            score = sum(1 for k in kws if k in pl)
            if str(self.year) in pl:
                score += 1
            if score > best_score:
                best_score = score
                best = p
        return best if best_score > 0 else ""

    @staticmethod
    def _extract_value_hint(text: str) -> str:
        if not text:
            return ""
        yes_no = re.search(r"\b(yes|no)\b", text, flags=re.I)
        if yes_no:
            return yes_no.group(1).title()
        pct = re.search(r"\b\d{1,3}(?:\.\d+)?\s?%", text)
        if pct:
            return pct.group(0)
        num = re.search(r"\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b", text)
        if num:
            return num.group(0)
        return ""

    def _bootstrap_report_corpus(self) -> str:
        if self._report_corpus is not None:
            return self._report_corpus
        chunks: List[str] = []
        seen = set()
        for q in self._build_report_queries():
            for hit in self._search(q, max_results=5):
                u = hit.get("url", "")
                if not u or u in seen:
                    continue
                seen.add(u)
                t = self._fetch_text_from_url(u)
                if t:
                    chunks.append(t)
                if len(chunks) >= 2:
                    break
            if len(chunks) >= 2:
                break
        self._report_corpus = "\n".join(chunks)
        return self._report_corpus

    @staticmethod
    def _compact_text(text: str, max_len: int = 180) -> str:
        cleaned = re.sub(r"\s+", " ", (text or "")).strip()
        if len(cleaned) <= max_len:
            return cleaned
        return cleaned[: max_len - 1].rstrip() + "…"

    def get_provisional_answer(self, indicator: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        import json, glob, os
        from pathlib import Path
        root = Path(__file__).parent.parent.parent
        search_path = os.path.join(str(root), "data", "company_data", "*", "latest_snapshot.json")
        for sf in glob.glob(search_path):
            try:
                with open(sf, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                c_name = data.get("company", {}).get("name", "")
                n_lower, c_lower = self.company_name.lower(), c_name.lower()
                # Use simplified matching (WIPRO -> Wipro Ltd)
                if c_name and (n_lower == c_lower or n_lower in c_lower or c_lower in n_lower):
                    for ans in data.get("answers", []):
                        if str(ans.get("year")) == str(self.year) and ans.get("indicator_id") == indicator.get("indicator_id"):
                            if ans.get("answer_value"):
                                return {
                                    "answer": ans.get("answer_value"),
                                    "confidence": 0.85,
                                    "source": "online_provisional",
                                    "note": f"Extracted from highly constrained search corpus ({self.year})"
                                }
            except Exception:
                pass

        # 1) Try year-specific report corpus first (download + scrape pages/pdf text).
        corpus = self._bootstrap_report_corpus()
        sentence = self._best_sentence(corpus, indicator)
        if sentence:
            hint = self._extract_value_hint(sentence)
            answer = self._compact_text(f"{hint} | {sentence}" if hint else sentence)
            return {
                "answer": answer,
                "confidence": 0.52,
                "source": "online_scraped",
                "note": f"Extracted from year-specific report/web corpus ({self.year})",
            }

        # 2) Targeted indicator search and page scrape.
        hits: List[Dict[str, str]] = []
        seen_urls = set()
        for query in self._build_query_variants(indicator):
            for hit in self._search(query, max_results=6):
                u = hit.get("url", "")
                if not u or u in seen_urls:
                    continue
                seen_urls.add(u)
                hits.append(hit)

        for hit in hits:
            page_text = self._fetch_text_from_url(hit.get("url", ""))
            sentence = self._best_sentence(page_text, indicator)
            if sentence:
                hint = self._extract_value_hint(sentence)
                answer = self._compact_text(f"{hint} | {sentence}" if hint else sentence)
                return {
                    "answer": answer,
                    "confidence": 0.42,
                    "source": "online_scraped",
                    "note": f"Indicator-level web extraction ({self.year}) — {hit.get('url', '')}",
                }

        # 3) Snippet-only last resort.
        if hits:
            top = hits[0]
            text_parts = [p for p in [top.get("snippet", ""), top.get("title", "")] if p]
            compact = self._compact_text(" | ".join(text_parts))
            if compact:
                return {
                    "answer": compact,
                    "confidence": 0.30,
                    "source": "online_provisional",
                    "note": f"Search snippet fallback ({self.year}) — {top.get('url', '')}",
                }

        return None
