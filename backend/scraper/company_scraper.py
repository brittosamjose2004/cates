"""
company_scraper.py
------------------
Scrapes basic company profile from:
  1. Yahoo Finance  (sector, industry, employees, revenue, HQ, website)
  2. Screener.in    (Indian companies — key ratios, about section)
"""

import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Dict, Any
from rich.console import Console

console = Console()


def _with_timeout(fn, timeout_seconds: int, default):
    """Run blocking calls with a hard timeout so pipeline jobs do not hang."""
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    try:
        return future.result(timeout=timeout_seconds)
    except TimeoutError:
        return default
    except Exception:
        return default
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


class CompanyScraper:
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, company_name: str, ticker_hint: str = ""):
        self.company_name = company_name
        self.ticker_hint = (ticker_hint or "").strip().upper()

    # ── Yahoo Finance ──────────────────────────────────────────────────────────

    def _find_ticker(self) -> str:
        """Resolve ticker symbol, preferring explicit hint when provided."""
        # 1) Try explicit ticker hint first (with common exchange suffixes).
        hint = self.ticker_hint
        if hint:
            candidates = [hint]
            if "." not in hint:
                # For Indian equities, Yahoo symbols are usually *.NS or *.BO.
                candidates.extend([f"{hint}.NS", f"{hint}.BO"])
            for sym in candidates:
                try:
                    import yfinance as yf
                    info = _with_timeout(lambda: yf.Ticker(sym).info or {}, timeout_seconds=10, default={})
                    if info.get("symbol") or info.get("shortName") or info.get("longName"):
                        return sym
                except Exception:
                    continue

        try:
            url = (
                "https://query2.finance.yahoo.com/v1/finance/search"
                f"?q={requests.utils.quote(self.company_name)}"
                "&quotesCount=5&newsCount=0&listsCount=0"
            )
            resp = requests.get(url, headers=self.HEADERS, timeout=10)
            resp.raise_for_status()
            quotes = resp.json().get("quotes", [])
            if quotes:
                # Prefer NSE/BSE tickers for Indian names when available.
                for q in quotes:
                    sym = q.get("symbol", "")
                    if sym.endswith(".NS"):
                        return sym
                for q in quotes:
                    sym = q.get("symbol", "")
                    if sym.endswith(".BO"):
                        return sym
                return quotes[0].get("symbol", "")
            return ""
        except Exception:
            # 2) Last fallback: sanitize company name to a compact symbol token.
            token = re.sub(r"[^A-Z0-9]", "", self.company_name.upper())
            return token[:12] if token else ""

    def scrape_yahoo_finance(self) -> Dict[str, Any]:
        """Return profile + financial summary from Yahoo Finance."""
        try:
            import yfinance as yf

            ticker_symbol = self._find_ticker()
            if not ticker_symbol:
                return {}

            info = _with_timeout(lambda: yf.Ticker(ticker_symbol).info or {}, timeout_seconds=20, default={})
            if not info:
                return {"ticker": ticker_symbol}
            result = {"ticker": ticker_symbol}

            mapping = {
                "sector":            "sector",
                "industry":          "industry",
                "website":           "website",
                "total_employees":   "fullTimeEmployees",
                "description":       "longBusinessSummary",
                "exchange":          "exchange",
                "currency":          "currency",
                "country":           "country",
                "city":              "city",
                "revenue":           "totalRevenue",
                "gross_profit":      "grossProfits",
                "ebitda":            "ebitda",
                "operating_cashflow":"operatingCashflow",
                "free_cashflow":     "freeCashflow",
                "total_assets":      "totalAssets",
                "total_debt":        "totalDebt",
                "market_cap":        "marketCap",
                "pe_ratio":          "trailingPE",
                "pb_ratio":          "priceToBook",
                "roe":               "returnOnEquity",
                "roa":               "returnOnAssets",
                "profit_margins":    "profitMargins",
                "gross_margins":     "grossMargins",
                "operating_margins": "operatingMargins",
                "debt_to_equity":    "debtToEquity",
                "current_ratio":     "currentRatio",
                "quick_ratio":       "quickRatio",
                "dividend_yield":    "dividendYield",
                "payout_ratio":      "payoutRatio",
                "beta":              "beta",
                "52w_high":          "fiftyTwoWeekHigh",
                "52w_low":           "fiftyTwoWeekLow",
                "shares_outstanding":"sharesOutstanding",
                "book_value":        "bookValue",
                "revenue_per_share": "revenuePerShare",
                "earnings_per_share":"trailingEps",
            }
            for key, yf_key in mapping.items():
                val = info.get(yf_key)
                if val is not None and str(val) not in ("None", "nan", ""):
                    result[key] = str(val)

            city    = result.pop("city",    "")
            country = result.get("country", "")
            if city or country:
                result["headquarters"] = ", ".join(filter(None, [city, country]))

            # ── ESG / Sustainability scores ────────────────────────────────
            try:
                ticker_obj = yf.Ticker(ticker_symbol)
                esg = _with_timeout(lambda: ticker_obj.sustainability, timeout_seconds=20, default=None)
                if esg is not None and not esg.empty:
                    for idx in esg.index:
                        val = esg.loc[idx].iloc[0] if hasattr(esg.loc[idx], 'iloc') else esg.loc[idx]
                        if val is not None and str(val) not in ("None", "nan", ""):
                            result[f"esg_{idx}"] = str(val)
            except Exception:
                pass

            return result
        except Exception:
            return {}

    # ── Screener.in ───────────────────────────────────────────────────────────

    def scrape_screener(self) -> Dict[str, Any]:
        """Return key ratios & description from screener.in (Indian companies)."""
        try:
            search_url = (
                "https://www.screener.in/api/company/search/"
                f"?q={requests.utils.quote(self.company_name)}&v=3"
            )
            resp = requests.get(search_url, headers=self.HEADERS, timeout=10)
            if resp.status_code != 200:
                return {}

            results = resp.json()
            if not results:
                return {}

            company_path = results[0].get("url", "")
            if not company_path:
                return {}

            page = requests.get(
                f"https://www.screener.in{company_path}",
                headers=self.HEADERS,
                timeout=15,
            )
            if page.status_code != 200:
                return {}

            soup = BeautifulSoup(page.text, "lxml")
            data: Dict[str, Any] = {}

            # Official name
            h1 = soup.find("h1")
            if h1:
                data["official_name"] = h1.get_text(strip=True)

            # Top key ratios
            for li in soup.select("#top-ratios li"):
                name_el = li.select_one(".name")
                val_el  = li.select_one(".value")
                if name_el and val_el:
                    k = name_el.get_text(strip=True)
                    v = val_el.get_text(strip=True)
                    data[f"sc_{k}"] = v

            # About / description
            about = soup.select_one("#company-profile .sub") or soup.select_one(".company-profile")
            if about:
                data["about"] = about.get_text(separator=" ", strip=True)[:600]

            return data
        except Exception:
            return {}

    # ── Combined ──────────────────────────────────────────────────────────────

    def get_company_info(self) -> Dict[str, Any]:
        """Run all scrapers and return merged data dict."""
        console.print(f"\n[bold cyan]🔍 Scraping: {self.company_name}[/bold cyan]")

        all_data: Dict[str, Any] = {"company_name": self.company_name}

        console.print("  [dim]→ Yahoo Finance ...[/dim]", end="  ")
        yf_data = self.scrape_yahoo_finance()
        all_data.update(yf_data)
        console.print("[green]✓[/green]" if yf_data else "[yellow]no data[/yellow]")

        console.print("  [dim]→ Screener.in   ...[/dim]", end="  ")
        sc_data = self.scrape_screener()
        all_data.update(sc_data)
        console.print("[green]✓[/green]" if sc_data else "[yellow]no data[/yellow]")

        return all_data
