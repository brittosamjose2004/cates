"""
financial_scraper.py
---------------------
Pulls HISTORICAL annual financials (last N years) from Yahoo Finance.

Returns data keyed by fiscal year (int):
  {
    2023: {"Total Revenue": "120000000", "Net Income": "15000000", ...},
    2022: {...},
    ...
  }
"""

from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from rich.console import Console

console = Console()


def _with_timeout(fn, timeout_seconds: int, default):
    """Guard potentially blocking yfinance calls with a hard timeout."""
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


class FinancialScraper:
    def __init__(self, ticker_symbol: str, company_name: str = ""):
        self.ticker  = ticker_symbol
        self.company = company_name

    def get_historical_financials(self, years: int = 5) -> Dict[int, Dict[str, Any]]:
        """
        Returns dict  {fiscal_year: {metric_name: value_str}}
        for up to `years` annual periods.
        """
        try:
            import yfinance as yf
            import pandas as pd

            t = yf.Ticker(self.ticker)
            result: Dict[int, Dict[str, Any]] = {}

            def _ingest(df: pd.DataFrame, prefix: str = ""):
                if df is None or df.empty:
                    return
                for col in list(df.columns)[:years]:
                    yr = col.year
                    result.setdefault(yr, {})
                    for idx in df.index:
                        raw = df.loc[idx, col]
                        if pd.notna(raw):
                            key = f"{prefix}{idx}" if prefix else str(idx)
                            result[yr][key] = str(int(raw)) if isinstance(raw, float) and raw == int(raw) else str(raw)

            _ingest(_with_timeout(lambda: t.financials, timeout_seconds=25, default=None))               # Income statement
            _ingest(_with_timeout(lambda: t.balance_sheet, timeout_seconds=25, default=None), "bs_")    # Balance sheet
            _ingest(_with_timeout(lambda: t.cashflow, timeout_seconds=25, default=None), "cf_")         # Cash flow

            return result
        except Exception as e:
            console.print(f"[yellow]  Historical finance warning: {e}[/yellow]")
            return {}

    def get_employee_history(self) -> Dict[str, str]:
        """Current employee count (Yahoo Finance doesn't provide year-by-year)."""
        try:
            import yfinance as yf
            info = _with_timeout(lambda: yf.Ticker(self.ticker).info or {}, timeout_seconds=20, default={})
            emp = info.get("fullTimeEmployees")
            return {"total_employees": str(emp)} if emp else {}
        except Exception:
            return {}

    def get_esg_scores(self) -> Dict[str, str]:
        """Fetch ESG sustainability scores from Yahoo Finance."""
        try:
            import yfinance as yf
            esg = _with_timeout(lambda: yf.Ticker(self.ticker).sustainability, timeout_seconds=20, default=None)
            if esg is None or esg.empty:
                return {}
            result: Dict[str, str] = {}
            for idx in esg.index:
                val = esg.loc[idx].iloc[0] if hasattr(esg.loc[idx], 'iloc') else esg.loc[idx]
                if val is not None and str(val) not in ("None", "nan", ""):
                    result[f"esg_{idx}"] = str(val)
            return result
        except Exception:
            return {}
