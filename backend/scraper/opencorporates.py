"""
opencorporates.py
-----------------
OpenCorporates API client for company name search and selection.

API docs: https://api.opencorporates.com/documentation/API-Reference
Base URL: https://api.opencorporates.com/v0.4/

Usage:
    from backend.scraper.opencorporates import search_companies, select_company
    results = search_companies("Infosys", jurisdiction_code="in")
    company = select_company(results)
"""

from __future__ import annotations

import time
import urllib.parse
import urllib.request
import json
from typing import Optional

# ── API constants ──────────────────────────────────────────────────────────────

_BASE_URL = "https://api.opencorporates.com/v0.4"
_SEARCH_ENDPOINT = "/companies/search"
_RESULTS_PER_PAGE = 20
_REQUEST_TIMEOUT = 15  # seconds


def _get(url: str) -> dict:
    """Make a GET request to the OpenCorporates API and return parsed JSON."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Impactree-Sustainability-Platform/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def search_companies(
    query: str,
    jurisdiction_code: Optional[str] = None,
    page: int = 1,
    per_page: int = _RESULTS_PER_PAGE,
    api_token: Optional[str] = None,
) -> list[dict]:
    """
    Search OpenCorporates for companies matching *query*.

    Parameters
    ----------
    query            : Company name search string.
    jurisdiction_code: Two-letter country code (e.g. "in" for India, "gb" for UK).
                       Pass None to search globally.
    page             : Result page number (1-based).
    per_page         : Results per page (max 100 without API token).
    api_token        : Optional OpenCorporates API token for higher rate limits.

    Returns
    -------
    List of dicts, each representing one matched company with keys:
        name, company_number, jurisdiction_code, incorporation_date,
        current_status, registered_address, opencorporates_url
    """
    params: dict[str, str] = {
        "q": query,
        "page": str(page),
        "per_page": str(per_page),
    }
    if jurisdiction_code:
        params["jurisdiction_code"] = jurisdiction_code.lower()
    if api_token:
        params["api_token"] = api_token

    url = f"{_BASE_URL}{_SEARCH_ENDPOINT}?{urllib.parse.urlencode(params)}"

    try:
        data = _get(url)
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise RuntimeError(
                "OpenCorporates requires an API token.\n\n"
                "  1. Register for a free account at https://opencorporates.com/users/account_requests/new\n"
                "  2. Retrieve your token from https://opencorporates.com/users/edit\n"
                "  3. Pass it with --api-token YOUR_TOKEN  or set:\n"
                "       export OPENCORPORATES_API_TOKEN=YOUR_TOKEN\n"
            ) from exc
        if exc.code == 429:
            raise RuntimeError(
                "OpenCorporates rate limit reached. "
                "Upgrade your account or wait before retrying."
            ) from exc
        raise RuntimeError(f"OpenCorporates API error {exc.code}: {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Failed to reach OpenCorporates API: {exc}") from exc

    companies = []
    for item in data.get("results", {}).get("companies", []):
        co = item.get("company", {})
        addr = co.get("registered_address") or {}
        companies.append(
            {
                "name": co.get("name", ""),
                "company_number": co.get("company_number", ""),
                "jurisdiction_code": co.get("jurisdiction_code", ""),
                "incorporation_date": co.get("incorporation_date", ""),
                "current_status": co.get("current_status", ""),
                "registered_address": _fmt_address(addr),
                "opencorporates_url": co.get("opencorporates_url", ""),
            }
        )
    return companies


def _fmt_address(addr: dict) -> str:
    """Format an OpenCorporates address dict into a single string."""
    if isinstance(addr, str):
        return addr
    parts = [
        addr.get("street_address", ""),
        addr.get("locality", ""),
        addr.get("region", ""),
        addr.get("postal_code", ""),
        addr.get("country", ""),
    ]
    return ", ".join(p for p in parts if p)
