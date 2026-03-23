"""
questionnaire/engine.py
-----------------------
QuestionnaireEngine — drives an interactive or auto-fill Q&A session.

Flow:
  1. setup()                → ensure Company + QuestionnaireSession rows exist
  2. get_prefilled_answer() → try scraped data, then historical answers
  3. run_interactive()      → prompt user to accept / edit / skip each indicator
  4. run_auto()             → silently accept every prefilled answer
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path
from typing import Optional, Dict, Any, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich import box
import sqlalchemy.orm as orm

from backend.database.db import get_session, init_db
from backend.database.models import Company, ScrapedData, QuestionnaireSession, Answer
from backend.processor.csv_loader import ImpactreeCSVLoader
from backend.processor.data_mapper import DataMapper
from backend.scraper.provisional_scraper import ProvisionalWebScraper

console = Console()


class QuestionnaireEngine:
    def __init__(
        self,
        company_name: str,
        year: int,
        standard: str = "ALL",
        allow_historical_prefill: bool = True,
        allow_smart_defaults: bool = True,
        allow_cross_year_financial_fallback: bool = True,
        allow_online_provisional_fallback: bool = True,
        provisional_max_attempts: int = 0,
        provisional_time_budget_seconds: int = 0,
    ):
        self.company_name = company_name
        self.year = year
        self.standard = standard
        self.allow_historical_prefill = allow_historical_prefill
        self.allow_smart_defaults = allow_smart_defaults
        self.allow_cross_year_financial_fallback = allow_cross_year_financial_fallback
        self.allow_online_provisional_fallback = allow_online_provisional_fallback
        self.provisional_max_attempts = max(0, int(provisional_max_attempts or 0))
        self.provisional_time_budget_seconds = max(0, int(provisional_time_budget_seconds or 0))
        self.session: Optional[orm.Session] = None
        self.company: Optional[Company] = None
        self.qs_session: Optional[QuestionnaireSession] = None
        self.mapper: Optional[DataMapper] = None
        self.provisional_scraper: Optional[ProvisionalWebScraper] = None
        self._indicators: List[Dict[str, Any]] = []

    def _get_provisional_answer_bounded(
        self,
        indicator: Dict[str, Any],
        timeout_seconds: int = 90,
    ) -> Optional[Dict[str, Any]]:
        """Bound online fallback latency per indicator to keep scoring deterministic."""
        if not self.provisional_scraper:
            return None
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(self.provisional_scraper.get_provisional_answer, indicator)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeout:
            return None
        except Exception:
            return None
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    # ── Setup ─────────────────────────────────────────────────────────────────

    def setup(self) -> None:
        """Load DB session, resolve company, load indicators, build mapper."""
        init_db()
        self.session = get_session()

        # Resolve Company record — same priority chain as the scrape command:
        # 1. exact name, 2. ticker (handles "TCS" → ticker "TCS.NS"), 3. partial ilike
        _needle = self.company_name.strip().upper()
        self.company = (
            self.session.query(Company)
            .filter(Company.name == self.company_name)
            .first()
            or self.session.query(Company)
            .filter(
                Company.ticker.in_([
                    _needle,
                    _needle + ".NS",
                    _needle + ".BO",
                ])
            )
            .first()
            or self.session.query(Company)
            .filter(Company.name.ilike(f"%{self.company_name}%"))
            .first()
        )
        if self.company is None:
            self.company = Company(name=self.company_name)
            self.session.add(self.company)
            self.session.commit()
            console.print(
                f"[yellow]Company '{self.company_name}' not found — created new record.[/yellow]"
            )

        # Resolve or create QuestionnaireSession
        self.qs_session = (
            self.session.query(QuestionnaireSession)
            .filter_by(
                company_id=self.company.id,
                year=self.year,
                standard=self.standard,
            )
            .first()
        )

        # Load indicators
        if self.standard == "ALL":
            self._indicators = ImpactreeCSVLoader.get_all_indicators()
        else:
            self._indicators = ImpactreeCSVLoader.get_indicators_by_standard(
                self.standard
            )

        total = len(self._indicators)

        if self.qs_session is None:
            self.qs_session = QuestionnaireSession(
                company_id=self.company.id,
                year=self.year,
                standard=self.standard,
                status="in_progress",
                total_questions=total,
                answered_questions=0,
            )
            self.session.add(self.qs_session)
            self.session.commit()

        # Build mapper from scraped data stored in DB
        scraped_rows = (
            self.session.query(ScrapedData)
            .filter_by(company_id=self.company.id, year=self.year)
            .all()
        )
        scraped_dict: Dict[str, Any] = {row.data_key: row.data_value for row in scraped_rows}
        scraped_dict["company_name"] = self.company.name

        # Optionally merge the latest yahoo_historical metrics into mapper context.
        # Strict year-wise mode disables this to avoid cross-year leakage.
        if self.allow_cross_year_financial_fallback:
            from sqlalchemy import func as _sqlfunc
            max_fy = (
                self.session.query(_sqlfunc.max(ScrapedData.year))
                .filter_by(company_id=self.company.id, source="yahoo_historical")
                .scalar()
            )
            if max_fy:
                hist_rows = (
                    self.session.query(ScrapedData)
                    .filter_by(
                        company_id=self.company.id,
                        year=max_fy,
                        source="yahoo_historical",
                    )
                    .all()
                )
                for row in hist_rows:
                    # Don't overwrite current-year yahoo data
                    if row.data_key not in scraped_dict:
                        scraped_dict[row.data_key] = row.data_value

        self.mapper = DataMapper(scraped_dict)
        self.provisional_scraper = ProvisionalWebScraper(self.company.name, self.year)

    # ── Prefill logic ─────────────────────────────────────────────────────────

    def get_prefilled_answer(self, indicator_id: str) -> Optional[Dict[str, Any]]:
        """Return {answer, confidence, source, note} or None."""
        # 1. Try scraped / mapped data
        suggestion = self.mapper.get(indicator_id) if self.mapper else None
        if suggestion:
            return suggestion

        # 2. Fall back to most-recent historical answer in DB (optional)
        if not self.allow_historical_prefill:
            return None

        historical = (
            self.session.query(Answer)
            .filter(
                Answer.company_id == self.company.id,
                Answer.indicator_id == indicator_id,
                Answer.year < self.year,
                Answer.answer_value.isnot(None),
            )
            .order_by(Answer.year.desc())
            .first()
        )
        if historical:
            return {
                "answer": historical.answer_value,
                "confidence": historical.confidence or 0.5,
                "source": "historical",
                "note": f"Copied from {historical.year}",
            }
        return None

    def get_historical_answer(self, indicator_id: str) -> Optional[Answer]:
        """Return the most recent past Answer row for the same indicator."""
        return (
            self.session.query(Answer)
            .filter(
                Answer.company_id == self.company.id,
                Answer.indicator_id == indicator_id,
                Answer.year < self.year,
            )
            .order_by(Answer.year.desc())
            .first()
        )

    # ── Save ──────────────────────────────────────────────────────────────────

    def save_answer(
        self,
        indicator: Dict[str, Any],
        answer_value: str,
        source: str = "manual",
        confidence: float = 1.0,
        notes: str = "",
    ) -> None:
        """Upsert an Answer row and update session progress counter."""
        assert self.session and self.company and self.qs_session

        existing = (
            self.session.query(Answer)
            .filter_by(
                company_id=self.company.id,
                year=self.year,
                indicator_id=indicator.get("indicator_id", ""),
            )
            .first()
        )

        ind_id = indicator.get("indicator_id", "")
        module  = ind_id.split("-")[1] if "-" in ind_id else ""

        if existing:
            was_empty = not existing.answer_value
            existing.answer_value = answer_value
            existing.source = source
            existing.confidence = confidence
            existing.notes = notes
            if was_empty and answer_value:
                self.qs_session.answered_questions = (
                    self.qs_session.answered_questions or 0
                ) + 1
        else:
            new_ans = Answer(
                session_id=self.qs_session.id,
                company_id=self.company.id,
                year=self.year,
                indicator_id=ind_id,
                module=module,
                indicator_name=indicator.get("indicator_name", ""),
                question_text=indicator.get("question", ""),
                answer_value=answer_value,
                answer_unit=indicator.get("unit", ""),
                response_format=indicator.get("response_format", ""),
                source=source,
                confidence=confidence,
                notes=notes,
                is_verified=(source == "manual"),
            )
            self.session.add(new_ans)
            if answer_value:
                self.qs_session.answered_questions = (
                    self.qs_session.answered_questions or 0
                ) + 1

        self.session.commit()

    # ── Interactive mode ──────────────────────────────────────────────────────

    def run_interactive(self, module_filter: Optional[str] = None) -> None:
        """Walk through indicators, show prefills, prompt user to accept/edit/skip."""
        self.setup()

        indicators = self._indicators
        if module_filter:
            indicators = [
                i for i in indicators if module_filter.upper() in i.get("indicator_id", "")
            ]

        answered = already_answered = 0

        for idx, indicator in enumerate(indicators, 1):
            ind_id = indicator.get("indicator_id", "")
            name   = indicator.get("indicator_name", ind_id)
            q_text = indicator.get("question", "")
            module = indicator.get("module_name", "")
            fmt    = indicator.get("response_format", "")

            # Skip already-answered unless user wants to revisit (simple flow: skip)
            existing = (
                self.session.query(Answer)
                .filter_by(company_id=self.company.id, year=self.year, indicator_id=ind_id)
                .first()
            )
            if existing and existing.answer_value and existing.is_verified:
                already_answered += 1
                continue

            # Header
            console.print()
            console.print(
                Panel(
                    f"[bold]{ind_id}[/bold]  [{module}]\n"
                    f"[cyan]{name}[/cyan]\n\n"
                    f"[white]{q_text}[/white]\n\n"
                    f"[dim]Format: {fmt}[/dim]",
                    title=f"[bold yellow]Question {idx}/{len(indicators)}[/bold yellow]",
                    border_style="blue",
                )
            )

            prefill = self.get_prefilled_answer(ind_id)
            default_val = ""

            if prefill:
                conf_pct = int((prefill.get("confidence") or 0) * 100)
                src_label = prefill.get("source", "scraped")
                note      = prefill.get("note", "")
                console.print(
                    f"[green]Suggestion ({src_label}, {conf_pct}% confidence):[/green] "
                    f"[bold]{prefill['answer']}[/bold]"
                    + (f"  [dim]({note})[/dim]" if note else "")
                )
                default_val = prefill["answer"]

            # Historical hint
            hist = self.get_historical_answer(ind_id)
            if hist and hist.answer_value:
                console.print(
                    f"[dim]Historical ({hist.year}): {hist.answer_value}[/dim]"
                )

            # Prompt
            action = Prompt.ask(
                "[bold]Action[/bold]",
                choices=["a", "e", "s", "q"],
                default="a" if default_val else "e",
            )
            # a=accept  e=edit  s=skip  q=quit

            if action == "q":
                console.print("[yellow]Session paused. Progress saved.[/yellow]")
                break

            if action == "s":
                continue

            if action == "a" and default_val:
                val    = default_val
                source = prefill.get("source", "scraped") if prefill else "manual"
                conf   = prefill.get("confidence", 1.0) if prefill else 1.0
            else:
                val    = Prompt.ask("Enter answer", default=default_val or "")
                source = "manual"
                conf   = 1.0

            if val.strip():
                self.save_answer(indicator, val.strip(), source=source, confidence=conf)
                answered += 1
                console.print("[green]✓ Saved[/green]")

        # Summary
        total_done = (self.qs_session.answered_questions or 0)
        console.print()
        console.print(
            Panel(
                f"Answered this session: [bold green]{answered}[/bold green]\n"
                f"Already answered:      [bold cyan]{already_answered}[/bold cyan]\n"
                f"Total answered so far: [bold yellow]{total_done}[/bold yellow] / "
                f"{self.qs_session.total_questions or len(self._indicators)}",
                title="[bold]Session Summary[/bold]",
                border_style="green",
            )
        )

        if total_done >= (self.qs_session.total_questions or len(self._indicators)):
            self.qs_session.status = "completed"
            self.session.commit()

    # ── Auto mode ─────────────────────────────────────────────────────────────

    def run_auto(self, module_filter: Optional[str] = None) -> int:
        """Auto-fill indicators from available extracted data.

        In strict mode (allow_smart_defaults=False), unanswered indicators are left blank.
        """
        from backend.processor.data_mapper import smart_default

        self.setup()

        indicators = self._indicators
        if module_filter:
            indicators = [
                i for i in indicators if module_filter.upper() in i.get("indicator_id", "")
            ]

        real_data = skipped = defaults = 0
        provisional = 0
        cleared = 0
        provisional_attempts = 0
        provisional_start = time.time()

        for indicator in indicators:
            ind_id = indicator.get("indicator_id", "")

            existing = (
                self.session.query(Answer)
                .filter_by(company_id=self.company.id, year=self.year, indicator_id=ind_id)
                .first()
            )
            if existing and existing.is_verified:
                skipped += 1
                continue

            prefill = self.get_prefilled_answer(ind_id)
            if prefill and prefill.get("answer"):
                self.save_answer(
                    indicator,
                    prefill["answer"],
                    source=prefill.get("source", "scraped"),
                    confidence=prefill.get("confidence", 0.7),
                    notes=prefill.get("note", ""),
                )
                real_data += 1
                continue

            # Try bounded online fallback before defaults.
            if self.allow_online_provisional_fallback and self.provisional_scraper:
                within_attempt_budget = (
                    self.provisional_max_attempts <= 0
                    or provisional_attempts < self.provisional_max_attempts
                )
                within_time_budget = (
                    self.provisional_time_budget_seconds <= 0
                    or (time.time() - provisional_start) < self.provisional_time_budget_seconds
                )
                if within_attempt_budget and within_time_budget:
                    provisional_attempts += 1
                    fallback = self._get_provisional_answer_bounded(indicator, timeout_seconds=4)
                    if fallback and fallback.get("answer"):
                        self.save_answer(
                            indicator,
                            fallback["answer"],
                            source=fallback.get("source", "online_provisional"),
                            confidence=fallback.get("confidence", 0.35),
                            notes=fallback.get("note", ""),
                        )
                        provisional += 1
                        continue

            # Guarantee complete indicator coverage if requested.
            if self.allow_smart_defaults:
                default = smart_default(indicator)
                self.save_answer(
                    indicator,
                    default["answer"],
                    source="smart_default",
                    confidence=default["confidence"],
                    notes=default["note"],
                )
                defaults += 1
                continue

            # Strict mode: clear stale auto values when nothing usable was found.
            if existing and not existing.is_verified:
                existing.answer_value = None
                existing.source = "unavailable"
                existing.confidence = 0.0
                existing.notes = "No year-specific extracted data found after report download and online fallback scraping"
                self.session.commit()
                cleared += 1
            elif existing is None:
                # Keep full indicator coverage with explicit unavailable rows.
                module = ind_id.split("-")[1] if "-" in ind_id else ""
                missing_row = Answer(
                    session_id=self.qs_session.id,
                    company_id=self.company.id,
                    year=self.year,
                    indicator_id=ind_id,
                    module=module,
                    indicator_name=indicator.get("indicator_name", ""),
                    question_text=indicator.get("question", ""),
                    answer_value=None,
                    answer_unit=indicator.get("unit", ""),
                    response_format=indicator.get("response_format", ""),
                    source="unavailable",
                    confidence=0.0,
                    notes="No year-specific extracted data found after report download and online fallback scraping",
                    is_verified=False,
                )
                self.session.add(missing_row)
                self.session.commit()
                cleared += 1

        total = real_data + provisional + defaults
        if self.allow_smart_defaults:
            console.print(
                f"\n[bold cyan]{self.company_name}[/bold cyan] ({self.year}) — auto-fill complete:\n"
                f"  [green]✓ {real_data}[/green] filled from real scraped/financial data\n"
                f"  [cyan]~ {provisional}[/cyan] filled from online provisional fallback\n"
                f"  [yellow]~ {defaults}[/yellow] filled with smart defaults (needs review)\n"
                f"  [dim]• provisional attempts: {provisional_attempts}[/dim]\n"
                f"  [dim]↩ {skipped} skipped (already manually verified)[/dim]\n"
                f"  [bold]Total: {total} / {len(self._indicators)}[/bold]"
            )
        else:
            console.print(
                f"\n[bold cyan]{self.company_name}[/bold cyan] ({self.year}) — strict auto-fill complete:\n"
                f"  [green]✓ {real_data}[/green] filled from extracted year-specific data\n"
                f"  [cyan]~ {provisional}[/cyan] filled from online provisional fallback\n"
                f"  [dim]• provisional attempts: {provisional_attempts}[/dim]\n"
                f"  [dim]↩ {skipped} skipped (already manually verified)[/dim]\n"
                f"  [dim]⊘ {cleared} cleared stale auto-filled values[/dim]\n"
                f"  [bold]Total: {real_data + provisional} / {len(self._indicators)}[/bold]"
            )
        return total

    # ── View saved answers ────────────────────────────────────────────────────

    def show_answers(self, module_filter: Optional[str] = None) -> None:
        """Print a Rich table of saved answers for this company + year."""
        if not self.session:
            self.setup()

        query = self.session.query(Answer).filter_by(
            company_id=self.company.id, year=self.year
        )
        if module_filter:
            query = query.filter(Answer.module.ilike(f"%{module_filter}%"))

        rows = query.order_by(Answer.indicator_id).all()

        if not rows:
            console.print("[yellow]No answers found.[/yellow]")
            return

        tbl = Table(
            "Indicator", "Name", "Answer", "Source", "Conf",
            title=f"Answers — {self.company_name} ({self.year})",
            box=box.SIMPLE_HEAVY,
            show_lines=True,
        )
        for r in rows:
            conf_str = f"{int((r.confidence or 0)*100)}%" if r.confidence else "—"
            tbl.add_row(
                r.indicator_id or "",
                (r.indicator_name or "")[:40],
                (r.answer_value or "")[:60],
                r.source or "",
                conf_str,
            )
        console.print(tbl)

    # ── Year-over-year history ────────────────────────────────────────────────

    def show_history(self, indicator_ids: Optional[List[str]] = None) -> None:
        """Print year-over-year comparison for key indicators."""
        if not self.session:
            self.setup()

        q = self.session.query(Answer).filter_by(company_id=self.company.id)
        if indicator_ids:
            q = q.filter(Answer.indicator_id.in_(indicator_ids))
        rows = q.order_by(Answer.indicator_id, Answer.year).all()

        if not rows:
            console.print("[yellow]No history found.[/yellow]")
            return

        # Group by indicator
        from collections import defaultdict
        years_seen: List[int] = sorted({r.year for r in rows})
        by_ind: Dict[str, Dict[int, str]] = defaultdict(dict)
        names: Dict[str, str] = {}
        for r in rows:
            by_ind[r.indicator_id][r.year] = r.answer_value or ""
            names[r.indicator_id] = r.indicator_name or r.indicator_id

        tbl = Table(
            "Indicator", *[str(y) for y in years_seen],
            title=f"Year-over-Year — {self.company_name}",
            box=box.SIMPLE_HEAVY,
            show_lines=True,
        )
        for ind_id in sorted(by_ind):
            row_vals = [by_ind[ind_id].get(y, "—") for y in years_seen]
            # Truncate for display
            row_vals = [(v[:50] + "…" if len(v) > 50 else v) for v in row_vals]
            tbl.add_row(ind_id, *row_vals)
        console.print(tbl)

    # ── Export ────────────────────────────────────────────────────────────────

    def export(self, format: str = "csv", output_path: Optional[str] = None) -> str:
        """Export answers to CSV or JSON. Returns output path."""
        import json, csv, io
        from datetime import datetime

        if not self.session:
            self.setup()

        rows = (
            self.session.query(Answer)
            .filter_by(company_id=self.company.id, year=self.year)
            .order_by(Answer.indicator_id)
            .all()
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = self.company_name.replace(" ", "_")
        default_name = f"{safe_name}_{self.year}_{timestamp}.{format}"
        out_path = output_path or default_name

        if format == "json":
            data = [
                {
                    "indicator_id": r.indicator_id,
                    "indicator_name": r.indicator_name,
                    "question": r.question_text,
                    "answer": r.answer_value,
                    "unit": r.answer_unit,
                    "source": r.source,
                    "confidence": r.confidence,
                    "is_verified": r.is_verified,
                    "notes": r.notes,
                }
                for r in rows
            ]
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "indicator_id", "indicator_name", "question",
                        "answer", "unit", "source", "confidence",
                        "is_verified", "notes",
                    ],
                )
                writer.writeheader()
                for r in rows:
                    writer.writerow(
                        {
                            "indicator_id": r.indicator_id,
                            "indicator_name": r.indicator_name,
                            "question": r.question_text,
                            "answer": r.answer_value,
                            "unit": r.answer_unit,
                            "source": r.source,
                            "confidence": r.confidence,
                            "is_verified": r.is_verified,
                            "notes": r.notes,
                        }
                    )

        return out_path
