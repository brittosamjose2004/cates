"""
cli.py
------
Impactree Backend CLI — entry point.

Usage:
  python backend/cli.py init
  python backend/cli.py scrape "Tata Steel" --year 2024
  python backend/cli.py questionnaire "Tata Steel" --year 2024
  python backend/cli.py answers "Tata Steel" --year 2024
  python backend/cli.py history "Tata Steel"
  python backend/cli.py export "Tata Steel" --year 2024 --format csv
  python backend/cli.py companies
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from backend.database.db import init_db, get_session
from backend.database.models import Company, ScrapedData, QuestionnaireSession, Answer

console = Console()

# ── CLI group ─────────────────────────────────────────────────────────────────

@click.group()
def cli():
    """Impactree Questionnaire Backend — Sustainability Data CLI."""


# ── init ──────────────────────────────────────────────────────────────────────

@cli.command()
def init():
    """Initialise the database and verify CSV files are readable."""
    console.print("[bold cyan]Initialising database...[/bold cyan]")
    init_db()
    console.print("[green]✓ Database ready.[/green]")

    from backend.processor.csv_loader import ImpactreeCSVLoader
    try:
        indicators = ImpactreeCSVLoader.get_all_indicators()
        console.print(f"[green]✓ CSV loaded — {len(indicators)} indicators found.[/green]")
    except Exception as e:
        console.print(f"[red]✗ CSV load failed: {e}[/red]")

    console.print(
        Panel("[bold]Impactree CLI is ready.[/bold]\n"
              "Next: [cyan]python backend/cli.py scrape \"<Company Name>\"[/cyan]",
              border_style="green")
    )


# ── scrape ────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("company_name")
@click.option("--year", default=None, type=int,
              help="Fiscal year to tag data under (default: current year)")
@click.option("--historical-years", default=5, show_default=True,
              help="How many historical fiscal years to fetch")
@click.option("--brsr-pdf", default=None, type=click.Path(exists=True),
              help="Path to a local BRSR/Annual Report PDF to parse (skips download)")
def scrape(company_name: str, year: int, historical_years: int, brsr_pdf: str):
    """Scrape company profile + financial history and save to DB."""
    import datetime

    year = year or datetime.date.today().year
    init_db()
    session = get_session()

    console.print(Panel(
        f"Scraping [bold cyan]{company_name}[/bold cyan] for [bold]{year}[/bold]",
        border_style="blue",
    ))

    # ── Company profile ────────────────────────────────────────────────────────
    from backend.scraper.company_scraper import CompanyScraper

    scraper = CompanyScraper(company_name)
    with console.status("Fetching company profile..."):
        info = scraper.get_company_info()

    if not info:
        console.print("[red]Could not retrieve company info. Aborting.[/red]")
        sys.exit(1)

    # Warn if Screener and Yahoo returned different companies (ambiguous query)
    yahoo_name = (info.get("company_name") or "").lower()
    screener_name = (info.get("official_name") or "").lower()
    yahoo_ticker = (info.get("ticker") or "").upper().replace(".NS", "").replace(".BO", "")
    # Suppress if Yahoo's "name" is just the ticker symbol (e.g. "TCS" for TCS.NS)
    yahoo_is_ticker = yahoo_name.upper() == yahoo_ticker
    if (yahoo_name and screener_name and not yahoo_is_ticker and
            not any(w in screener_name for w in yahoo_name.split()[:2]) and
            not any(w in yahoo_name for w in screener_name.split()[:2])):
        console.print(
            f"[yellow]⚠ Company name mismatch:[/yellow]\n"
            f"  Yahoo Finance → [cyan]{info.get('company_name')}[/cyan] "
            f"(ticker: {info.get('ticker')})\n"
            f"  Screener.in   → [cyan]{info.get('official_name')}[/cyan]\n"
            f"  [dim]Try a more specific name, e.g. \"TCS\" or \"Tata Consultancy\"[/dim]"
        )

    # Resolve / create Company row — prefer exact official_name then ticker
    official_name = info.get("official_name") or company_name
    ticker_val = info.get("ticker") or ""

    company = (
        session.query(Company).filter(Company.name == official_name).first()
        or session.query(Company).filter(Company.ticker == ticker_val, ticker_val != "").first()
        or session.query(Company).filter(Company.name.ilike(f"%{company_name}%")).first()
    )
    if not company:
        company = Company(
            name=official_name,
            sector=info.get("sector"),
            industry=info.get("industry"),
            exchange=info.get("exchange"),
            ticker=info.get("ticker"),
            website=info.get("website"),
            headquarters=info.get("headquarters"),
            description=info.get("description", "")[:500] if info.get("description") else None,
        )
        session.add(company)
    else:
        # Only update name if target name doesn't already belong to a different row
        name_conflict = (
            session.query(Company)
            .filter(Company.name == official_name, Company.id != company.id)
            .first()
        )
        if not name_conflict:
            company.name = official_name
        company.sector       = info.get("sector") or company.sector
        company.industry     = info.get("industry") or company.industry
        company.exchange     = info.get("exchange") or company.exchange
        company.ticker       = info.get("ticker") or company.ticker
        company.website      = info.get("website") or company.website
        company.headquarters = info.get("headquarters") or company.headquarters

    session.commit()

    # Save all scraped k/v pairs into ScrapedData for a given year
    def _upsert(key: str, value: str, source: str = "yahoo", yr: int = year):
        if value is None:
            return
        existing = (
            session.query(ScrapedData)
            .filter_by(company_id=company.id, year=yr, source=source, data_key=key)
            .first()
        )
        if existing:
            existing.data_value = str(value)
        else:
            session.add(ScrapedData(
                company_id=company.id,
                year=yr,
                source=source,
                data_key=key,
                data_value=str(value),
            ))

    for k, v in info.items():
        if v is not None:
            _upsert(k, str(v), source="yahoo")

    session.commit()

    # Print profile summary
    tbl = Table("Field", "Value", box=box.SIMPLE, show_header=True)
    for k, v in info.items():
        if v and str(v).strip() not in ("None", "nan", ""):
            tbl.add_row(k, str(v)[:80])
    console.print(tbl)

    # ── Historical financials ──────────────────────────────────────────────────
    ticker = info.get("ticker")
    if ticker:
        from backend.scraper.financial_scraper import FinancialScraper

        fin_scraper = FinancialScraper(ticker, company_name)

        with console.status(f"Fetching {historical_years} years of financials..."):
            history = fin_scraper.get_historical_financials(years=historical_years)

        if history:
            for fy, metrics in history.items():
                for metric_key, metric_val in metrics.items():
                    if metric_val is not None:
                        _upsert(metric_key, str(metric_val), source="yahoo_historical", yr=fy)

            session.commit()
            console.print(
                f"[green]✓ Saved historical financials for {len(history)} fiscal years.[/green]"
            )
            for fy in sorted(history.keys()):
                n = len(history[fy])
                console.print(f"  [dim]{fy}: {n} metrics[/dim]")

        # ── ESG scores ────────────────────────────────────────────────────────
        with console.status("Fetching ESG / sustainability scores..."):
            esg_data = fin_scraper.get_esg_scores()
        if esg_data:
            for k, v in esg_data.items():
                _upsert(k, v, source="yahoo_esg")
            session.commit()
            console.print(f"[green]✓ Saved {len(esg_data)} ESG score fields.[/green]")
        else:
            console.print("[yellow]No ESG scores available for this company.[/yellow]")

        # ── BRSR PDF scrape ───────────────────────────────────────────────────
        from backend.scraper.brsr_scraper import BRSRScraper
        brsr_scraper = BRSRScraper(company_name, ticker=ticker)
        if brsr_pdf:
            # User provided a local PDF — parse it directly
            console.print()
            console.print("  [bold cyan]📄 BRSR PDF Scraper (local file)[/bold cyan]")
            brsr_data = brsr_scraper.parse_local_pdf(brsr_pdf)
        else:
            brsr_data = brsr_scraper.scrape()
        if brsr_data:
            for k, v in brsr_data.items():
                _upsert(k, v, source="brsr_pdf", yr=year)
            session.commit()
            console.print(f"[green]✓ Saved {len(brsr_data)} fields from BRSR PDF.[/green]")

    else:
        console.print("[yellow]No ticker found — skipping historical financials.[/yellow]")

    console.print(
        Panel(
            f"[bold green]Scrape complete![/bold green]\n"
            f"Company: [cyan]{company.name}[/cyan]  |  Year: [bold]{year}[/bold]\n\n"
            f"Next: [cyan]python backend/cli.py questionnaire \"{company_name}\" --year {year}[/cyan]",
            border_style="green",
        )
    )


# ── questionnaire ─────────────────────────────────────────────────────────────

@cli.command()
@click.argument("company_name")
@click.option("--year", default=None, type=int,
              help="Fiscal year (default: current year)")
@click.option("--standard", default="ALL",
              type=click.Choice(["ALL", "BRSR", "CDP", "EcoVadis", "GRI"], case_sensitive=False),
              show_default=True)
@click.option("--module", default=None, help="Filter to a specific module, e.g. M01")
@click.option("--auto", is_flag=True, default=False,
              help="Auto-fill all scraped/historical answers without prompting")
def questionnaire(company_name: str, year: int, standard: str, module: str, auto: bool):
    """Run the sustainability questionnaire for a company and year."""
    import datetime

    year = year or datetime.date.today().year
    standard = standard.upper()

    from backend.questionnaire.engine import QuestionnaireEngine

    engine = QuestionnaireEngine(company_name, year, standard=standard)

    if auto:
        engine.run_auto(module_filter=module)
    else:
        console.print(
            Panel(
                "[bold]Controls[/bold]\n"
                "  [green]a[/green] — accept suggestion\n"
                "  [yellow]e[/yellow] — edit / enter manually\n"
                "  [red]s[/red] — skip this question\n"
                "  [red]q[/red] — quit and save progress",
                title="Interactive Questionnaire",
                border_style="blue",
            )
        )
        engine.run_interactive(module_filter=module)


# ── answers ───────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("company_name")
@click.option("--year", default=None, type=int)
@click.option("--module", default=None, help="Filter by module, e.g. M14")
def answers(company_name: str, year: int, module: str):
    """View saved answers for a company and year."""
    import datetime

    year = year or datetime.date.today().year
    from backend.questionnaire.engine import QuestionnaireEngine

    engine = QuestionnaireEngine(company_name, year)
    engine.show_answers(module_filter=module)


# ── history ───────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("company_name")
@click.option("--indicators", default=None,
              help="Comma-separated indicator IDs, e.g. IMP-M01-I01,IMP-M14-I01")
def history(company_name: str, indicators: str):
    """Show year-over-year answer history for a company."""
    import datetime

    ind_list = [i.strip() for i in indicators.split(",")] if indicators else None

    from backend.questionnaire.engine import QuestionnaireEngine

    engine = QuestionnaireEngine(company_name, datetime.date.today().year)
    engine.show_history(indicator_ids=ind_list)


# ── export ────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("company_name")
@click.option("--year", default=None, type=int)
@click.option("--format", "fmt", default="csv",
              type=click.Choice(["csv", "json"], case_sensitive=False),
              show_default=True)
@click.option("--output", default=None, help="Output file path")
def export(company_name: str, year: int, fmt: str, output: str):
    """Export saved answers to CSV or JSON."""
    import datetime

    year = year or datetime.date.today().year
    from backend.questionnaire.engine import QuestionnaireEngine

    engine = QuestionnaireEngine(company_name, year)
    out = engine.export(format=fmt, output_path=output)
    console.print(f"[green]✓ Exported to:[/green] [bold]{out}[/bold]")


# ── companies ─────────────────────────────────────────────────────────────────

@cli.command()
def companies():
    """List all companies stored in the database."""
    init_db()
    session = get_session()

    rows = session.query(Company).order_by(Company.name).all()
    if not rows:
        console.print("[yellow]No companies in database. Run 'scrape' first.[/yellow]")
        return

    tbl = Table("ID", "Name", "Ticker", "Sector", "Exchange",
                title="Companies in Database", box=box.SIMPLE_HEAVY)
    for c in rows:
        tbl.add_row(str(c.id), c.name or "", c.ticker or "", c.sector or "", c.exchange or "")
    console.print(tbl)

    # Sessions summary
    console.print()
    sess_rows = (
        session.query(QuestionnaireSession, Company)
        .join(Company, QuestionnaireSession.company_id == Company.id)
        .order_by(Company.name, QuestionnaireSession.year.desc())
        .all()
    )
    if sess_rows:
        tbl2 = Table("Company", "Year", "Standard", "Progress", "Status",
                     title="Questionnaire Sessions", box=box.SIMPLE_HEAVY)
        for qs, comp in sess_rows:
            tbl2.add_row(
                comp.name,
                str(qs.year),
                qs.standard or "ALL",
                f"{qs.answered_questions or 0}/{qs.total_questions or '?'}",
                qs.status or "—",
            )
        console.print(tbl2)


# ── entry point ───────────────────────────────────────────────────────────────

# ── search-company ────────────────────────────────────────────────────────────

@cli.command("search-company")
@click.argument("query")
@click.option("--jurisdiction", default="in", show_default=True,
              help="Two-letter country code, e.g. 'in' (India), 'gb' (UK), 'us' (USA). "
                   "Pass 'all' to search globally.")
@click.option("--limit", default=20, show_default=True,
              help="Number of results to display (max 100).")
@click.option("--api-token", default=None, envvar="OPENCORPORATES_API_TOKEN",
              help="OpenCorporates API token (or set OPENCORPORATES_API_TOKEN env var).")
def search_company(query: str, jurisdiction: str, limit: int, api_token: str):
    """Search for companies by name using the OpenCorporates API.

    \b
    Examples:
      python backend/cli.py search-company "Infosys"
      python backend/cli.py search-company "HCL" --jurisdiction in
      python backend/cli.py search-company "Tata Consultancy" --jurisdiction all
    """
    from backend.scraper.opencorporates import search_companies

    jcode = None if jurisdiction.lower() == "all" else jurisdiction.lower()

    console.print(f"\n[bold cyan]Searching OpenCorporates for:[/bold cyan] [white]{query}[/white]"
                  + (f"  [dim](jurisdiction: {jcode})[/dim]" if jcode else "  [dim](global)[/dim]"))
    console.print()

    try:
        results = search_companies(
            query,
            jurisdiction_code=jcode,
            per_page=min(limit, 100),
            api_token=api_token,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if "API token" in msg or "requires an API token" in msg:
            console.print(
                Panel(
                    "[bold red]OpenCorporates API Token Required[/bold red]\n\n"
                    "OpenCorporates requires a free API token for all search requests.\n\n"
                    "[bold]Steps to get your free token:[/bold]\n"
                    "  1. Register at [link=https://opencorporates.com/users/account_requests/new]"
                    "https://opencorporates.com[/link]\n"
                    "  2. Get your token from: Account → API Access\n"
                    "  3. Use it with:\n\n"
                    "       [bold cyan]python backend/cli.py search-company \"Infosys\" "
                    "--api-token YOUR_TOKEN[/bold cyan]\n\n"
                    "  Or export it once as an environment variable:\n\n"
                    "       [bold cyan]export OPENCORPORATES_API_TOKEN=YOUR_TOKEN[/bold cyan]\n"
                    "       [bold cyan]python backend/cli.py search-company \"Infosys\"[/bold cyan]",
                    border_style="red",
                    padding=(1, 2),
                )
            )
        else:
            console.print(f"[red]Error:[/red] {msg}")
        return

    if not results:
        console.print("[yellow]No companies found. Try a different query or jurisdiction.[/yellow]")
        return

    # ── Display numbered table ──────────────────────────────────────────────
    tbl = Table(
        "#", "Company Name", "Number", "Jurisdiction", "Status",
        "Incorporated", "Address",
        title=f"OpenCorporates Results: '{query}'",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
    )
    tbl.columns[0].style = "bold yellow"
    tbl.columns[1].min_width = 30
    tbl.columns[6].max_width = 40

    for i, co in enumerate(results, 1):
        tbl.add_row(
            str(i),
            co["name"],
            co["company_number"],
            co["jurisdiction_code"].upper(),
            co["current_status"] or "—",
            co["incorporation_date"] or "—",
            co["registered_address"] or "—",
        )

    console.print(tbl)
    console.print(f"[dim]Showing {len(results)} result(s). Use --limit N for more.[/dim]")
    console.print()

    # ── Interactive selection ───────────────────────────────────────────────
    console.print("[bold]Select a company[/bold] to view details, or press [bold]Enter[/bold] to exit.")
    choice = click.prompt(
        "Enter company number (or 0 to exit)",
        type=click.IntRange(0, len(results)),
        default=0,
        show_default=True,
    )

    if choice == 0:
        console.print("[dim]No company selected.[/dim]")
        return

    selected = results[choice - 1]

    # ── Detail panel ────────────────────────────────────────────────────────
    detail_lines = [
        f"[bold]Name:[/bold]          {selected['name']}",
        f"[bold]Company No:[/bold]    {selected['company_number']}",
        f"[bold]Jurisdiction:[/bold]  {selected['jurisdiction_code'].upper()}",
        f"[bold]Status:[/bold]        {selected['current_status'] or '—'}",
        f"[bold]Incorporated:[/bold]  {selected['incorporation_date'] or '—'}",
        f"[bold]Address:[/bold]       {selected['registered_address'] or '—'}",
        f"[bold]URL:[/bold]           {selected['opencorporates_url']}",
    ]
    console.print(
        Panel(
            "\n".join(detail_lines),
            title=f"[bold cyan]Selected Company[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )
    console.print()

    # ── Offer to use this company in Impactree ──────────────────────────────
    if click.confirm(
        f"Use [bold]{selected['name']}[/bold] as the company for scraping/questionnaire?",
        default=False,
    ):
        init_db()
        session = get_session()
        existing = (
            session.query(Company)
            .filter(Company.name == selected["name"])
            .first()
        )
        if existing:
            console.print(
                f"[green]Company already exists in database:[/green] {existing.name} (ID {existing.id})"
            )
        else:
            new_co = Company(
                name=selected["name"],
                ticker=None,
                sector=None,
                exchange=selected["jurisdiction_code"].upper(),
            )
            session.add(new_co)
            session.commit()
            console.print(
                f"[green]✓ Added to database:[/green] {selected['name']} (ID {new_co.id})"
            )
        console.print()
        console.print(
            f"[dim]Next step:[/dim]  python backend/cli.py scrape \"{selected['name']}\" --year 2024"
        )


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
