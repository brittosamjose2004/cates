import sqlite3
from backend.questionnaire.engine import QuestionnaireEngine

company_name = "COAL INDIA LIMITED"
company_id = 32
year = 2025

engine = QuestionnaireEngine(
    company_name,
    year,
    standard="ALL",
    allow_historical_prefill=False,
    allow_smart_defaults=True,
    allow_cross_year_financial_fallback=False,
    allow_online_provisional_fallback=True,
    provisional_max_attempts=8,
    provisional_time_budget_seconds=30,
)
engine.setup()

if engine.company and engine.company.id != company_id:
    from backend.database.db import get_session
    from backend.database.models import Company
    db = get_session()
    exact = db.query(Company).filter_by(id=company_id).first()
    if exact:
        engine.company = exact
    db.close()

rows = engine.run_auto(module_filter=None)
print("rows", rows)

conn = sqlite3.connect("data/impactree.db")
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM answers WHERE company_id=? AND year=?", (company_id, year))
print("answer_count", cur.fetchone()[0])
cur.execute("SELECT source, COUNT(*) FROM answers WHERE company_id=? AND year=? GROUP BY source ORDER BY COUNT(*) DESC", (company_id, year))
print("source_counts", cur.fetchall())
conn.close()
