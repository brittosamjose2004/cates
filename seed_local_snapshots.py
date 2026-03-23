import os
import glob
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Adjust path so backend is importable
import sys
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from backend.database.db import init_db, get_session
from backend.database.models import Company, ScrapedData, QuestionnaireSession, Answer, PipelineJob
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

def seed_database_from_snapshots():
    print("Initializing Database...")
    init_db()
    db = get_session()

    snapshot_files = glob.glob(str(ROOT / "data" / "company_data" / "*" / "latest_snapshot.json"))
    print(f"Found {len(snapshot_files)} snapshot files.")

    for sf in snapshot_files:
        print(f"Processing {sf}...")
        with open(sf, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        c_data = data.get("company", {})
        if not c_data:
            continue
            
        c_name = c_data.get("name")
        print(f"  -> Company: {c_name}")
        
        # 1. Upsert Company
        company = db.query(Company).filter(Company.name == c_name).first()
        if not company:
            if c_data.get("ticker"):
                company = db.query(Company).filter(Company.ticker == c_data.get("ticker")).first()
                
        if not company:
            company = Company(
                name=c_name,
                cin=c_data.get("cin"),
                sector=c_data.get("sector"),
                exchange=c_data.get("exchange"),
                ticker=c_data.get("ticker"),
                website=c_data.get("website"),
                headquarters=c_data.get("headquarters")
            )
            db.add(company)
            db.commit()
            db.refresh(company)
        else:
            company.cin = c_data.get("cin") or company.cin
            company.sector = c_data.get("sector") or company.sector
            company.exchange = c_data.get("exchange") or company.exchange
            company.ticker = c_data.get("ticker") or company.ticker
            company.website = c_data.get("website") or company.website
            company.headquarters = c_data.get("headquarters") or company.headquarters
            db.commit()

        # 2. Upsert ScrapedData
        scraped_data_list = data.get("scraped_data", [])
        for sd in scraped_data_list:
            stmt = sqlite_insert(ScrapedData).values(
                company_id=company.id,
                year=sd.get("year"),
                source=sd.get("source"),
                data_key=sd.get("key"),
                data_value=sd.get("value"),
                scraped_at=datetime.fromisoformat(sd.get("scraped_at", datetime.utcnow().isoformat())) if sd.get("scraped_at") else datetime.utcnow()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["company_id", "year", "source", "data_key"],
                set_={
                    "data_value": sd.get("value"),
                    "scraped_at": datetime.fromisoformat(sd.get("scraped_at", datetime.utcnow().isoformat())) if sd.get("scraped_at") else datetime.utcnow()
                }
            )
            db.execute(stmt)
        db.commit()

        # 3. Upsert QuestionnaireSession
        sessions_list = data.get("sessions", [])
        session_id_map = {} # Map old session ID to new session ID
        for s in sessions_list:
            stmt = sqlite_insert(QuestionnaireSession).values(
                company_id=company.id,
                year=s.get("year"),
                standard=s.get("standard", "ALL"),
                status=s.get("status", "in_progress"),
                total_questions=s.get("total_questions", 0),
                answered_questions=s.get("answered_questions", 0),
                created_at=datetime.fromisoformat(s.get("created_at", datetime.utcnow().isoformat())) if s.get("created_at") else datetime.utcnow(),
                updated_at=datetime.fromisoformat(s.get("updated_at", datetime.utcnow().isoformat())) if s.get("updated_at") else datetime.utcnow()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["company_id", "year", "standard"],
                set_={
                    "status": s.get("status", "in_progress"),
                    "total_questions": s.get("total_questions", 0),
                    "answered_questions": s.get("answered_questions", 0),
                    "updated_at": datetime.fromisoformat(s.get("updated_at", datetime.utcnow().isoformat())) if s.get("updated_at") else datetime.utcnow()
                }
            )
            db.execute(stmt)
            db.commit()
            
            # Get the session back
            new_session = db.query(QuestionnaireSession).filter_by(
                company_id=company.id, year=s.get("year"), standard=s.get("standard", "ALL")
            ).first()
            if new_session:
                session_id_map[s.get("id")] = new_session.id

        # 4. Upsert Answer
        answers_list = data.get("answers", [])
        for ans in answers_list:
            mapped_session_id = session_id_map.get(ans.get("session_id"))
            if not mapped_session_id:
                # Fallback, just pick the first session for that year
                fallback = db.query(QuestionnaireSession).filter_by(company_id=company.id, year=ans.get("year")).first()
                if fallback:
                    mapped_session_id = fallback.id
                else:
                    continue # Skip if no session

            stmt = sqlite_insert(Answer).values(
                session_id=mapped_session_id,
                company_id=company.id,
                year=ans.get("year"),
                indicator_id=ans.get("indicator_id"),
                module=ans.get("module"),
                indicator_name=ans.get("indicator_name"),
                answer_value=ans.get("answer_value"),
                answer_unit=ans.get("answer_unit"),
                source=ans.get("source"),
                confidence=ans.get("confidence"),
                is_verified=ans.get("is_verified", False),
                updated_at=datetime.fromisoformat(ans.get("updated_at", datetime.utcnow().isoformat())) if ans.get("updated_at") else datetime.utcnow()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["company_id", "year", "indicator_id"],
                set_={
                    "answer_value": ans.get("answer_value"),
                    "answer_unit": ans.get("answer_unit"),
                    "source": ans.get("source"),
                    "confidence": ans.get("confidence"),
                    "is_verified": ans.get("is_verified", False),
                    "updated_at": datetime.fromisoformat(ans.get("updated_at", datetime.utcnow().isoformat())) if ans.get("updated_at") else datetime.utcnow()
                }
            )
            db.execute(stmt)
        db.commit()

        print(f"  -> Inserted {len(scraped_data_list)} scraped_data, {len(sessions_list)} sessions, {len(answers_list)} answers.")

    print("Database seeding from local snapshots completed successfully!")

if __name__ == "__main__":
    seed_database_from_snapshots()
