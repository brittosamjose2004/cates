from pathlib import Path
import os

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent          # /workspaces/new_sap/
DATA_DIR = BASE_DIR / "data"
CSV_DIR  = BASE_DIR                              # CSVs live at root

DB_PATH  = DATA_DIR / "impactree.db"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Optional cloud database URL (e.g., Vercel Postgres / Neon / Supabase).
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# ── CSV files ──────────────────────────────────────────────────────────────────
CSV_FILES = {
    "questionnaire":  CSV_DIR / "Impactree_Standard_Questionnaire_v1.0.xlsx - Impactree Questionnaire.csv",
    "source_mapping": CSV_DIR / "Impactree_Standard_Questionnaire_v1.0.xlsx - Source Mapping.csv",
    "raw_extraction": CSV_DIR / "Impactree_Standard_Questionnaire_v1.0.xlsx - Raw Extraction Log.csv",
    "coverage":       CSV_DIR / "Impactree_Standard_Questionnaire_v1.0.xlsx - Coverage Dashboard.csv",
}

# ── Standards ──────────────────────────────────────────────────────────────────
STANDARDS = ["BRSR", "CDP", "EcoVadis", "GRI", "ALL"]
