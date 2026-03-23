"""
csv_loader.py
-------------
Cached loader for all four Impactree CSV files.
Provides clean DataFrames and helper query methods.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from typing import List, Dict, Any, Optional
from config import CSV_FILES


class ImpactreeCSVLoader:
    _questionnaire_df: Optional[pd.DataFrame] = None
    _source_mapping_df: Optional[pd.DataFrame] = None
    _raw_extraction_df: Optional[pd.DataFrame] = None

    # ── Loaders ───────────────────────────────────────────────────────────────

    # Questionnaire CSV column positions → clean names
    _Q_COLS = {
        0:  "indicator_id",
        1:  "module_name",
        2:  "indicator_name",
        3:  "question",
        4:  "response_format",
        5:  "data_type",
        6:  "reporting_period",
        7:  "guidance",
        8:  "brsr",
        9:  "cdp",
        10: "ecovadis",
        11: "gri",
        12: "priority",
        13: "brsr_mapping",
        14: "cdp_mapping",
        15: "ecovadis_mapping",
        16: "gri_mapping",
    }

    @classmethod
    def questionnaire(cls) -> pd.DataFrame:
        if cls._questionnaire_df is None:
            df = pd.read_csv(CSV_FILES["questionnaire"])
            # Rename columns to clean names based on position
            rename_map = {
                df.columns[pos]: name
                for pos, name in cls._Q_COLS.items()
                if pos < len(df.columns)
            }
            df = df.rename(columns=rename_map)
            # Keep only rows that are actual indicators (IMP-MXX-IXX)
            cls._questionnaire_df = df[
                df["indicator_id"].str.match(r"^IMP-M\d+-I\d+$", na=False)
            ].reset_index(drop=True)
        return cls._questionnaire_df

    @classmethod
    def source_mapping(cls) -> pd.DataFrame:
        if cls._source_mapping_df is None:
            # Row 0 is a title banner — skip it
            df = pd.read_csv(CSV_FILES["source_mapping"], skiprows=1)
            cls._source_mapping_df = df.dropna(subset=[df.columns[0]])
        return cls._source_mapping_df

    @classmethod
    def raw_extraction(cls) -> pd.DataFrame:
        if cls._raw_extraction_df is None:
            cls._raw_extraction_df = pd.read_csv(CSV_FILES["raw_extraction"])
        return cls._raw_extraction_df

    # ── Query helpers ─────────────────────────────────────────────────────────

    @classmethod
    def get_all_indicators(cls) -> List[Dict[str, Any]]:
        """Return all 151 Impactree indicators as a list of dicts."""
        return cls.questionnaire().to_dict("records")

    @classmethod
    def get_indicator(cls, indicator_id: str) -> Optional[Dict[str, Any]]:
        df = cls.questionnaire()
        rows = df[df["indicator_id"] == indicator_id]
        return rows.iloc[0].to_dict() if not rows.empty else None

    @classmethod
    def get_indicators_by_standard(cls, standard: str) -> List[Dict[str, Any]]:
        """Return indicators that have at least one question from `standard`."""
        if standard == "ALL":
            return cls.get_all_indicators()
        df = cls.questionnaire()
        # Use the clean column name (brsr / cdp / ecovadis / gri)
        col = standard.lower()
        if col not in df.columns:
            return cls.get_all_indicators()
        filtered = df[df[col].notna() & ~df[col].isin(["—", "", "nan"])]
        return filtered.to_dict("records")

    @classmethod
    def get_indicators_by_module(cls, module_code: str) -> List[Dict[str, Any]]:
        """E.g. module_code='M05'"""
        df = cls.questionnaire()
        filtered = df[df["indicator_id"].str.contains(module_code.upper(), na=False)]
        return filtered.to_dict("records")

    @classmethod
    def get_source_questions(
        cls, indicator_id: str, standard: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Source questions that map to a given Impactree indicator."""
        df = cls.source_mapping()
        id_col  = df.columns[0]
        std_col = df.columns[3] if len(df.columns) > 3 else None
        filtered = df[df[id_col] == indicator_id]
        if standard and std_col:
            filtered = filtered[filtered[std_col].str.upper() == standard.upper()]
        return filtered.to_dict("records")

    @classmethod
    def col(cls, indicator: Dict, index: int, default: str = "") -> str:
        """Safe column accessor by position."""
        keys = list(indicator.keys())
        if index < len(keys):
            val = indicator[keys[index]]
            return str(val) if val and str(val) not in ("nan", "None") else default
        return default
