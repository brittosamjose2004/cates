"""
data_mapper.py
--------------
Maps scraped company / financial data to Impactree indicator answers.
Provides smart defaults for indicators that have no direct scraped data.
"""

from typing import Dict, Any, Optional, List, Tuple, Callable


# ── Helper formatters ─────────────────────────────────────────────────────────

def _fmt_int(v: str) -> str:
    try:
        return f"{int(float(v)):,}"
    except Exception:
        return v


def _fmt_currency(v: str) -> str:
    try:
        n = float(v)
        if abs(n) >= 1_000_000_000:
            return f"₹{n / 1_000_000_000:.2f}B"
        if abs(n) >= 1_000_000:
            return f"₹{n / 1_000_000:.2f}M"
        return f"₹{int(n):,}"
    except Exception:
        return v


def _fmt_pct(v: str) -> str:
    try:
        return f"{float(v) * 100:.1f}%"
    except Exception:
        return v


# ── Smart defaults ────────────────────────────────────────────────────────────

# Module-level context messages used when no scraped data is available
_MODULE_CONTEXT: Dict[str, str] = {
    "M02": "Refer to Sustainability Report / BRSR Filing for policy details",
    "M03": "Refer to Corporate Governance Report and Board Committee disclosures",
    "M04": "Refer to Risk Management section of Annual Report / BRSR",
    "M05": "GHG data not auto-extractable — refer to Sustainability Report / BRSR Section B",
    "M06": "Energy consumption data not auto-extractable — refer to Sustainability/BRSR Report",
    "M07": "Water consumption data not auto-extractable — refer to Sustainability Report",
    "M08": "Waste data not auto-extractable — refer to Sustainability / EHS Report",
    "M09": "Air emissions data not auto-extractable — refer to EHS/Environmental Report",
    "M10": "Biodiversity data not auto-extractable — refer to Sustainability Report",
    "M11": "Forest commodity sourcing data — refer to Supply Chain Sustainability Report",
    "M12": "Plastics data not auto-extractable — refer to Packaging / Sustainability Report",
    "M13": "Supply chain ESG data not auto-extractable — refer to Supplier Code of Conduct / BRSR",
    "M14": "Detailed labor data — refer to BRSR Labor Section / HR Report",
    "M15": "OHS data not auto-extractable — refer to BRSR / Sustainability Report",
    "M16": "DEI metrics not auto-extractable — refer to BRSR / Diversity Report",
    "M17": "Training data not auto-extractable — refer to HR / Learning & Development Report",
    "M18": "CSR/community data not auto-extractable — refer to CSR Report / BRSR Section C",
    "M19": "Product responsibility data not auto-extractable — refer to Annual Report / compliance docs",
    "M21": "Compliance data not auto-extractable — refer to Legal/Compliance Report",
}

# Response-format level hints
_FMT_HINT: Dict[str, str] = {
    "Yes / No + Description": "Status: To be confirmed — verify with relevant department",
    "Yes / No": "To be confirmed — verify internally",
    "Numeric": "Numeric value requires manual input",
    "Data Table": "Tabular data requires manual input from sustainability/EHS reports",
    "Data Table + Narrative": "Tabular + narrative data requires manual input from sustainability reports",
    "Multiple + Description": "Refer to Sustainability Report for current policy status",
    "Narrative": "Narrative response — refer to Annual Report / Sustainability Report for details",
    "Mixed": "Partially available — refer to sustainability report for complete data",
}


def smart_default(indicator: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a contextual placeholder answer for an indicator that has no
    real scraped data. Always returns a non-empty answer so every indicator
    gets filled in auto mode.
    """
    ind_id = indicator.get("indicator_id", "")
    module = ind_id.split("-")[1] if "-" in ind_id else "M00"
    fmt    = indicator.get("response_format", "Narrative")

    module_msg = _MODULE_CONTEXT.get(module, "")
    fmt_hint   = _FMT_HINT.get(fmt, "Refer to Annual Report / Sustainability Report")

    if module_msg:
        answer = f"{fmt_hint} | {module_msg}"
    else:
        answer = fmt_hint

    return {
        "answer": answer,
        "confidence": 0.10,
        "source": "smart_default",
        "note": "Auto-generated placeholder — requires manual verification",
    }


# ── Indicator map ─────────────────────────────────────────────────────────────
# indicator_id -> [(scraped_key, transform_fn, confidence, description)]

_Map = List[Tuple[str, Callable, float, str]]

INDICATOR_MAP: Dict[str, _Map] = {

    # ── M01 General & Organizational Profile ──────────────────────────────────
    "IMP-M01-I01": [
        ("official_name",       lambda v: v,                                0.95, "Registered name (Screener)"),
        ("company_name",        lambda v: v,                                0.90, "Company name"),
        ("brsr_cin",            lambda v: f"CIN: {v}",                      0.95, "Corporate ID (BRSR PDF)"),
        ("brsr_incorporation_year", lambda v: f"Year of incorporation: {v}", 0.92, "Incorporation year (BRSR PDF)"),
        ("ticker",              lambda v: f"Ticker: {v}",                   0.90, "Stock ticker"),
        ("exchange",            lambda v: f"Listed on: {v}",                0.90, "Exchange"),
        ("website",             lambda v: v,                                0.90, "Official website"),
        ("headquarters",        lambda v: v,                                0.85, "Registered / HQ address"),
        ("country",             lambda v: v,                                0.80, "Country"),
        ("shares_outstanding",  lambda v: f"Shares outstanding: {_fmt_int(v)}", 0.85, "Shares outstanding"),
    ],
    "IMP-M01-I02": [
        ("sector",              lambda v: v,                                0.85, "Sector"),
        ("industry",            lambda v: v,                                0.85, "Industry"),
        ("description",         lambda v: (v or "")[:400],                  0.70, "Business description (Yahoo)"),
        ("about",               lambda v: (v or "")[:400],                  0.75, "About (Screener)"),
        ("revenue_per_share",   lambda v: f"Revenue per share: ₹{v}",       0.75, "Revenue per share"),
        ("brsr_export_pct",     lambda v: f"Export contribution: {v}",      0.88, "Export % of turnover (BRSR)"),
    ],
    "IMP-M01-I03": [
        ("headquarters",        lambda v: v,                                0.80, "Primary location"),
        ("country",             lambda v: v,                                0.80, "Country of operation"),
        ("brsr_num_countries",  lambda v: f"Operational presence: {v}",     0.90, "Countries (BRSR PDF)"),
        ("brsr_num_offices",    lambda v: f"Offices/locations: {v}",        0.85, "Offices (BRSR PDF)"),
        ("brsr_export_pct",     lambda v: f"International revenue: {v}",    0.85, "Export % (BRSR PDF)"),
    ],
    "IMP-M01-I04": [
        ("currency",            lambda v: f"Reporting currency: {v}",       0.90, "Reporting currency"),
        ("exchange",            lambda v: f"Listed exchange: {v}",          0.80, "Exchange"),
        ("book_value",          lambda v: f"Book value per share: ₹{v}",    0.80, "Book value"),
    ],
    "IMP-M01-I05": [
        ("brsr_subsidiaries_count", lambda v: f"Number of subsidiaries: {v}", 0.92, "Subsidiaries count (BRSR PDF)"),
        ("total_assets",        lambda v: f"Total consolidated assets: {_fmt_currency(v)}", 0.70, "Total assets"),
        ("total_debt",          lambda v: f"Total debt: {_fmt_currency(v)}", 0.70, "Total debt"),
    ],
    "IMP-M01-I06": [
        ("website",             lambda v: f"Stakeholder portal: {v}",       0.50, "Website"),
        ("esg_peerCount",       lambda v: f"ESG peer group size: {v} companies", 0.60, "ESG peer count"),
    ],
    "IMP-M01-I07": [
        ("description",         lambda v: f"Business overview: {(v or '')[:300]}", 0.45, "Business description"),
        ("industry",            lambda v: f"Industry: {v}",                 0.50, "Industry"),
        ("brsr_num_countries",  lambda v: f"Operates in {v}",               0.80, "Countries"),
    ],

    # ── M02 Sustainability Management & Reporting ─────────────────────────────
    "IMP-M02-I01": [
        ("esg_totalEsg",        lambda v: f"Yahoo Finance Total ESG Score: {v}", 0.65, "Total ESG score"),
        ("brsr_iso14001_cert",  lambda v: f"ISO 14001: {v}",                0.90, "ISO 14001 (BRSR PDF)"),
        ("brsr_iso45001_cert",  lambda v: f"OHS certification: {v}",        0.90, "ISO 45001 (BRSR PDF)"),
        ("brsr_iso50001_cert",  lambda v: f"Energy certification: {v}",     0.90, "ISO 50001 (BRSR PDF)"),
        ("website",             lambda v: f"See sustainability section at {v}", 0.30, "Website"),
    ],
    "IMP-M02-I03": [
        ("brsr_iso14001_cert",  lambda v: f"Environmental: {v}",            0.92, "ISO 14001 (BRSR PDF)"),
        ("brsr_iso45001_cert",  lambda v: f"OHS: {v}",                      0.92, "ISO 45001/OHSAS (BRSR PDF)"),
        ("brsr_iso50001_cert",  lambda v: f"Energy management: {v}",        0.92, "ISO 50001 (BRSR PDF)"),
    ],
    "IMP-M02-I04": [
        ("esg_totalEsg",        lambda v: f"ESG Score: {v} — check CDP/UN Global Compact membership", 0.55, "ESG score"),
    ],
    "IMP-M02-I05": [
        ("esg_totalEsg",        lambda v: f"ESG Score (Yahoo): {v} — third-party assurance confirm with auditor", 0.50, "ESG score"),
    ],
    "IMP-M02-I07": [
        ("currency",            lambda v: f"Currency used: {v}",            0.85, "Financial reporting currency"),
        ("exchange",            lambda v: f"Listed on: {v}",                0.80, "Exchange"),
        ("esg_totalEsg",        lambda v: f"ESG Score (Yahoo Finance): {v}", 0.65, "ESG score"),
    ],
    "IMP-M02-I08": [
        ("esg_totalEsg",        lambda v: f"Total ESG Score: {v} — align to relevant SDGs via sustainability report", 0.45, "ESG score"),
    ],

    # ── M03 Governance & Ethics ───────────────────────────────────────────────
    "IMP-M03-I01": [
        ("brsr_board_size",         lambda v: f"Total board strength: {v}",     0.92, "Board size (BRSR PDF)"),
        ("brsr_independent_directors", lambda v: f"Independent directors: {v}", 0.92, "Independent directors (BRSR PDF)"),
        ("brsr_board_meetings",     lambda v: f"Board meetings held: {v}",      0.90, "Board meetings (BRSR PDF)"),
        ("esg_governanceScore",     lambda v: f"Yahoo Finance Governance Score: {v}", 0.65, "Governance ESG score"),
    ],
    "IMP-M03-I02": [
        ("brsr_board_size",         lambda v: f"Board size: {v}",               0.80, "Board size (BRSR PDF)"),
        ("esg_governanceScore",     lambda v: f"Governance Score: {v} — refer to Corporate Governance Report", 0.55, "Governance score"),
    ],
    "IMP-M03-I03": [
        ("brsr_sh_complaints",      lambda v: f"Sexual harassment cases: {v}",  0.88, "SH complaints (BRSR PDF)"),
        ("brsr_complaints_filed",   lambda v: f"Total grievances filed: {v}",   0.85, "Complaints filed (BRSR PDF)"),
    ],
    "IMP-M03-I04": [
        ("brsr_complaints_filed",   lambda v: f"Complaints received: {v}",      0.80, "Complaints (BRSR PDF)"),
        ("brsr_complaints_pending", lambda v: f"Complaints pending: {v}",       0.80, "Pending (BRSR PDF)"),
    ],
    "IMP-M03-I06": [
        ("Tax Provision",       lambda v: f"Tax expense: {_fmt_currency(v)}", 0.88, "Tax Provision (IS)"),
        ("Tax Rate For Calcs",  lambda v: f"Effective tax rate: {_fmt_pct(v)}", 0.85, "Tax Rate"),
        ("currency",            lambda v: f"Reporting currency: {v}",       0.85, "Currency"),
    ],
    "IMP-M03-I08": [
        ("brsr_complaints_filed",   lambda v: f"Grievances filed: {v}",         0.90, "Grievances filed (BRSR PDF)"),
        ("brsr_complaints_pending", lambda v: f"Grievances pending: {v}",       0.90, "Grievances pending (BRSR PDF)"),
        ("brsr_sh_complaints",      lambda v: f"Sexual harassment complaints: {v}", 0.88, "SH cases (BRSR PDF)"),
    ],
    "IMP-M03-I09": [
        ("brsr_data_breaches",      lambda v: f"Data breach incidents: {v}",       0.80, "Data breaches (BRSR PDF)"),
        ("brsr_complaints_filed",   lambda v: f"Regulatory complaints filed: {v}",  0.78, "Complaints filed (BRSR PDF)"),
        ("esg_governanceScore",     lambda v: f"Governance ESG Score: {v} — penalties confirmed in Annual Report", 0.45, "Governance score"),
    ],

    # ── M04 Risk & Opportunity Management ────────────────────────────────────
    "IMP-M04-I01": [
        ("esg_totalEsg",        lambda v: f"Total ESG Risk Score: {v} — risk process described in BRSR/Annual Report", 0.50, "ESG risk score"),
    ],
    "IMP-M04-I02": [
        ("esg_totalEsg",        lambda v: f"Total ESG Risk Score: {v} — refer to TCFD/Climate risk section", 0.55, "ESG risk score"),
        ("beta",                lambda v: f"Market beta (volatility indicator): {v}", 0.60, "Beta"),
    ],
    "IMP-M04-I04": [
        ("brsr_water_withdrawal", lambda v: f"Total water withdrawal: {v} (context for water risk)", 0.65, "Water (BRSR PDF)"),
        ("brsr_water_intensity",  lambda v: f"Water intensity: {v}",             0.70, "Water intensity (BRSR PDF)"),
    ],

    # ── M05 GHG Emissions & Climate Change ───────────────────────────────────
    "IMP-M05-I01": [
        ("brsr_scope1_ghg",     lambda v: v,                                0.92, "Scope 1 GHG (BRSR PDF)"),
    ],
    "IMP-M05-I02": [
        ("brsr_scope2_ghg",     lambda v: v,                                0.92, "Scope 2 GHG (BRSR PDF)"),
    ],
    "IMP-M05-I03": [
        ("brsr_scope3_ghg",     lambda v: v,                                0.90, "Scope 3 GHG (BRSR PDF)"),
        ("brsr_total_ghg",      lambda v: f"Total GHG (all scopes): {v}",   0.85, "Total GHG (BRSR PDF)"),
    ],
    "IMP-M05-I04": [
        ("brsr_ghg_intensity",  lambda v: v,                                0.88, "GHG intensity (BRSR PDF)"),
        ("brsr_energy_intensity", lambda v: f"Energy intensity: {v}",       0.82, "Energy intensity (BRSR PDF)"),
    ],
    "IMP-M05-I05": [
        ("brsr_ghg_reduction_pct", lambda v: f"GHG reduction achieved: {v}", 0.90, "GHG reduction % (BRSR PDF)"),
        ("brsr_ghg_intensity",  lambda v: f"Current GHG intensity: {v}",    0.80, "GHG intensity (BRSR PDF)"),
    ],
    "IMP-M05-I08": [
        ("brsr_ghg_reduction_pct", lambda v: f"Emission reduction achieved: {v}", 0.85, "GHG reduction (BRSR PDF)"),
        ("brsr_renewable_energy_pct", lambda v: f"Renewable energy share: {v}", 0.82, "Renewable % (BRSR PDF)"),
    ],

    # ── M06 Energy ────────────────────────────────────────────────────────────
    "IMP-M06-I01": [
        ("brsr_total_energy",       lambda v: v,                            0.92, "Total energy consumption (BRSR PDF)"),
    ],
    "IMP-M06-I02": [
        ("brsr_renewable_energy",   lambda v: v,                            0.90, "Renewable energy (BRSR PDF)"),
        ("brsr_renewable_energy_pct", lambda v: f"Renewable share: {v}",    0.88, "Renewable % (BRSR PDF)"),
    ],
    "IMP-M06-I03": [
        ("brsr_nonrenewable_energy", lambda v: v,                           0.90, "Non-renewable energy (BRSR PDF)"),
    ],
    "IMP-M06-I05": [
        ("brsr_energy_intensity",   lambda v: v,                            0.88, "Energy intensity (BRSR PDF)"),
    ],
    "IMP-M06-I06": [
        ("brsr_renewable_energy_pct", lambda v: f"Renewable energy share: {v}", 0.88, "Renewable % (BRSR PDF)"),
        ("brsr_energy_intensity",   lambda v: f"Energy intensity: {v}",     0.82, "Energy intensity (BRSR PDF)"),
    ],
    "IMP-M06-I07": [
        ("brsr_renewable_energy",   lambda v: f"Renewable energy used: {v}", 0.80, "Renewable energy (BRSR PDF)"),
        ("brsr_renewable_energy_pct", lambda v: f"Renewable share: {v}",    0.82, "Renewable % (BRSR PDF)"),
        ("Research And Development", lambda v: f"R&D investment (includes energy tech): {_fmt_currency(v)}", 0.55, "R&D spend"),
    ],

    # ── M07 Water & Effluents ─────────────────────────────────────────────────
    "IMP-M07-I01": [
        ("brsr_water_withdrawal",   lambda v: v,                            0.90, "Water withdrawal (BRSR PDF)"),
    ],
    "IMP-M07-I02": [
        ("brsr_water_consumption",  lambda v: v,                            0.90, "Water consumption (BRSR PDF)"),
    ],
    "IMP-M07-I03": [
        ("brsr_water_recycled",     lambda v: v,                            0.88, "Water recycled (BRSR PDF)"),
    ],
    "IMP-M07-I04": [
        ("brsr_water_discharge",    lambda v: v,                            0.90, "Water discharge (BRSR PDF)"),
    ],
    "IMP-M07-I05": [
        ("brsr_water_intensity",    lambda v: v,                            0.88, "Water intensity (BRSR PDF)"),
    ],

    # ── M08 Waste & Materials ─────────────────────────────────────────────────
    "IMP-M08-I01": [
        ("brsr_total_waste",        lambda v: v,                            0.90, "Total waste generated (BRSR PDF)"),
    ],
    "IMP-M08-I02": [
        ("brsr_waste_recycled",     lambda v: v,                            0.88, "Waste recycled/recovered (BRSR PDF)"),
    ],
    "IMP-M08-I04": [
        ("brsr_hazardous_waste",    lambda v: v,                            0.88, "Hazardous waste (BRSR PDF)"),
    ],

    # ── M09 Air Quality ───────────────────────────────────────────────────────
    "IMP-M09-I01": [
        ("brsr_nox_emissions",      lambda v: v,                            0.90, "NOx emissions (BRSR PDF)"),
    ],
    "IMP-M09-I02": [
        ("brsr_sox_emissions",      lambda v: v,                            0.90, "SOx emissions (BRSR PDF)"),
    ],
    "IMP-M09-I03": [
        ("brsr_pm_emissions",       lambda v: v,                            0.90, "PM emissions (BRSR PDF)"),
    ],
    "IMP-M09-I04": [
        ("brsr_voc_emissions",      lambda v: v,                            0.88, "VOC emissions (BRSR PDF)"),
    ],
    "IMP-M09-I07": [
        ("brsr_nox_emissions",      lambda v: f"NOx: {v}",                  0.75, "NOx (BRSR PDF)"),
        ("brsr_sox_emissions",      lambda v: f"SOx: {v}",                  0.75, "SOx (BRSR PDF)"),
        ("brsr_pm_emissions",       lambda v: f"PM: {v}",                   0.75, "PM (BRSR PDF)"),
    ],

    # ── M13 Supply Chain & Procurement ───────────────────────────────────────
    "IMP-M13-I01": [
        ("brsr_msme_sourcing_pct",  lambda v: f"MSME sourcing: {v}",        0.90, "MSME sourcing % (BRSR PDF)"),
        ("brsr_local_sourcing_pct", lambda v: f"Local sourcing: {v}",       0.90, "Local sourcing % (BRSR PDF)"),
    ],
    "IMP-M13-I02": [
        ("brsr_supplier_assessed_pct", lambda v: f"Suppliers assessed for ESG: {v}", 0.88, "Suppliers assessed (BRSR PDF)"),
    ],
    "IMP-M13-I03": [
        ("brsr_supplier_assessed_pct", lambda v: f"Suppliers assessed: {v}", 0.80, "Suppliers assessed (BRSR PDF)"),
    ],
    "IMP-M13-I04": [
        ("brsr_msme_sourcing_pct",  lambda v: f"MSME procurement: {v}",     0.90, "MSME % (BRSR PDF)"),
        ("brsr_local_sourcing_pct", lambda v: f"Local procurement: {v}",    0.88, "Local % (BRSR PDF)"),
    ],

    # ── M14 Labor & Human Rights ──────────────────────────────────────────────
    "IMP-M14-I01": [
        ("brsr_total_employees",  lambda v: f"Total permanent employees: {v}",  0.95, "Total employees (BRSR PDF)"),
        ("brsr_male_employees",   lambda v: f"Male employees: {v}",             0.92, "Male employees (BRSR PDF)"),
        ("brsr_female_employees", lambda v: f"Female employees: {v}",           0.92, "Female employees (BRSR PDF)"),
        ("brsr_total_workers",    lambda v: f"Total workers: {v}",              0.92, "Workers (BRSR PDF)"),
        ("brsr_contract_workers", lambda v: f"Contract workers: {v}",           0.90, "Contract workers (BRSR PDF)"),
        ("brsr_differently_abled", lambda v: f"Differently abled: {v}",         0.90, "Differently abled (BRSR PDF)"),
        ("total_employees",       _fmt_int,                                    0.88, "Full-time employees (Yahoo)"),
    ],
    "IMP-M14-I02": [
        ("brsr_turnover_rate",    lambda v: f"Employee turnover rate: {v}",     0.90, "Turnover rate (BRSR PDF)"),
        ("brsr_attrition_rate",   lambda v: f"Attrition rate: {v}",             0.90, "Attrition rate (BRSR PDF)"),
        ("brsr_new_hires",        lambda v: f"New hires this year: {v}",        0.85, "New hires (BRSR PDF)"),
    ],
    "IMP-M14-I03": [
        ("brsr_min_wage_pct",     lambda v: f"Employees paid ≥ minimum wage: {v}", 0.92, "Min wage compliance (BRSR PDF)"),
        ("brsr_median_salary",    lambda v: f"Median annual remuneration: {v}", 0.88, "Median salary (BRSR PDF)"),
        ("total_employees",       lambda v: f"Total employees: {_fmt_int(v)}", 0.70, "Headcount for wage calc"),
        ("Selling General And Administration", lambda v: f"SG&A (includes employee costs): {_fmt_currency(v)}", 0.65, "SG&A"),
    ],
    "IMP-M14-I04": [
        ("brsr_health_insurance_pct", lambda v: f"Health insurance coverage: {v}", 0.92, "Health insurance (BRSR PDF)"),
        ("brsr_pf_coverage_pct",   lambda v: f"Provident fund coverage: {v}",    0.90, "PF coverage (BRSR PDF)"),
    ],
    "IMP-M14-I05": [
        ("brsr_new_hires",        lambda v: f"New permanent employees: {v} (for parental leave context)", 0.60, "New hires (BRSR PDF)"),
        ("brsr_female_employees", lambda v: f"Female employees: {v}",           0.65, "Female employees (BRSR PDF)"),
    ],
    "IMP-M14-I09": [
        ("esg_socialScore",     lambda v: f"Yahoo Finance Social Score: {v}", 0.60, "Social ESG score"),
        ("brsr_total_employees", lambda v: f"Employees to be trained: {v}",   0.55, "Total employees (BRSR PDF)"),
    ],
    "IMP-M14-I10": [
        ("esg_socialScore",     lambda v: f"Social Score: {v} — refer to Human Rights Policy / BRSR", 0.55, "Social ESG score"),
    ],
    "IMP-M14-I11": [
        ("brsr_sh_complaints",    lambda v: f"Sexual harassment complaints: {v}", 0.92, "SH cases (BRSR PDF)"),
        ("brsr_complaints_filed", lambda v: f"Total grievances filed: {v}",      0.88, "Grievances filed (BRSR PDF)"),
        ("brsr_complaints_pending", lambda v: f"Pending at year-end: {v}",       0.88, "Pending grievances (BRSR PDF)"),
    ],

    # ── M15 Occupational Health & Safety ─────────────────────────────────────
    "IMP-M15-I01": [
        ("brsr_iso45001_cert",   lambda v: f"OHS certification: {v}",           0.92, "ISO 45001 (BRSR PDF)"),
        ("brsr_total_employees", lambda v: f"Employees covered: {v}",           0.80, "Employees (BRSR PDF)"),
        ("esg_socialScore",      lambda v: f"Social ESG Score: {v} — OHS system confirm internally", 0.45, "Social score"),
    ],
    "IMP-M15-I04": [
        ("brsr_ltifr",           lambda v: f"LTIFR: {v}",                        0.92, "LTIFR (BRSR PDF)"),
        ("brsr_safety_incidents", lambda v: f"Recordable safety incidents: {v}", 0.88, "Safety incidents (BRSR PDF)"),
    ],
    "IMP-M15-I05": [
        ("brsr_fatalities",      lambda v: f"Work-related fatalities: {v}",      0.95, "Fatalities (BRSR PDF)"),
    ],
    "IMP-M15-I08": [
        ("brsr_ohs_training_pct", lambda v: f"Safety training coverage: {v}",   0.90, "OHS training % (BRSR PDF)"),
        ("brsr_total_training_hrs", lambda v: f"Total training hours org-wide: {v}", 0.80, "Training hrs (BRSR PDF)"),
    ],
    "IMP-M15-I09": [
        ("brsr_iso45001_cert",   lambda v: f"OHS certified: {v}",                0.85, "ISO 45001 (BRSR PDF)"),
        ("brsr_ohs_training_pct", lambda v: f"Safety training coverage: {v}",   0.82, "OHS training % (BRSR PDF)"),
    ],

    # ── M16 Diversity, Equity & Inclusion ────────────────────────────────────
    "IMP-M16-I01": [
        ("brsr_board_women",    lambda v: f"Women directors: {v}",               0.92, "Women directors (BRSR PDF)"),
        ("brsr_board_size",     lambda v: f"Total board strength: {v}",          0.90, "Board size (BRSR PDF)"),
        ("brsr_women_percent",  lambda v: f"Women in workforce: {v}",            0.88, "Women % workforce (BRSR PDF)"),
        ("esg_socialScore",     lambda v: f"Social Score: {v} — refer to BRSR DEI section", 0.45, "Social ESG score"),
    ],
    "IMP-M16-I02": [
        ("brsr_male_employees",   lambda v: f"Male employees: {v}",              0.92, "Male employees (BRSR PDF)"),
        ("brsr_female_employees", lambda v: f"Female employees: {v}",            0.92, "Female employees (BRSR PDF)"),
        ("brsr_women_percent",    lambda v: f"Women in workforce: {v}",          0.90, "Women % (BRSR PDF)"),
        ("total_employees",       lambda v: f"Total workforce: {_fmt_int(v)}", 0.55, "Total employees"),
    ],
    "IMP-M16-I03": [
        ("brsr_differently_abled", lambda v: f"Differently abled employees: {v}", 0.92, "Differently abled (BRSR PDF)"),
    ],
    "IMP-M16-I06": [
        ("brsr_avg_training_hrs", lambda v: f"Average training hrs/employee: {v}", 0.80, "Avg training hrs (BRSR PDF)"),
        ("brsr_total_employees",  lambda v: f"Total employees reviewed: {v}",    0.70, "Total employees (BRSR PDF)"),
    ],

    # ── M17 Training & Skill Development ─────────────────────────────────────
    "IMP-M17-I01": [
        ("brsr_total_employees",  lambda v: f"Employees to be covered: {v}",     0.75, "Employees (BRSR PDF)"),
        ("brsr_board_size",       lambda v: f"Board members to be trained: {v}", 0.70, "Board size (BRSR PDF)"),
    ],
    "IMP-M17-I02": [
        ("brsr_ohs_training_pct", lambda v: f"Safety training coverage: {v}",   0.90, "OHS training % (BRSR PDF)"),
        ("brsr_total_employees",  lambda v: f"Total employees: {v}",             0.70, "Headcount (BRSR PDF)"),
    ],
    "IMP-M17-I03": [
        ("brsr_avg_training_hrs", lambda v: f"Average training hours/employee: {v}", 0.92, "Avg training hrs (BRSR PDF)"),
        ("brsr_total_training_hrs", lambda v: f"Total training hours: {v}",      0.90, "Total training hrs (BRSR PDF)"),
        ("Research And Development", lambda v: f"R&D investment: {_fmt_currency(v)}", 0.55, "R&D spend"),
        ("total_employees",     lambda v: f"Workforce being trained: {_fmt_int(v)} employees", 0.50, "Headcount"),
    ],

    # ── M18 Community & Social Impact ────────────────────────────────────────
    "IMP-M18-I04": [
        ("brsr_csr_spend",      lambda v: f"CSR expenditure: {v}",               0.95, "CSR expenditure (BRSR PDF)"),
        ("brsr_csr_obligatory", lambda v: f"Mandatory CSR obligation: {v}",      0.95, "CSR mandatory amt (BRSR PDF)"),
        ("brsr_csr_projects",   lambda v: f"CSR projects undertaken: {v}",       0.90, "CSR projects (BRSR PDF)"),
        ("Net Income",          lambda v: f"Net profit (CSR 2% basis): {_fmt_currency(v)}", 0.82, "Net Income"),
        ("Pretax Income",       lambda v: f"Pre-tax income: {_fmt_currency(v)}",            0.78, "Pretax Income"),
    ],
    "IMP-M18-I03": [
        ("brsr_volunteers",        lambda v: f"Employee volunteers: {v}",         0.85, "Volunteers (BRSR PDF)"),
        ("brsr_csr_spend",         lambda v: f"CSR investment: {v}",             0.75, "CSR spend (BRSR PDF)"),
        ("brsr_csr_projects",      lambda v: f"CSR projects undertaken: {v}",    0.80, "CSR projects (BRSR PDF)"),
    ],
    "IMP-M18-I05": [
        ("brsr_csr_beneficiaries", lambda v: f"CSR beneficiaries reached: {v}",  0.92, "CSR beneficiaries (BRSR PDF)"),
        ("brsr_volunteers",        lambda v: f"Employee volunteers engaged: {v}", 0.88, "Volunteers (BRSR PDF)"),
        ("brsr_csr_projects",      lambda v: f"CSR projects: {v}",               0.85, "CSR projects (BRSR PDF)"),
    ],
    "IMP-M18-I06": [
        ("brsr_volunteers",        lambda v: f"Community volunteers: {v}",       0.85, "Volunteers (BRSR PDF)"),
        ("brsr_csr_beneficiaries", lambda v: f"Lives impacted: {v}",             0.82, "CSR beneficiaries (BRSR PDF)"),
        ("brsr_num_countries",     lambda v: f"Operating in {v} (community presence)", 0.70, "Countries (BRSR PDF)"),
    ],

    # ── M19 Customer & Product Responsibility ────────────────────────────────
    "IMP-M19-I05": [
        ("brsr_consumer_complaints", lambda v: f"Consumer complaints filed: {v}", 0.90, "Consumer complaints (BRSR PDF)"),
        ("brsr_complaints_pending",  lambda v: f"Complaints pending: {v}",        0.85, "Pending (BRSR PDF)"),
    ],
    "IMP-M19-I06": [
        ("brsr_data_breaches",       lambda v: f"Data breach incidents: {v}",     0.90, "Data breaches (BRSR PDF)"),
        ("esg_privacyAndDataSecurity", lambda v: f"Privacy & Data Security score: {v}", 0.65, "ESG privacy score"),
    ],
    "IMP-M19-I08": [
        ("esg_customerSatisfaction", lambda v: f"Customer Satisfaction score: {v}", 0.65, "ESG customer score"),
        ("brsr_consumer_complaints", lambda v: f"Consumer complaints: {v}",       0.70, "Consumer complaints (BRSR PDF)"),
    ],

    # ── M20 Economic Performance ──────────────────────────────────────────────
    "IMP-M20-I01": [
        ("Total Revenue",            lambda v: f"Total Revenue: {_fmt_currency(v)}",     0.92, "Total Revenue (IS)"),
        ("Gross Profit",             lambda v: f"Gross Profit: {_fmt_currency(v)}",      0.88, "Gross Profit (IS)"),
        ("Operating Income",         lambda v: f"Operating Income: {_fmt_currency(v)}",  0.88, "Operating Income (IS)"),
        ("EBIT",                     lambda v: f"EBIT: {_fmt_currency(v)}",               0.85, "EBIT (IS)"),
        ("Net Income",               lambda v: f"Net Income: {_fmt_currency(v)}",         0.90, "Net Income (IS)"),
        ("Tax Provision",            lambda v: f"Tax Paid: {_fmt_currency(v)}",           0.85, "Tax Provision (IS)"),
        ("cf_Cash Dividends Paid",   lambda v: f"Dividends Paid: {_fmt_currency(v)}",    0.85, "Dividends (CF)"),
        ("market_cap",               lambda v: f"Market Cap: {_fmt_currency(v)}",         0.80, "Market Cap"),
        ("revenue",                  lambda v: f"Revenue (Yahoo): {_fmt_currency(v)}",   0.82, "Revenue (Yahoo)"),
        ("gross_margins",            lambda v: f"Gross Margin: {_fmt_pct(v)}",            0.85, "Gross margin"),
        ("profit_margins",           lambda v: f"Net Profit Margin: {_fmt_pct(v)}",       0.85, "Profit margin"),
        ("operating_margins",        lambda v: f"Operating Margin: {_fmt_pct(v)}",        0.85, "Operating margin"),
        ("earnings_per_share",       lambda v: f"EPS: ₹{v}",                              0.85, "EPS"),
        ("brsr_csr_spend",           lambda v: f"CSR spend (community investment): {v}", 0.80, "CSR spend (BRSR PDF)"),
    ],
    "IMP-M20-I02": [
        ("EBITDA",                   lambda v: f"EBITDA: {_fmt_currency(v)}",             0.82, "EBITDA (IS)"),
        ("cf_Operating Cash Flow",   lambda v: f"Operating Cash Flow: {_fmt_currency(v)}", 0.82, "OCF (CF)"),
        ("cf_Free Cash Flow",        lambda v: f"Free Cash Flow: {_fmt_currency(v)}",     0.80, "FCF (CF)"),
        ("ebitda",                   lambda v: f"EBITDA (Yahoo): {_fmt_currency(v)}",     0.80, "EBITDA (Yahoo)"),
        ("roe",                      lambda v: f"Return on Equity: {_fmt_pct(v)}",        0.80, "ROE"),
        ("roa",                      lambda v: f"Return on Assets: {_fmt_pct(v)}",        0.80, "ROA"),
        ("debt_to_equity",           lambda v: f"Debt-to-Equity: {v}",                    0.80, "D/E ratio"),
        ("current_ratio",            lambda v: f"Current Ratio: {v}",                     0.80, "Current ratio"),
    ],
    "IMP-M20-I03": [
        ("Research And Development", lambda v: f"R&D Spend: {_fmt_currency(v)}",          0.90, "R&D (IS)"),
        ("cf_Capital Expenditure",   lambda v: f"Capital Expenditure: {_fmt_currency(v)}", 0.90, "Capex (CF)"),
        ("cf_Purchase Of PPE",       lambda v: f"PPE Purchase: {_fmt_currency(v)}",       0.85, "PPE (CF)"),
        ("Operating Expense",        lambda v: f"Total Operating Expense: {_fmt_currency(v)}", 0.75, "OpEx"),
    ],
    "IMP-M20-I04": [
        ("description",              lambda v: f"Business overview (for LCA scope): {(v or '')[:200]}", 0.25, "Business description"),
        ("industry",                 lambda v: f"Industry sector: {v}",                   0.40, "Industry"),
    ],

    # ── M21 Legal & Environmental Compliance ─────────────────────────────────
    "IMP-M21-I01": [
        ("brsr_iso14001_cert",   lambda v: f"Environmental certification: {v}", 0.82, "ISO 14001 (BRSR PDF)"),
        ("esg_totalEsg",         lambda v: f"Total ESG Risk Score: {v} — environmental compliance detail in Annual Report", 0.50, "ESG score"),
    ],
    "IMP-M21-I03": [
        ("brsr_sh_complaints",   lambda v: f"Sexual harassment cases: {v}",     0.85, "SH cases (BRSR PDF)"),
        ("esg_totalEsg",         lambda v: f"ESG Score: {v} — labor law compliance confirm internally", 0.45, "ESG score"),
    ],
    "IMP-M21-I04": [
        ("brsr_complaints_filed",  lambda v: f"Total grievances filed: {v}",    0.82, "Grievances (BRSR PDF)"),
        ("brsr_data_breaches",     lambda v: f"Data breach incidents: {v}",     0.80, "Data breaches (BRSR PDF)"),
        ("esg_totalEsg",           lambda v: f"ESG Score: {v} — compliance detail in Annual Report", 0.45, "ESG score"),
    ],
}


class DataMapper:
    def __init__(self, scraped_data: Dict[str, Any]):
        self.scraped = scraped_data

    def map_all(self) -> Dict[str, Dict[str, Any]]:
        """Return {indicator_id: {answer, confidence, source, note}} for mapped indicators."""
        result: Dict[str, Dict[str, Any]] = {}

        for ind_id, mappings in INDICATOR_MAP.items():
            parts: List[str] = []
            best_conf = 0.0
            best_note = ""

            for scrape_key, transform_fn, confidence, note in mappings:
                val = self.scraped.get(scrape_key)
                if val and str(val).strip() not in ("", "None", "nan", "NaN"):
                    try:
                        transformed = str(transform_fn(str(val).strip()))
                        if transformed and transformed not in ("None", "nan"):
                            parts.append(transformed)
                            if confidence > best_conf:
                                best_conf = confidence
                                best_note = note
                    except Exception:
                        pass

            if parts:
                seen: set = set()
                unique = [p for p in parts if not (p in seen or seen.add(p))]  # type: ignore
                result[ind_id] = {
                    "answer":     " | ".join(unique),
                    "confidence": best_conf,
                    "source":     "scraped",
                    "note":       best_note,
                }

        return result

    def get(self, indicator_id: str) -> Optional[Dict[str, Any]]:
        return self.map_all().get(indicator_id)

