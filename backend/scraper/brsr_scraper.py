"""
brsr_scraper.py
---------------
Downloads the most recent BRSR (Business Responsibility & Sustainability Report)
PDF from NSE India filings for a given company, then extracts key ESG metrics.

Extraction covers 3 sections of the annual report PDF:
  - First 30 pages  : company profile, CIN, subsidiaries, key highlights
  - Middle 40-65%   : governance report, board composition, directors
  - Last 90 pages   : BRSR / Sustainability section

Extracted keys (all prefixed brsr_):
  Profile  : brsr_cin, brsr_incorporation_year, brsr_subsidiaries_count,
             brsr_num_countries, brsr_num_offices, brsr_export_pct
  Board    : brsr_board_size, brsr_independent_directors, brsr_board_meetings
  GHG      : brsr_scope1_ghg, brsr_scope2_ghg, brsr_scope3_ghg,
             brsr_total_ghg, brsr_ghg_intensity, brsr_ghg_reduction_pct
  Energy   : brsr_total_energy, brsr_renewable_energy, brsr_nonrenewable_energy,
             brsr_energy_intensity, brsr_renewable_energy_pct
  Water    : brsr_water_withdrawal, brsr_water_consumption, brsr_water_recycled,
             brsr_water_discharge, brsr_water_intensity
  Waste    : brsr_total_waste, brsr_waste_recycled, brsr_hazardous_waste
  Air      : brsr_nox_emissions, brsr_sox_emissions, brsr_pm_emissions,
             brsr_voc_emissions
  OHS      : brsr_fatalities, brsr_ltifr, brsr_safety_incidents,
             brsr_ohs_training_pct
  Labor    : brsr_total_employees, brsr_male_employees, brsr_female_employees,
             brsr_total_workers, brsr_contract_workers, brsr_differently_abled,
             brsr_new_hires, brsr_turnover_rate, brsr_attrition_rate,
             brsr_avg_training_hrs, brsr_total_training_hrs,
             brsr_median_salary, brsr_min_wage_pct,
             brsr_health_insurance_pct, brsr_pf_coverage_pct
  DEI      : brsr_women_percent, brsr_board_women
  Compliants: brsr_complaints_filed, brsr_complaints_pending,
              brsr_sh_complaints, brsr_consumer_complaints, brsr_data_breaches
  Certs    : brsr_iso14001_cert, brsr_iso45001_cert, brsr_iso50001_cert
  CSR      : brsr_csr_spend, brsr_csr_projects, brsr_csr_obligatory,
             brsr_csr_beneficiaries
  Supply   : brsr_msme_sourcing_pct, brsr_local_sourcing_pct,
             brsr_supplier_assessed_pct
"""

import re
import os
import tempfile
import requests
from typing import Dict, Any, Optional

from rich.console import Console

console = Console()


# ── Regex patterns ────────────────────────────────────────────────────────────
# Each entry: (result_key, regex_pattern, unit_label)
# - Numeric captures: value stored as "{number} {unit}"
# - Text captures: value stored as-is (certifications, CIN, year)
# - Zero-capture patterns: unit label stored as the value

_PATTERNS = [
    # ── GHG Emissions ─────────────────────────────────────────────────────────
    ("brsr_scope1_ghg",
     r"scope[\s\-]*1[^\n]{0,300}?(\d[\d,\.]+)\s*(?:tco2e|tco2|mt\s*co2e?|metric\s*tons?\s*co2)",
     "tCO2e"),
    ("brsr_scope1_ghg",    # BRSR table: 'Total Scope 1 emissions...↵Metric tons...↵12,900.5' (HCL) or '8,745(2) 7,150' (Infosys with footnote)
     r"[Tt]otal\s+[Ss]cope\s+1\s+emissions[\s\S]{0,400}?([1-9][\d,]+(?:\.\d+)?)(?:\(\d+\))?\s+[1-9][\d,]+",
     "tCO2e"),
    ("brsr_scope2_ghg",
     r"scope[\s\-]*2[^\n]{0,300}?([\d,\.]+)\s*(?:tco2e|tco2|mt\s*co2e?|metric\s*tons?\s*co2)",
     "tCO2e"),
    ("brsr_scope2_ghg",    # BRSR table: 'Total Scope 2 emissions...↵Metric tons...↵109,074.02' (HCL) or '38,586(2) 55,881' (Infosys)
     r"[Tt]otal\s+[Ss]cope\s+2\s+emissions[\s\S]{0,400}?([1-9][\d,]+(?:\.\d+)?)(?:\(\d+\))?\s+[1-9][\d,]+",
     "tCO2e"),
    ("brsr_scope3_ghg",
     r"scope[\s\-]*3[^\n]{0,300}?(\d[\d,\.]+)\s*(?:tco2e|tco2|mt\s*co2e?|metric\s*tons?\s*co2)",
     "tCO2e"),
    ("brsr_scope3_ghg",         # 'Total Scope 3 emissions 772,372' (no unit on same line)
     r"[Tt]otal\s+[Ss]cope\s+3\s+emissions?\s+([\d,]+)\b",
     "tCO2e"),
    ("brsr_scope3_ghg",         # Infosys: 'Total Scope 3 emissions...1,79,370(1) 1,80,737' (cross-newline with footnote)
     r"[Tt]otal\s+[Ss]cope\s+3\s+emissions[\s\S]{0,400}?([1-9][\d,]+(?:\.\d+)?)(?:\(\d+\))?\s+[1-9][\d,]+",
     "tCO2e"),
    ("brsr_total_ghg",
     r"total\s+(?:ghg\s+)?emissions?[^\n]{0,200}?(\d[\d,\.]+)\s*(?:tco2e|tco2|mt)",
     "tCO2e"),
    ("brsr_ghg_intensity",
     r"(?:ghg|emission|carbon)\s+intensity[^\n]{0,150}?(\d[\d,\.]+)",
     "tCO2e/unit"),
    ("brsr_ghg_reduction_pct",
     r"(?:ghg|emission|carbon)\s+reduction[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% reduction"),

    # ── Energy ────────────────────────────────────────────────────────────────
    ("brsr_total_energy",
     r"total\s+energy\s+consumption[^\n]{0,200}?(\d[\d,\.]+)\s*(?:gj|gigajoule|mwh|terajoule|tj)",
     "GJ"),
    ("brsr_total_energy",       # compact "323,445GJ" or "323,445 GJ"
     r"(\d[\d,]{4,})\s*GJ\b(?!/|\s*per|\s*unit)",
     "GJ"),
    ("brsr_total_energy",       # TCS: 'Total energy consumed (A+B+C+D+E+F) 1,94,09,26,732' / Infosys: '(A + B + C + D + E + F)(1) 8,50,434' (integer)
     r"[Tt]otal\s+energy\s+consumed\s+\([A-Z\s+]+\)(?:\(\d+\))?\s+([1-9][\d,]+)\s",
     "MJ"),
    ("brsr_total_energy",       # HCL: 'Total energy consumed (A+B+C+D+E+F) 940,692.98' (GJ unit, decimal)
     r"[Tt]otal\s+energy\s+consumed\s+\([A-Z\s+]+\)(?:\(\d+\))?\s+([1-9][\d,]+(?:\.\d+)?)\s",
     "GJ"),
    ("brsr_renewable_energy",
     r"(?:total\s+)?renewable\s+energy[^\n]{0,200}?(\d[\d,\.]+)\s*(?:gj|gigajoule|mwh)",
     "GJ"),
    ("brsr_renewable_energy",   # TCS: 'Total energy consumed from renewable sources (A+B+C) 1,53,76,37,748' (MJ integer)
     r"[Tt]otal\s+energy\s+consumed\s+from\s+renewable[^\n]{0,50}?\s+([1-9][\d,]+)\s",
     "MJ"),
    ("brsr_renewable_energy",   # HCL: '...from renewable sources (A+B+C) 323,444.62' (GJ decimal)
     r"[Tt]otal\s+energy\s+consumed\s+from\s+renewable[^\n]{0,50}?\s+([1-9][\d,]+(?:\.\d+)?)\s",
     "GJ"),
    ("brsr_renewable_energy",   # Infosys: 'Total energy consumption (A + B + C) 5,85,702' (renewable subtotal, GJ)
     r"[Tt]otal\s+energy\s+consumption\s+\([ABC\s+]{3,15}\)(?:\(\d+\))?\s+([1-9][\d,]+)\s",
     "GJ"),
    ("brsr_nonrenewable_energy",
     r"non[-\s\u2013]*renewable\s+energy[^\n]{0,200}?(\d[\d,\.]+)\s*(?:gj|gigajoule|mwh)",
     "GJ"),
    ("brsr_nonrenewable_energy",  # TCS: '...from non-renewable sources (D+E+F) 40,32,88,984' (MJ integer)
     r"[Tt]otal\s+energy\s+consumed\s+from\s+non[-\s\u2013]*renewable[^\n]{0,50}?\s+([1-9][\d,]+)\s",
     "MJ"),
    ("brsr_nonrenewable_energy",  # HCL: '...from non–renewable sources (D+E+F) 617,248.35' (GJ decimal, en-dash)
     r"[Tt]otal\s+energy\s+consumed\s+from\s+non[-\s\u2013]*renewable[^\n]{0,50}?\s+([1-9][\d,]+(?:\.\d+)?)\s",
     "GJ"),
    ("brsr_energy_intensity",
     r"energy\s+intensity[^\n]{0,150}?(?<!\d)([1-9][\d,\.]+)",
     "GJ/unit"),
    ("brsr_renewable_energy_pct",
     r"renewable\s+energy[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% of total"),

    # ── Water ─────────────────────────────────────────────────────────────────
    ("brsr_water_withdrawal",
     r"(?:total\s+)?water\s+withdrawal[^\n]{0,200}?([1-9][\d,]+(?:\.\d+)?)\s*(?:kl|kilolitre|ml|megalitre|cum|m3)",
     "KL"),
    ("brsr_water_withdrawal",   # TCS/HCL: 'Total volume of water withdrawal...34,88,269' or '849,928.34' (handles both integer and decimal)
     r"[Tt]otal\s+volume\s+of\s+water\s+withdrawal[^\n]{0,80}?\s+([1-9][\d,]+(?:\.\d+)?)\s",
     "KL"),
    ("brsr_water_consumption",
     r"(?:total\s+)?water\s+consumption[^\n]{0,200}?([1-9][\d,]+(?:\.\d+)?)[ \t]*(?:kl|kilolitre|ml|megalitre)",
     "KL"),
    ("brsr_water_consumption",  # TCS/HCL: 'Total volume of water consumption...28,71,784' or '834,013.09' (handles both)
     r"[Tt]otal\s+volume\s+of\s+water\s+consumption[^\n]{0,50}?\s+([1-9][\d,]+(?:\.\d+)?)\s",
     "KL"),
    ("brsr_water_recycled",
     r"water\s+recycle[d]?[^\n]{0,150}?(\d[\d,\.]+)\s*(?:kl|kilolitre|ml|%)",
     "KL"),
    ("brsr_water_recycled",    # BRSR table: 'Water Recycled and Reused (in KL) 6,16,485'  (unit before value)
     r"water\s+recycle[d]?\s+(?:and\s+reuse[d]?\s*)?(?:\([^\)]*kl[^\)]*\))[^\n]{0,60}?(\d[\d,\.]+)",
     "KL"),
    ("brsr_water_recycled",    # Exact BRSR standard-table row: 'Water Recycled and Reused (in KL) NUMBER'
     r"[Ww]ater\s+[Rr]ecycled\s+and\s+[Rr]eused[^\n]{0,30}?([1-9][\d,\.]+)",
     "KL"),
    ("brsr_water_recycled",    # Cross-newline variant with (in KL) on same or next line
     r"water\s+recycle[d]?[^\n]{0,100}?(?:in\s+kl|in\s+kilolitre)[^\n]{0,60}?\n[^\n]{0,30}?(\d[\d,\.]+)",
     "KL"),
    ("brsr_water_discharge",
     r"(?:total\s+)?(?:water\s+discharge[d]?|effluent\s+discharge[d]?)[^\n]{0,200}?(\d[\d,\.]+)\s*(?:kl|kilolitre|ml|megalitre)",
     "KL"),
    ("brsr_water_intensity",
     r"water\s+intensity\s+in\s+terms\s+of\s+physical\s+output[^\n]{0,80}?(?<!\d)([1-9][\d,\.]+)",
     "KL/employee"),
    ("brsr_water_intensity",
     r"water\s+intensity[^\n]{0,150}?(?<!\d)([1-9][\d,\.]+)",
     "KL/unit"),

    # ── Waste ─────────────────────────────────────────────────────────────────
    ("brsr_total_waste",
     r"total\s+waste\s*(?:generated)?[^\n]{0,200}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton|tonne)",
     "MT"),
    ("brsr_total_waste",       # BRSR table: 'Total Waste Generated (in MT) 1234.56' (unit before value)
     r"total\s+waste\s*generated[^\n]{0,40}?(?:\([^\)]*mt[^\)]*\))[^\n]{0,60}?(\d[\d,\.]+)",
     "MT"),
    ("brsr_total_waste",       # BRSR table without unit: 'Total Waste Generated NUMBER'
     r"[Tt]otal\s+[Ww]aste\s+[Gg]enerated\s+([1-9][\d,\.]+)",
     "MT"),
    ("brsr_total_waste",       # Cross-newline e-waste/office-waste sum line
     r"total\s+waste[^\n]{0,100}?(?:in\s+mt|metric\s*ton)[^\n]{0,60}?\n[^\n]{0,30}?(\d[\d,\.]+)",
     "MT"),
    ("brsr_waste_recycled",
     r"waste\s+recycle[d]?[^\n]{0,150}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton|tonne|%)",
     "MT"),
    ("brsr_waste_recycled",    # BRSR table: 'Waste Recycled (in MT) XXX'
     r"waste\s+recycle[d]?[^\n]{0,40}?(?:\([^\)]*mt[^\)]*\))[^\n]{0,60}?(\d[\d,\.]+)",
     "MT"),
    ("brsr_waste_recycled",    # BRSR table without unit
     r"[Ww]aste\s+[Rr]ecycled[^\n]{0,30}?([1-9][\d,\.]+)",
     "MT"),
    ("brsr_hazardous_waste",
     r"hazardous\s+waste[^\n]{0,200}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton)",
     "MT"),
    ("brsr_hazardous_waste",   # BRSR table: 'Hazardous Waste (in MT) XXX'
     r"hazardous\s+waste[^\n]{0,40}?(?:\([^\)]*mt[^\)]*\))[^\n]{0,60}?(\d[\d,\.]+)",
     "MT"),
    ("brsr_hazardous_waste",   # BRSR table without explicit unit
     r"[Hh]azardous\s+[Ww]aste\s+([1-9][\d,\.]+)",
     "MT"),

    # ── Air Quality ───────────────────────────────────────────────────────────
    ("brsr_nox_emissions",      # HCL: 'Nox Tons 2.52'  / Infosys: 'NOx Kg 40,286' (unit BEFORE value)
     r"[Nn]ox\s+(?:[Tt]ons?|MT|[Kk]g|mg|mg/?m3|tonne?s?)\s+(\d[\d,\.]*)",
     "MT"),
    ("brsr_nox_emissions",
     r"(?:nox\b|nitrogen\s+oxide)[^\n]{0,200}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton|kg|tonne|t\b)",
     "MT"),
    ("brsr_sox_emissions",      # HCL: 'Sox Tons 0.19' / Infosys: 'SOx Kg 873' (unit BEFORE value)
     r"[Ss]ox\s+(?:[Tt]ons?|MT|[Kk]g|mg|tonne?s?)\s+(\d[\d,\.]*)",
     "MT"),
    ("brsr_sox_emissions",
     r"(?:sox\b|so2\b|sulphur\s+oxide|sulfur\s+oxide)[^\n]{0,200}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton|kg|tonne|t\b)",
     "MT"),
    ("brsr_pm_emissions",       # HCL: 'Particulate matter Tons 0.38' / Infosys: 'Particulate matter (PM) Kg 4,423' (unit BEFORE value)
     r"[Pp]articulate\s+matter[^\n]{0,30}?(?:[Tt]ons?|MT|[Kk]g|tonne?s?)\s+(\d[\d,\.]*)",
     "MT"),
    ("brsr_pm_emissions",
     r"(?:pm[\s\-]*2\.?5|pm[\s\-]*10\b|particulate\s+matter)[^\n]{0,200}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton|kg|tonne|t\b)",
     "MT"),
    ("brsr_voc_emissions",
     r"(?:voc\b|volatile\s+organic\s+compound)[^\n]{0,200}?(\d[\d,\.]+)\s*(?:mt|metric\s*ton|kg|tonne|t\b)",
     "MT"),

    # ── OHS ───────────────────────────────────────────────────────────────────
    ("brsr_fatalities",
     r"(?:number\s+of\s+)?fatalit(?:y|ies)[^\n]{0,100}?(\d+)",
     "count"),
    ("brsr_ltifr",
     r"ltifr[^\n]{0,100}?(\d[\d,\.]+)",
     "rate/million hrs"),
    ("brsr_safety_incidents",    # BRSR table: 'Total recordable work-related injuries Employees 29 27'
     r"[Tt]otal\s+recordable\s+work[\s\-]*related\s+injur(?:y|ies)[^\n]{0,60}?(\d{1,5})\b(?!\s*[-–]\s*\d)",
     "count"),
    ("brsr_safety_incidents",    # reversed: '133 Safety incidents' (TCS format)
     r"(\d{1,5})\s+[Ss]afety\s+incidents?\b",
     "count"),
    ("brsr_safety_incidents",    # alternate: 'N work-related incidents'
     r"(\d{1,4})\s+(?:work[\s\-]*related\s+|recordable\s+)?incidents?",
     "count"),
    ("brsr_ohs_training_pct",
     r"(?:health\s+(?:and\s+)?safety|ohs|safety)\s+training[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% employees"),

    # ── Employees & Workers ───────────────────────────────────────────────────
    ("brsr_total_employees",    # BRSR table: '3 Total employees (E+F) 234,496'
     r"[Tt]otal\s+employees\s+\([^)]+\)\s+([1-9][\d,]+)",
     "count"),
    ("brsr_total_employees",
     r"total\s+(?:number\s+of\s+)?(?:permanent\s+)?employees[^\n]{0,100}?(\d[\d,]+)",
     "count"),
    ("brsr_total_employees",    # BEFORE LABEL: '3,23,578\nEmployees' (Infosys/Indian number format)
     r"([1-9][\d,]{4,})\s*\n(?:Employees|Headcount)\b",
     "count"),
    ("brsr_total_employees",    # TCS: 'NNN,NNN associates globally'
     r"([1-9]\d{2,}[,\d]*)\+?\s+associates?\s+(?:globally|in\s+\d+)",
     "count"),
    ("brsr_male_employees",
     r"(?:^|\b)male[^\n]{0,60}?([1-9][\d,]{3,})\b",
     "count"),
    ("brsr_female_employees",
     r"(?:^|\b)female[^\n]{0,60}?([1-9][\d,]{3,})\b",
     "count"),
    ("brsr_total_workers",
     r"total\s+(?:number\s+of\s+)?workers[^\n]{0,100}?(\d[\d,]+)",
     "count"),
    ("brsr_contract_workers",
     r"(?:contract(?:ual)?|temporary|casual)\s+workers?[^\n]{0,80}?(\d[\d,]+)",
     "count"),
    ("brsr_differently_abled",
     r"differently[\s\-]abled[^\n]{0,80}?(\d[\d,]+)",
     "count"),
    ("brsr_new_hires",
     r"new\s+(?:permanent\s+)?(?:employees?|hires?|joiners?)\s*[:\-]?\s*(\d[\d,]+)",
     "count"),
    ("brsr_turnover_rate",
     r"(?:employee\s+)?turnover\s*(?:rate)?[^\n]{0,80}?(\d[\d,\.]+)\s*%",
     "%"),
    ("brsr_attrition_rate",
     r"attrition\s+rate[^\n]{0,80}?(\d[\d,\.]+)\s*%",
     "%"),
    ("brsr_attrition_rate",     # BRSR table: 'Permanent\nEmployees\n13.06% 12.81% 5.66% 12.99%' → Total is 4th
     r"Permanent\s*\n?\s*Employees?\s*\n?\s*(?:[\d\.]+%\s+){2,3}([\d\.]+)%",
     "%"),
    ("brsr_attrition_rate",     # reversed: '13.3% LTM attrition'
     r"(\d[\d,\.]+)\s*%\s*(?:ltm\s+|last.{0,15}months?.{0,5}\s+)?attrition",
     "%"),
    ("brsr_attrition_rate",     # Infosys: 'Permanent employees 14.5 13.6 14.1 ...' (M F Total FY25 columns)
     r"[Pp]ermanent\s+employees\s+[\d\.]+\s+[\d\.]+\s+([\d\.]+)\s",
     "%"),
    ("brsr_avg_training_hrs",   # SPECIFIC first: 'Average Learning Hours↵per employee² 96.4'
     r"(?:average|avg)\s+learning\s+hours?[\s\S]{0,30}?per\s+employee\s*\d?\s+([\d]{2,3}\.[\d])",
     "hours/employee"),
    ("brsr_avg_training_hrs",   # generic: 'average training hours N per employee'
     r"(?:average\s+training|avg\s+training)\s+hours?[^\n]{0,80}?per\s+employee[^\n]{0,20}?(\d[\d,\.]+)",
     "hours/employee"),
    ("brsr_total_training_hrs",
     r"total\s+(?:training|learning)\s+hours?[^\n]{0,80}?(\d[\d,]+)",
     "hours"),
    ("brsr_total_training_hrs",   # '56 Million+ learning hours'
     r"(\d[\d,\.]+)\s*[Mm]illion\+?\s+(?:learning|training)\s+hours?",
     "million hours"),

    # ── Wages & Benefits ──────────────────────────────────────────────────────
    ("brsr_median_salary",
     r"median\s+(?:annual\s+)?(?:remuneration|salary|wages?)\s*:?[^\n]{0,100}?(?:₹|rs\.?\s*|inr\s*)?(\d[\d,]+)",
     "INR"),
    ("brsr_min_wage_pct",
     r"(?:equal\s+to\s+or\s+(?:more|above)\s+than\s+|at\s+least\s+)minimum\s+wage[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% of workforce"),
    ("brsr_health_insurance_pct",
     r"health\s+(?:(?:and\s+accident)\s+)?insurance[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% covered"),
    ("brsr_pf_coverage_pct",
     r"provident\s+fund[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% covered"),

    # ── DEI ───────────────────────────────────────────────────────────────────
    ("brsr_women_percent",
     r"women[^\n]{0,60}?(\d[\d,\.]+)\s*%",
     "%"),
    ("brsr_board_women",
     r"women\s+(?:director|on\s+(?:the\s+)?board)[^\n]{0,80}?(\d+)",
     "count"),

    # ── Board & Governance ────────────────────────────────────────────────────
    ("brsr_board_size",
     r"board[^\n]{0,60}?(?:comprises?|consists?\s+of|has)\s*(\d+)\s*(?:members?|directors?)",
     "directors"),
    ("brsr_board_size",         # alt: "N directors on the board / as on"
     r"(\d+)\s*directors?\s+(?:on\s+the\s+board|as\s+on|comprise)",
     "directors"),
    ("brsr_board_size",         # Infosys: 'Board of Directors 9 2 22.2' (women representation table)
     r"[Bb]oard\s+of\s+[Dd]irectors\s+(\d{1,2})\b",
     "directors"),
    ("brsr_independent_directors",
     r"(\d{1,2})\s*(?:non[\s\-]*executive\s+)?independent\s+directors?",
     "count"),
    ("brsr_independent_directors",  # alt
     r"independent\s+directors?[^\n]{0,60}?:\s*(\d{1,2})",
     "count"),
    ("brsr_board_meetings",
     r"(\d{1,2})\s*board\s+meetings?\s+(?:were\s+)?(?:held|conducted)",
     "meetings/year"),
    ("brsr_board_meetings",     # alt: '7 meetings held by the Board'
     r"(\d{1,2})\s*meetings?\s+(?:were\s+)?held\s+(?:by\s+the\s+)?board",
     "meetings/year"),

    # ── Company Profile ───────────────────────────────────────────────────────
    ("brsr_cin",
     r"CIN[^\n]{0,80}([A-Z]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6})",
     ""),
    ("brsr_incorporation_year",
     r"[Yy]ear\s+of\s+[Ii]ncorporation[^\n]{0,50}((?:19|20)\d{2})",
     ""),
    ("brsr_incorporation_year",    # fallback: 'incorporated in 1981'
     r"(?:incorporated|established|founded)[^\n]{0,80}?((?:19|20)\d{2})",
     ""),
    ("brsr_subsidiaries_count",
     r"(\d+)\s+(?:wholly[\s\-]*owned\s+)?subsidiaries?",
     "subsidiaries"),
    ("brsr_subsidiaries_count",  # alt
     r"subsidiaries?[^\n]{0,50}?:\s*(\d+)",
     "subsidiaries"),
    ("brsr_num_countries",
     r"(?:present\s+in|operates?\s+(?:in|across)|across)\s*(\d+)\s*countries",
     "countries"),
    ("brsr_num_offices",
     r"(\d{3,})\s*(?:development\s+|delivery\s+|service\s+)?(?:offices?|locations?|delivery\s+centers?)",
     "offices"),
    ("brsr_export_pct",
     r"(?:exports?|international\s+(?:revenues?|business))[^\n]{0,100}?(\d[\d,\.]+)\s*%",
     "% of revenue"),

    # ── Complaints & Grievances ───────────────────────────────────────────────
    ("brsr_complaints_filed",
     r"(?:total\s+)?(?:grievances?|complaints?)\s+(?:filed|received)[^\n]{0,80}?(\d{1,5})\b(?![-–\d])",
     "filed"),
    ("brsr_complaints_filed",   # BRSR table format: 'Number of complaints filed during the year XXX'
     r"number\s+of\s+complaints?\s+(?:filed|received)[^\n]{0,80}?(\d{1,5})\b",
     "filed"),
    ("brsr_complaints_pending",
     r"(?:grievances?|complaints?)\s+pending[^\n]{0,80}?(\d{1,5})\b(?![-–\d])",
     "pending"),
    ("brsr_complaints_pending",
     r"number\s+of\s+complaints?\s+pending[^\n]{0,80}?(\d{1,5})\b",
     "pending"),
    ("brsr_sh_complaints",      # Most specific: 'sexual harassment ... filed/received: N'
     r"sexual\s+harassment[^\n]{0,150}?(?:filed|received|complaints?)[^\n]{0,50}?(\d+)",
     "cases"),
    ("brsr_sh_complaints",      # Alt: 'Complaints on sexual harassment: 3' or table format
     r"complaints?\s+on\s+sexual\s+harassment[^\n]{0,80}?(\d+)",
     "cases"),
    ("brsr_sh_complaints",      # TCS BRSR table: 'Sexual Harassment 125 23 - 110 17 -'  (prev_pending filed - resolved pending -)
     # Captures the SECOND number (filed during year) from the table row — comes BEFORE simple match
     r"[Ss]exual\s+[Hh]arassment\s+\d+\s+(\d+)\s+[-–]",
     "cases filed"),
    ("brsr_sh_complaints",      # Standard BRSR table: 'Sexual Harassment NUMBER' (count follows directly)
     r"[Ss]exual\s+[Hh]arassment\s+(\d+)",
     "cases"),
    ("brsr_consumer_complaints",
     r"consumer\s+complaints?[^\n]{0,80}?(\d+)",
     "count"),
    ("brsr_data_breaches",
     r"data\s+breach(?:es?)?[^\n]{0,80}?(\d+)",
     "incidents"),
    ("brsr_data_breaches",      # BRSR: 'Number of instances of data breaches \n  1' (value on next line)
     r"[Nn]umber\s+of\s+instances\s+of\s+data\s+breach(?:es?)?\s*\n\s*(\d+)",
     "incidents"),
    ("brsr_data_breaches",      # BRSR: 'Number of data breach incidents: 0' or 'Cyber security incidents: X'
     r"(?:cyber\s*security\s*incidents?|privacy\s*breach(?:es?)?)[^\n]{0,80}?(\d+)",
     "incidents"),

    # ── ISO Certifications (text captures) ───────────────────────────────────
    ("brsr_iso14001_cert",
     r"(ISO[\s\-]*14001)",
     "ISO 14001 certified"),
    ("brsr_iso45001_cert",
     r"((?:ISO[\s\-]*45001|OHSAS[\s\-]*18001))",
     "OHS certified"),
    ("brsr_iso50001_cert",
     r"(ISO[\s\-]*50001)",
     "ISO 50001 certified"),

    # ── CSR ───────────────────────────────────────────────────────────────────
    ("brsr_csr_spend",
     r"(?:total\s+)?csr\s+(?:expenditure|spend|amount\s+spent)[^\n]{0,150}?(?:₹|rs\.?\s*)?(\d[\d,\.]+)\s*(?:crore|cr\.?|lakh)?",
     "INR Cr"),
    ("brsr_csr_spend",          # number BEFORE label: ₹1,038 crore \n Global CSR spend
     r"(?:₹|rs\.?\s*)(\d[\d,\.]+)\s*(?:crore|cr\.?)[\s\S]{0,100}?(?:global\s+)?csr\s+spend",
     "INR Cr"),
    ("brsr_csr_spend",          # foundation grant / social spend
     r"(?:hclfoundation|csr\s+grant|social\s+spend)[^\n]{0,150}?(?:₹\s*)(\d[\d,\.]+)\s*(?:crore|cr\.?)",
     "INR Cr"),
    ("brsr_csr_obligatory",
     r"(?:prescribed|mandatory|obligatory)\s+csr[^\n]{0,150}?(?:₹|rs\.?\s*)?(\d[\d,\.]+)\s*(?:crore|cr\.?)",
     "INR Cr"),
    ("brsr_csr_projects",
     r"(?:number\s+of\s+)?csr\s+projects?[^\n]{0,80}?(\d+)",
     "count"),
    ("brsr_csr_beneficiaries",   # handles '14.8 million beneficiaries' or '125 mn+ beneficiaries' or '7.5 million lives'
     r"(\d[\d,\.]+)\s*(?:million|mn)\+?\s+(?:beneficiar|lives?\b)",
     "million people"),
    ("brsr_csr_beneficiaries",   # plain count >= 1000 only
     r"beneficiar(?:y|ies)[^\n]{0,80}?(\d[\d,]{3,})",
     "people"),

    # ── Supply Chain ─────────────────────────────────────────────────────────
    ("brsr_msme_sourcing_pct",
     r"(?:msme|small\s+(?:and\s+medium|enterprise))[^\n]{0,80}?(\d[\d,\.]+)\s*%",
     "% of procurement"),
    ("brsr_local_sourcing_pct",
     r"local\s+(?:sourcing|procurement|suppliers?)[^\n]{0,80}?(\d[\d,\.]+)\s*%",
     "% local"),
    ("brsr_supplier_assessed_pct",
     r"supplier[^\n]{0,80}?(?:assessed|audited|evaluated)[^\n]{0,50}?(\d[\d,\.]+)\s*%",
     "% assessed"),
    ("brsr_supplier_assessed_pct",  # 'covers 100% of suppliers'
     r"covers?\s*100\s*%\s+of[^\n]{0,50}?suppliers?",
     "100% of suppliers assessed"),

    # ── Key highlight numbers (large round numbers in annual report) ──────────
    ("brsr_total_employees",    # '600,000+ employees' in highlights
     r"([1-9]\d{2,}[,\d]*)\+?\s*(?:full[\s\-]*time\s+)?employees",
     "count"),
    ("brsr_volunteers",
     r"(\d[\d,\.]+[Kk]?\+?)\s+volunteers?",
     "count"),
]


# ── Scraper ───────────────────────────────────────────────────────────────────

class BRSRScraper:
    NSE_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    }

    def __init__(self, company_name: str, ticker: str = ""):
        self.company_name = company_name
        # Strip exchange suffix  e.g. "INFY.NS" → "INFY"
        self.nse_symbol = ticker.replace(".NS", "").replace(".BO", "").upper()

    # ── Step 1: Find annual report PDF URL from NSE ───────────────────────────

    def _find_annual_report_url(self) -> Optional[str]:
        """
        Use NSE India's annual reports API to find the latest annual report PDF.
        Returns the PDF URL (contains BRSR as a section), or None.
        """
        if not self.nse_symbol:
            return None
        try:
            url = (
                f"https://www.nseindia.com/api/annual-reports"
                f"?index=equities&symbol={self.nse_symbol}&start=0&end=5"
            )
            resp = requests.get(url, headers=self.NSE_HEADERS, timeout=15)
            if resp.status_code != 200:
                return None
            data = resp.json()
            filings = data.get("data", [])
            if not filings:
                return None
            # Most recent filing first
            return filings[0].get("fileName") or None
        except Exception:
            return None

    # ── Step 2: Download PDF ──────────────────────────────────────────────────

    def _download_pdf(self, url: str) -> Optional[str]:
        """Download PDF to a temp file. Returns file path or None."""
        try:
            resp = requests.get(
                url, headers=self.NSE_HEADERS, timeout=180, stream=True
            )
            if resp.status_code != 200:
                return None
            ct = resp.headers.get("Content-Type", "")
            if "pdf" not in ct.lower() and not url.lower().split("?")[0].endswith(".pdf"):
                return None
            tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    tmp.write(chunk)
            tmp.close()
            return tmp.name
        except Exception:
            return None

    # ── Step 3a: Extract from first 30 pages (company profile, CIN) ──────────

    def _extract_profile_text(self, pdf_path: str) -> str:
        """Extract text from first 30 pages: CIN, subsidiaries, key highlights."""
        try:
            import pypdf
            reader = pypdf.PdfReader(pdf_path)
            texts = []
            for i in range(min(30, len(reader.pages))):
                t = reader.pages[i].extract_text() or ""
                if t.strip():
                    texts.append(t)
            return "\n".join(texts)
        except Exception:
            return ""

    # ── Step 3b: Extract BRSR + governance from last ~120 pages ──────────────

    def _extract_brsr_text(self, pdf_path: str) -> str:
        """
        Extract text from the BRSR section using pypdf (fast).
        Falls back to pdfplumber if pypdf yields no text (scanned PDFs).
        Strategy: Scan the full document to find the BRSR/Sustainability section
        (may be in mid-document for some companies), then extract up to 180 pages
        from that start point. Falls back to the last 120 pages if not found.
        """
        # Keywords in PRIORITY order: most-specific formal section headers first.
        # Generic keywords like "brsr" appear in highlights/summaries far before
        # the actual formal BRSR section with standardised tables.
        brsr_keywords_priority = [
            # Formal section headers (most specific — found only in the actual BRSR section)
            "i. details of the listed entity",
            "details of the listed entity",
            "section c: principle",
            "principle-wise performance",
            # Section header keywords (appear in the BR&S report itself, not summaries)
            "business responsibility and sustainability report",
            # Fallback generic keyword
            "brsr",
        ]
        MAX_EXTRACT = 180   # max pages to extract once section is found
        TAIL_PAGES  = 120   # fallback tail size if BRSR not found anywhere

        def _extract_with_pypdf(pdf_path: str, start: int, end: int):
            try:
                import pypdf
                reader = pypdf.PdfReader(pdf_path)
                texts = []
                for i in range(start, min(end, len(reader.pages))):
                    t = reader.pages[i].extract_text() or ""
                    texts.append(t)
                return texts
            except Exception:
                return []

        def _extract_with_pdfplumber(pdf_path: str, start: int, end: int):
            try:
                import pdfplumber
                texts = []
                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages[start:end]:
                        t = page.extract_text() or ""
                        texts.append(t)
                return texts
            except Exception:
                return []

        try:
            import pypdf
            reader_tmp = pypdf.PdfReader(pdf_path)
            total = len(reader_tmp.pages)
            # Priority scan: find the FORMAL BRSR section by trying specific keywords
            # in priority order so we skip highlights/summaries that come earlier.
            brsr_start_page = None
            for priority_kw in brsr_keywords_priority:
                for i in range(20, total):
                    try:
                        t = reader_tmp.pages[i].extract_text() or ""
                        if priority_kw in t.lower():
                            brsr_start_page = i
                            break
                    except Exception:
                        continue
                if brsr_start_page is not None:
                    break
        except Exception:
            return ""

        # Determine extraction range
        if brsr_start_page is not None:
            extract_start = brsr_start_page
            extract_end   = min(total, brsr_start_page + MAX_EXTRACT)
        else:
            # Fallback: last TAIL_PAGES
            extract_start = max(0, total - TAIL_PAGES)
            extract_end   = total

        # Extract text in the found range
        texts = _extract_with_pypdf(pdf_path, extract_start, extract_end)

        # If pypdf gets almost nothing (scanned PDF), fall back to pdfplumber
        if sum(len(t) for t in texts) < 1000:
            texts = _extract_with_pdfplumber(pdf_path, extract_start, extract_end)

        parts = [t for t in texts if t.strip()]
        return "\n".join(parts)

    # ── Step 4: Parse metrics ─────────────────────────────────────────────────

    def _parse_metrics(self, text: str) -> Dict[str, str]:
        """Run all regex patterns over the PDF text and return found values.
        Handles:
          - Numeric captures  → stored as "{number} {unit}"
          - Text captures     → stored as-is (certifications, CIN, years)
          - Zero-capture      → unit label stored as the value (pure presence check)
        """
        metrics: Dict[str, str] = {}
        for key, pattern, unit in _PATTERNS:
            if key in metrics:
                continue
            m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if m:
                groups = m.groups()
                if groups:
                    raw = groups[0].replace(",", "").strip()
                    try:
                        float(raw)      # numeric value
                        metrics[key] = f"{raw} {unit}".strip() if unit else raw
                    except ValueError:
                        # Text value (certification name, CIN, year, etc.)
                        if raw and len(raw) > 1:
                            metrics[key] = f"{raw} {unit}".strip() if unit else raw
                else:
                    # Zero-capture: presence-only detection
                    metrics[key] = unit if unit else "Yes"
        return metrics

    # ── Parse from local PDF ──────────────────────────────────────────────────

    def parse_local_pdf(self, pdf_path: str) -> Dict[str, str]:
        """Parse a user-provided local BRSR PDF file."""
        console.print(f"  [dim]→ Extracting profile text (first 30 pages)...[/dim]", end="  ")
        profile_text = self._extract_profile_text(pdf_path)
        console.print(f"[green]{len(profile_text):,} chars[/green]")

        console.print(f"  [dim]→ Extracting BRSR/governance text (last 120 pages)...[/dim]", end="  ")
        brsr_text = self._extract_brsr_text(pdf_path)
        if not brsr_text.strip():
            console.print("[yellow]no text extracted (may be a scanned/image PDF)[/yellow]")
            return {}
        console.print(f"[green]{len(brsr_text):,} chars[/green]")

        combined = brsr_text + "\n" + profile_text  # BRSR first for priority
        console.print("  [dim]→ Parsing ESG metrics...[/dim]", end="  ")
        metrics = self._parse_metrics(combined)
        console.print(f"[green]{len(metrics)} fields extracted[/green]")
        return metrics

    # ── Public entry point ────────────────────────────────────────────────────

    def scrape(self) -> Dict[str, str]:
        """
        Full BRSR pipeline via NSE India annual reports API.
        Returns dict of brsr_* keys → extracted value strings, or empty dict.
        """
        console.print()
        console.print("  [bold cyan]📄 BRSR PDF Scraper[/bold cyan]")

        if not self.nse_symbol:
            console.print("  [yellow]No NSE symbol — skipping BRSR[/yellow]")
            return {}

        # 1. Find annual report PDF URL from NSE
        console.print("  [dim]→ Looking up annual report on NSE India...[/dim]", end="  ")
        pdf_url = self._find_annual_report_url()
        if not pdf_url:
            console.print("[yellow]not found[/yellow]")
            return {}
        console.print(f"[green]found[/green]")
        console.print(f"  [dim]  URL: {pdf_url[:75]}...[/dim]")

        # 2. Download PDF
        console.print("  [dim]→ Downloading annual report PDF...[/dim]", end="  ")
        pdf_path = self._download_pdf(pdf_url)
        if not pdf_path:
            console.print(
                "[yellow]download failed (network restriction / large file).[/yellow]\n"
                f"  [dim]  Tip: Download the PDF manually and use[/dim]\n"
                f"  [dim]  --brsr-pdf <path> to parse it.[/dim]\n"
                f"  [dim]  PDF URL: {pdf_url}[/dim]"
            )
            return {}
        console.print("[green]✓[/green]")

        try:
            # 3a. Extract company profile from first 30 pages
            console.print("  [dim]→ Extracting company profile (first 30 pages)...[/dim]", end="  ")
            profile_text = self._extract_profile_text(pdf_path)
            console.print(f"[green]{len(profile_text):,} chars[/green]")

            # 3b. Extract BRSR + governance from last 120 pages
            console.print("  [dim]→ Finding BRSR/governance section...[/dim]", end="  ")
            brsr_text = self._extract_brsr_text(pdf_path)
            if not brsr_text.strip():
                console.print("[yellow]no text extracted (may be a scanned/image PDF)[/yellow]")
                return {}
            console.print(f"[green]{len(brsr_text):,} chars[/green]")

            # Combine – BRSR section first so formal table data takes priority
            # over summary highlights in the profile section
            combined_text = brsr_text + "\n" + profile_text

            # Clean up PDF ligature artifacts (e.g. HCL: 'Pa/r_t.ligaiculate' → 'Particulate')
            import re as _re
            combined_text = _re.sub(r'/([a-z])_([a-z])\.liga', r'\1\2', combined_text)

            # 4. Parse metrics
            console.print("  [dim]→ Parsing ESG metrics...[/dim]", end="  ")
            metrics = self._parse_metrics(combined_text)
            console.print(f"[green]{len(metrics)} fields extracted[/green]")

            if metrics:
                from rich.table import Table
                from rich import box
                tbl = Table("Field", "Value", box=box.SIMPLE, show_header=True)
                for k, v in metrics.items():
                    tbl.add_row(k.replace("brsr_", ""), v)
                console.print(tbl)

            return metrics

        finally:
            try:
                os.unlink(pdf_path)
            except Exception:
                pass
