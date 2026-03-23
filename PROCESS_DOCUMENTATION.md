# Rubicr Caetis Process Documentation

## 1. Purpose

This document explains the full application flow end to end:

- how a company is searched and added
- how the pipeline is started
- how scraping works for Indian, global, and private companies
- how years are handled
- how data is stored in the database
- how the frontend shows the results
- which API endpoints are involved in each step

This is based on the current implementation in the repository.

## 2. High-Level Architecture

There are three main layers:

1. Frontend
   - React + TypeScript + Vite
   - Folder: `rubicr-caetis---super-admin/`

2. Backend
   - FastAPI
   - Folder: `backend/`

3. Database
   - SQLite
   - File: `data/impactree.db`

The frontend talks to the backend through `/api/*` routes.
The backend stores companies, pipeline jobs, sessions, answers, and scraped raw data in SQLite.

## 3. Main User Workflows

The application currently has three main workflows:

1. Add a company
2. Run the pipeline for one company
3. Run the pipeline for multiple companies

Each workflow eventually produces:

- one or more `PipelineJob` rows
- one or more `QuestionnaireSession` rows
- many `Answer` rows
- optional `ScrapedData` rows with raw source data

## 4. Workflow A: Add Company

### 4.1 Frontend entry point

The Add Company flow starts from the Master Universe screen.

Frontend component path:

- `rubicr-caetis---super-admin/components/MasterUniverse.tsx`
- `rubicr-caetis---super-admin/components/AddCompanyModal.tsx`

### 4.2 Add Company modal steps

The modal has these steps:

1. Search
2. Manual Entry (optional fallback)
3. Confirm Identity
4. Configuration
5. Trigger pipeline and poll job status

### 4.3 Company search process

When the user searches by company name:

1. Frontend calls:
   - `GET /api/search/companies?q=<name>&per_page=20`
2. Backend calls GLEIF APIs:
   - `fuzzycompletions`
   - `lei-records`
3. Backend merges and deduplicates results
4. Frontend displays:
   - legal name
   - LEI
   - address
   - jurisdiction
   - registration number
   - status
   - GLEIF link

### 4.4 Ticker auto-detection process

When a company is selected:

1. Frontend calls:
   - `GET /api/search/nse-symbol?q=<company name>`
2. Backend calls Yahoo Finance search API
3. Backend tries to resolve the best market symbol in this priority order:
   - NSE (`.NS`)
   - BSE (`.BO`)
   - any other global equity symbol
4. Frontend auto-fills the ticker input and shows the exchange if found

Examples:

- Tata Motors -> `TATAMOTORS` on `NSE`
- Apple -> `AAPL` on `NMS`
- Shell -> `SHEL` on `NYQ`

### 4.5 Company creation process

When the user confirms:

1. Frontend calls:
   - `POST /api/companies`
2. Backend creates a `companies` row with:
   - `name`
   - `ticker`
   - `cin` from LEI when available
   - `sector`
   - `headquarters` used as region storage

### 4.6 Automatic pipeline trigger after add

After the company is created, the frontend immediately calls:

- `POST /api/pipeline/run`

This means adding a company does not just save the company. It also starts ingestion immediately.

## 5. Workflow B: Run Pipeline For One Company

### 5.1 Frontend entry point

This flow starts from the Company Detail page.

Frontend component path:

- `rubicr-caetis---super-admin/components/CompanyDetail.tsx`
- `rubicr-caetis---super-admin/components/RunPipelineModal.tsx`

### 5.2 User inputs

The user selects:

- data source type
- one or more financial years

Then the frontend calls:

- `POST /api/pipeline/run`

with:

```json
{
  "company_ids": ["<company_id>"],
  "data_sources": ["Secondary"],
  "financial_years": ["FY2025", "FY2026"]
}
```

### 5.3 Backend processing

The backend does the following:

1. Resolves the selected company IDs
2. Parses `FY2025` style values into integer years
3. Creates one `PipelineJob` per company
4. Starts a background task `_run_pipeline_task(...)`

Pipeline statuses move through:

- `QUEUED`
- `FETCHING`
- `PUBLISHED`
- or `ERROR`

## 6. Workflow C: Run Pipeline For Multiple Companies

### 6.1 Frontend entry point

This flow starts from Master Universe.

Frontend component path:

- `rubicr-caetis---super-admin/components/GlobalRunPipelineModal.tsx`

### 6.2 User inputs

The user selects:

- data sources
- specific companies or all companies
- one or more years
- optional `allYears` toggle

### 6.3 Backend behavior

The same backend route is used:

- `POST /api/pipeline/run`

Behavior:

1. One job is created per company
2. Each job may cover multiple years
3. If `all_years` is true, backend expands the request to the latest 5 years ending at the highest selected year

Example:

```json
{
  "company_ids": ["9"],
  "financial_years": ["FY2026"],
  "all_years": true
}
```

becomes:

- `2022, 2023, 2024, 2025, 2026`

## 7. Backend Pipeline Execution

### 7.1 Main backend file

Pipeline route and background processing:

- `backend/api/routers/pipeline.py`

### 7.2 Core backend steps

For each job, `_run_pipeline_task(...)` does this:

1. Marks the job as `FETCHING`
2. Resolves the most suitable ticker symbol
3. Calls `run_all.py`
4. Checks whether questionnaire sessions were created for every requested year
5. If a year is missing, fills it directly with `QuestionnaireEngine`
6. Marks the job as `PUBLISHED`

If the process times out or crashes, the job becomes `ERROR`.

### 7.3 Ticker resolution behavior

The backend uses this logic:

1. If the company already has a short valid ticker, use it
2. If it is one of the built-in NSE companies, keep that built-in ticker
3. If the stored ticker looks like a fallback or placeholder, search Yahoo Finance again
4. Prefer:
   - NSE symbol
   - then BSE symbol
   - then any global equity ticker

## 8. `run_all.py` Execution Model

### 8.1 Purpose

`run_all.py` is the actual ingestion runner used by the backend.

It supports:

- interactive mode
- batch mode
- single year
- all years mode

### 8.2 Batch mode call from API

The backend calls it roughly like this:

```bash
python run_all.py \
  --batch \
  --companies "Company Name" \
  --year 2026 \
  --nse-symbol TATAMOTORS \
  --all-years \
  --num-years 5
```

### 8.3 Scrape steps inside `run_all.py`

For a given company, the flow is:

1. Try annual report PDF download when a ticker can be used against NSE/BSE flow
2. Verify the PDF when downloaded
3. Always run Yahoo Finance scrape for company profile and financials
4. If PDF is available, run BRSR PDF extraction
5. Run questionnaire auto-fill

Important behavior:

- PDF is optional
- Yahoo Finance path still runs even if PDF download fails
- private or unmatched companies still continue through questionnaire fallback

## 9. Scraping Strategy By Company Type

### 9.1 Indian listed company

Examples:

- Tata Motors
- HDFC Bank
- Infosys

Processing:

1. Ticker resolved to NSE/BSE symbol
2. Annual report PDF downloaded
3. BRSR data extracted from PDF
4. Yahoo Finance financials and ESG loaded
5. Questionnaire filled

Result quality:

- strongest coverage
- PDF-based + market-based + historical data

### 9.2 Global listed company

Examples:

- Apple
- Microsoft
- Shell

Processing:

1. Global ticker resolved through Yahoo Finance
2. NSE annual report PDF path usually not available
3. Yahoo Finance profile, financials, and ESG used
4. Questionnaire filled

Result quality:

- real market data
- no Indian BRSR PDF extraction unless such a PDF source exists separately

### 9.3 Private or unlisted company

Examples:

- internal entities
- private regional businesses
- entities with no supported market ticker

Processing:

1. No reliable exchange ticker
2. No annual report PDF route
3. No market financial source
4. Questionnaire fallback fills values using smart defaults

Result quality:

- basic coverage
- useful for UI continuity and workflow completeness
- not as strong as listed-company data

## 10. Questionnaire Fill Logic

### 10.1 Why fallback exists

Some companies do not produce raw scraped answers for every year.

Reasons include:

- no PDF found
- no market ticker
- no BRSR extraction result
- company name matched a different built-in company

To prevent empty company pages, the backend ensures each selected year has a questionnaire session.

### 10.2 Direct fill behavior

`_direct_questionnaire_fill(...)` does this:

1. Creates a `QuestionnaireEngine`
2. Calls `engine.setup()`
3. Pins the engine to the exact `company_id`
4. Runs `engine.run_auto(module_filter=None)`

This exact pinning is important because it avoids partial name matches writing answers to the wrong company.

## 11. Financial Year Handling

### 11.1 Input format

Frontend sends years like:

- `FY2023`
- `FY2024`
- `FY2025`

### 11.2 Backend conversion

Backend converts them into integer years:

- `FY2023` -> `2023`

### 11.3 Multi-year behavior

If multiple years are selected:

- the job stores the max year for display
- the background task loops over every selected year

### 11.4 `all_years` behavior

If the user enables `all_years`:

1. Backend finds the latest selected year
2. Expands to a 5-year range ending at that year

Example:

- selected: `FY2026`
- expanded years: `FY2022` to `FY2026`

## 12. Database Tables And Their Roles

### 12.1 `companies`

Stores one row per company.

Important fields:

- `id`
- `name`
- `ticker`
- `cin`
- `sector`
- `exchange`
- `headquarters`

### 12.2 `scraped_data`

Stores raw source key-value pairs per company, year, and source.

Examples of sources:

- `yahoo`
- `yahoo_historical`
- `yahoo_esg`
- `brsr_pdf`

### 12.3 `questionnaire_sessions`

Stores one session per:

- company
- year
- standard

This is what drives the year selector availability.

### 12.4 `answers`

Stores indicator answers.

Uniqueness is enforced by:

- company
- year
- indicator ID

This is the data used to show atomic indicators on the company detail page.

### 12.5 `pipeline_jobs`

Stores job execution status.

Important statuses:

- `QUEUED`
- `FETCHING`
- `SCORING`
- `NEEDS_REVIEW`
- `PUBLISHED`
- `ERROR`

The UI polls this table through the API.

## 13. How Results Appear In The UI

### 13.1 Company list page

Frontend calls:

- `GET /api/companies`

Backend returns summary data:

- company name
- ticker
- LEI/CIN
- region
- sector
- latest pipeline status
- latest year
- pillar scores

### 13.2 Company detail page

Frontend calls:

- `GET /api/companies/{id}`
- `GET /api/companies/{id}/years`

The detail API returns:

- risk pillars
- drivers
- indicators from `answers`
- evidence items
- status and version metadata

The years API returns distinct years from `questionnaire_sessions`.

If the year list is empty, the UI cannot show a year selector.

## 14. API Reference By Process

### 14.1 Search and add company

- `GET /api/search/companies`
- `GET /api/search/nse-symbol`
- `POST /api/companies`

### 14.2 Start pipeline

- `POST /api/pipeline/run`

### 14.3 Track jobs

- `GET /api/pipeline/status/{jobId}`
- `GET /api/pipeline/jobs`

### 14.4 View results

- `GET /api/companies`
- `GET /api/companies/{id}`
- `GET /api/companies/{id}/years`

## 15. End-to-End Example

### Example: Add Tata Motors and process FY2024 to FY2026

1. User opens Add Company
2. User searches `Tata Motors`
3. Frontend gets GLEIF entity list
4. User selects the correct entity
5. Frontend auto-detects ticker `TATAMOTORS`
6. User confirms and creates the company
7. Frontend posts to `/api/companies`
8. Frontend posts to `/api/pipeline/run`
9. Backend creates a job
10. Background task runs `run_all.py`
11. PDF and Yahoo data are processed
12. Questionnaire answers are saved for selected years
13. Job becomes `PUBLISHED`
14. Company detail page loads available years
15. Indicators appear for each available year

### Example: Add Apple

1. User searches `Apple`
2. Frontend auto-detects ticker `AAPL`
3. Pipeline runs
4. No Indian annual report PDF path is expected
5. Yahoo Finance data is used
6. Questionnaire fills answers
7. UI shows years and indicators

### Example: Add a private company with no market listing

1. User searches or manually enters company name
2. No usable ticker is found
3. Pipeline still runs
4. Questionnaire fallback creates answers
5. UI still shows a result set instead of an empty page

## 16. Known Behavior And Limits

1. GLEIF search covers legal entities, not every colloquial brand name
2. Exchange symbol auto-detection depends on Yahoo Finance search quality
3. NSE PDF extraction is strongest for Indian listed companies
4. Global companies generally rely on Yahoo profile, financial, and ESG data rather than BRSR PDF extraction
5. Private companies rely on smart-default answers when no external source is available
6. One pipeline job currently represents one company, even if multiple years are processed inside that job

## 17. Troubleshooting Guide

### Problem: Company added but no year selector appears

Check:

1. Was a `questionnaire_sessions` row created for that company ID?
2. Did `GET /api/companies/{id}/years` return anything?
3. Did the background task finish as `PUBLISHED`?

### Problem: Job says `PUBLISHED` but indicators are empty

Check:

1. Whether answers were saved under the exact selected company ID
2. Whether fallback direct fill ran for missing years
3. Whether the selected FY exists in the answers table

### Problem: No real data for a company

Interpretation:

1. If listed in India and PDF works, you should see richer data
2. If globally listed, expect Yahoo-based data
3. If private or unmatched, expect smart defaults

### Problem: Wrong ticker detected

Action:

1. Override the ticker in the Add Company confirmation screen
2. Re-run the pipeline

## 18. Run Commands

### Backend

```bash
cd /workspaces/new_sap
python -m uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
```

### Frontend

```bash
cd /workspaces/new_sap/rubicr-caetis---super-admin
npm run dev -- --host 0.0.0.0 --port 3000
```

### Health check

```bash
curl http://127.0.0.1:8000/api/health
```

## 19. Summary

The system is designed so the UI can always complete the user workflow:

- listed Indian companies -> PDF + market data + questionnaire
- global listed companies -> market data + questionnaire
- private companies -> questionnaire fallback

That means the application does not depend on only one scraping source. It uses layered fallbacks so the company detail page can still show years and indicators even when the best source is unavailable.