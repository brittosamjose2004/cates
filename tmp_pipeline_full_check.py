import json
import sqlite3
import time
from urllib import request

BASE = "http://127.0.0.1:8000"
COMPANY_ID = "32"
YEAR = 2025


def post_json(url: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

jobs = post_json(
    f"{BASE}/api/pipeline/run",
    {
        "company_ids": [COMPANY_ID],
        "data_sources": ["Secondary"],
        "financial_years": [f"FY{YEAR}"],
    },
)
job_id = jobs[0]["id"]
print("job_id", job_id)
status = jobs[0]["status"]
print("status", status)

start = time.time()
while status in {"QUEUED", "FETCHING", "SCORING"} and (time.time() - start) < 480:
    time.sleep(8)
    b = post_json(f"{BASE}/api/pipeline/status/batch", {"job_ids": [job_id]})
    if b:
        status = b[0]["status"]
        print("status", status)

print("final_status", status)

conn = sqlite3.connect("data/impactree.db")
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM answers WHERE company_id=? AND year=?", (int(COMPANY_ID), YEAR))
answer_count = cur.fetchone()[0]
cur.execute("SELECT source, COUNT(*) FROM answers WHERE company_id=? AND year=? GROUP BY source ORDER BY COUNT(*) DESC", (int(COMPANY_ID), YEAR))
source_counts = cur.fetchall()
conn.close()

print("answer_count", answer_count)
print("source_counts", source_counts)
print("log_path", f"data/pipeline_logs/job_{job_id}.log")
