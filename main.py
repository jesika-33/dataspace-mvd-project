import os
import csv
import io
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CSV Proxy Join Service", version="1.0")

# Configuration from environment variables
PROXY_TABLE_URL = os.getenv("PROXY_TABLE_URL", "https://pub-d6fe39b08661488e81a2159f5c153c29.r2.dev/drugsComTrain_raw.csv")

# Helper: fetch proxy table as dict (mapping)
async def fetch_proxy_table(url: str) -> Dict[str, str]:
    """Fetch CSV proxy table and return a dictionary mapping first column to second column."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        resp.raise_for_status()
        content = resp.text
        # Parse CSV
        reader = csv.DictReader(io.StringIO(content))
        mapping = {}
        # Assume columns: "part_id", "report_id" or generic; we'll use first two columns
        for row in reader:
            keys = list(row.keys())
            if len(keys) >= 2:
                mapping[row[keys[0]]] = row[keys[1]]
        return mapping

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/analyze")
async def analyze_relationship(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...)
):
    """
    Join two CSV datasets using proxy table:
    - hospital file uses column 'drugName'
    - pharmacy file uses column 'CONTAINS'
    """

    try:
        # 1. Validate
        if not (file1.filename.endswith('.csv') and file2.filename.endswith('.csv')):
            raise HTTPException(status_code=400, detail="Both files must be CSV.")

        # 2. Fetch proxy mapping
        mapping = await fetch_proxy_table(PROXY_TABLE_URL)
        if not mapping:
            raise HTTPException(status_code=500, detail="Proxy table is empty.")

        # 3. Read CSVs as DictReader (important change)
        file1_content = (await file1.read()).decode("utf-8", errors="ignore")
        file2_content = (await file2.read()).decode("utf-8", errors="ignore")

        reader1 = list(csv.DictReader(io.StringIO(file1_content)))
        reader2 = list(csv.DictReader(io.StringIO(file2_content)))

        if not reader1 or not reader2:
            raise HTTPException(status_code=400, detail="CSV files must have data rows.")

        # 4. Validate required columns
        if "drugName" not in reader1[0]:
            raise HTTPException(status_code=400, detail="file1 must contain column 'drugName'")
        if "CONTAINS" not in reader2[0]:
            raise HTTPException(status_code=400, detail="file2 must contain column 'CONTAINS'")

        # 5. Build index for pharmacy file (file2) using CONTAINS
        file2_index = {}
        for row in reader2:
            key = row.get("CONTAINS")
            if key:
                file2_index.setdefault(key, []).append(row)

        # 6. Join logic
        matches = []

        for row1 in reader1:
            drug_name = row1.get("drugName")
            if not drug_name:
                continue
    
            for proxy_key, proxy_value in mapping.items():
                if proxy_key in drug_name:  # substring match
    
                    for key2, rows in file2_index.items():
                        if proxy_value in key2:  # substring match
    
                            for row2 in rows:
                                matches.append({
                                    "hospital": row1,
                                    "pharmacy": row2,
                                    "matched_on": f"{proxy_key} -> {proxy_value}"
                                })
                            
        # 7. Return result
        return JSONResponse(content={
            "status": "success",
            "matches_found": len(matches),
            "matches": matches[:50],  # limit output
            "note": "Showing up to 50 matches"
        })

    except Exception as e:
        logger.exception("Error during join")
        raise HTTPException(status_code=500, detail=str(e))
