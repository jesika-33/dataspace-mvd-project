import os
import csv
import io
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CSV Proxy Join Service", version="1.0")

# Configuration
PROXY_TABLE_URL = os.getenv(
    "PROXY_TABLE_URL",
    "https://pub-d6fe39b08661488e81a2159f5c153c29.r2.dev/drugsComTrain_raw.csv"
)

# ---------------------------
# NORMALIZATION (single source of truth)
# ---------------------------
def normalize(value: str) -> str:
    if value is None:
        return ""
    return value.strip().lower()


# ---------------------------
# STREAM CSV (memory-safe)
# ---------------------------
def stream_csv(file, delimiter):
    file.seek(0)

    reader = csv.DictReader(
        (line.decode("utf-8", errors="ignore") for line in file),
        delimiter=delimiter
    )

    # Clean headers
    if reader.fieldnames:
        reader.fieldnames = [
            f.strip().replace('\ufeff', '') for f in reader.fieldnames
        ]

    # Normalize each row
    for row in reader:
        yield {k: normalize(v) for k, v in row.items()}


# ---------------------------
# FETCH PROXY TABLE
# ---------------------------
async def fetch_proxy_table(url: str) -> Dict[str, str]:
    logger.info(f"Fetching proxy table from: {url}")
    mapping = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()

        content = resp.text

        reader = csv.DictReader(io.StringIO(content), delimiter=',')

        # Clean headers
        if reader.fieldnames:
            reader.fieldnames = [
                f.strip().replace('\ufeff', '') for f in reader.fieldnames
            ]

        for row in reader:
            keys = list(row.keys())
            if len(keys) >= 2:
                k = normalize(row[keys[0]])
                v = normalize(row[keys[1]])

                if not k or not v:
                    continue

                mapping[k] = v

    logger.info(f"Loaded {len(mapping)} proxy mappings")

    # Debug sample
    for i, (k, v) in enumerate(list(mapping.items())[:5]):
        logger.info(f"Mapping sample {i}: '{k}' -> '{v}'")

    return mapping


# ---------------------------
# HEALTH CHECK
# ---------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------
# MAIN ANALYZE ENDPOINT
# ---------------------------
@app.post("/analyze")
async def analyze_relationship(
    file1: UploadFile = File(...),
    file2: UploadFile = File(...)
):
    try:
        logger.info("==== NEW /analyze REQUEST ====")
        logger.info(f"Received files: {file1.filename}, {file2.filename}")

        if not (file1.filename.endswith('.csv') and file2.filename.endswith('.csv')):
            raise HTTPException(status_code=400, detail="Both files must be CSV.")

        # 1. Load proxy mapping
        mapping = await fetch_proxy_table(PROXY_TABLE_URL)

        # 2. Build index for file2 (pharmacy)
        logger.info("Indexing file2 (pharmacy)...")
        file2_index = {}

        reader2 = stream_csv(file2.file, delimiter=';')

        # Need fieldnames separately (DictReader quirk)
        file2.file.seek(0)
        raw_reader2 = csv.DictReader(
            (line.decode("utf-8", errors="ignore") for line in file2.file),
            delimiter=';'
        )
        headers2 = [
            f.strip().replace('\ufeff', '') for f in raw_reader2.fieldnames
        ]

        logger.info(f"File2 columns: {headers2}")

        if "NAME" not in headers2:
            raise HTTPException(status_code=400, detail="file2 must contain column 'NAME'")

        for i, row in enumerate(reader2):
            key = row.get("NAME")
            if key:
                file2_index.setdefault(key, []).append(row)

            if i > 0 and i % 20000 == 0:
                logger.info(f"Indexed {i} rows from file2")

        logger.info(f"Finished indexing file2: {len(file2_index)} unique keys")
        logger.info(f"Sample file2 keys: {list(file2_index.keys())[:20]}")

        # 3. Process file1
        logger.info("Processing file1 (hospital)...")
        matches = []

        reader1 = stream_csv(file1.file, delimiter=',')

        # Same trick to read headers
        file1.file.seek(0)
        raw_reader1 = csv.DictReader(
            (line.decode("utf-8", errors="ignore") for line in file1.file),
            delimiter=','
        )
        headers1 = [
            f.strip().replace('\ufeff', '') for f in raw_reader1.fieldnames
        ]

        logger.info(f"File1 columns: {headers1}")

        if "drugName" not in headers1:
            raise HTTPException(status_code=400, detail="file1 must contain column 'drugName'")

        sample_drugs = set()

        for i, row1 in enumerate(reader1):
            drug_name = row1.get("drugName")
            if not drug_name:
                continue

            if len(sample_drugs) < 20:
                sample_drugs.add(drug_name)

            for proxy_key, proxy_value in mapping.items():
                if proxy_key in drug_name:
                    for key2, rows in file2_index.items():
                        if proxy_value in key2:
                            for row2 in rows:
                                matches.append({
                                    "hospital": row1,
                                    "pharmacy": row2,
                                    "matched_on": f"{proxy_key} -> {proxy_value} (partial)"
                                })

            if i > 0 and i % 20000 == 0:
                logger.info(f"Processed {i} rows, matches: {len(matches)}")

            if len(matches) >= 50:
                break

        logger.info(f"Sample drugNames: {list(sample_drugs)}")
        logger.info(f"Final matches: {len(matches)}")

        # Log first matches clearly
        for i, match in enumerate(matches[:10]):
            logger.info(
                f"Match {i}: {match['hospital'].get('drugName')} "
                f"→ {match['pharmacy'].get('NAME')} "
                f"via {match['matched_on']}"
            )

        return JSONResponse(content={
            "status": "success",
            "matches_found": len(matches),
            "matches": matches[:50],
            "note": "Normalized + streaming join"
        })

    except Exception as e:
        logger.exception("Error during join")
        raise HTTPException(status_code=500, detail=str(e))
