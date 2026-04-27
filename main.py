import os
import duckdb
import httpx
import tempfile
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="CSV Proxy Join Service (DuckDB)", version="3.0")

PROXY_TABLE_URL = os.getenv(
    "PROXY_TABLE_URL",
    "https://pub-d6fe39b08661488e81a2159f5c153c29.r2.dev/drugsComTrain_raw.csv"
)

HOSPITAL_URL = os.getenv(
    "ANONYMIZED_HOSPITAL_TABLE_URL",
    "https://pub-d6fe39b08661488e81a2159f5c153c29.r2.dev/data_spaces-anonymized.csv"
)

PHARMA_URL = os.getenv(
    "PHARMA_TABLE_URL",
    "https://pub-d6fe39b08661488e81a2159f5c153c29.r2.dev/2_MID.csv"
)


# ---------------------------
# DOWNLOAD FILE (STREAM SAFE)
# ---------------------------
async def download_file(url: str, max_retries: int = 3) -> str:
    filename = url.split("/")[-1]
    filepath = f"/tmp/{filename}"

    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading (attempt {attempt+1}): {url}")

            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url) as resp:
                    resp.raise_for_status()

                    with open(filepath, "wb") as f:
                        async for chunk in resp.aiter_bytes():
                            f.write(chunk)

            logger.info(f"Download complete: {filepath}")
            return filepath

        except Exception as e:
            logger.warning(f"Download failed (attempt {attempt+1}): {e}")

            if attempt == max_retries - 1:
                raise

    return filepath


# ---------------------------
# HEALTH
# ---------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------
# MAIN ANALYZE
# ---------------------------
@app.get("/analyze")
async def analyze(drugName: Optional[str] = None):
    try:
        logger.info(f"==== NEW ANALYSIS (DuckDB) with drugName={drugName} ====")

        # Download files
        hospital_path = await download_file(HOSPITAL_URL)
        pharma_path = await download_file(PHARMA_URL)
        proxy_path = await download_file(PROXY_TABLE_URL)

        con = duckdb.connect()

        # Base join query
        base_query = f"""
        SELECT
            h.*,
            ph.*
        FROM read_csv_auto('{hospital_path}') h
        JOIN read_csv_auto('{proxy_path}') p
            ON lower(h.drugName) LIKE '%' || lower(p."Hospital") || '%'
        JOIN read_csv_auto(
            '{pharma_path}',
            delim=',',
            quote='"',
            escape='"',
            header=True,
            ignore_errors=True,
            null_padding=True,
            sample_size=-1,
            parallel=false
        ) ph
            ON lower(ph.NAME) LIKE '%' || lower(p."Pharma_company (MID)") || '%'
        LIMIT 100
        """

        if drugName:
            query = base_query + " WHERE lower(h.drugName) LIKE '%' || ? || '%'"
            result = con.execute(query, [drugName.lower()]).fetchdf()
        else:
            result = con.execute(base_query).fetchdf()

        # Convert NaN/None to proper None for JSON serialization
        result = result.replace({np.nan: None})

        logger.info(f"Matches found: {len(result)}")
        return JSONResponse(content={
            "status": "success",
            "drugName_filter": drugName,
            "count": len(result),
            "rows": result.to_dict(orient="records")
        })

    except Exception as e:
        logger.exception("Error in DuckDB processing")
        raise HTTPException(status_code=500, detail=str(e))