import os, io, time, json, logging, re, uuid
from typing import List, Tuple, Optional

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceExistsError

# ----------------------------- Logging -----------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fiquebot")

# ----------------------------- App init -----------------------------
app = FastAPI(title="Fiquebot API", version="1.0.0")

# ----------------------------- CORS -----------------------------
# Allow local dev and a placeholder for Azure Static Web Apps.
# You can override with env var FRONTEND_ORIGINS="https://a.b.net,https://c.d.net"
_default_origins = [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://<YOUR-STATIC-WEB-APP>.azurestaticapps.net",  # placeholder
]
_env_origins = os.environ.get("FRONTEND_ORIGINS") or os.environ.get("ALLOWED_ORIGINS")
if _env_origins:
    allow_origins = [o.strip() for o in _env_origins.split(",") if o.strip()]
else:
    allow_origins = _default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------- Serve /ui (single-file frontend) -----------------------------
if os.path.isdir("web"):
    app.mount("/ui", StaticFiles(directory="web", html=True), name="ui")

    @app.get("/", include_in_schema=False)
    async def _root():
        # trailing slash ensures index.html is served
        return RedirectResponse(url="/ui/")
else:
    @app.get("/", include_in_schema=False)
    async def _root_missing():
        return JSONResponse(
            {"message": "Frontend not found. Create web/index.html or deploy Azure Static Web App."},
            status_code=200,
        )

# ----------------------------- Config -----------------------------
TRN_EP = os.environ.get("AZURE_TRANSLATOR_ENDPOINT", "https://api.cognitive.microsofttranslator.com").rstrip("/")
TRN_KEY = os.environ.get("AZURE_TRANSLATOR_KEY", "")
TRN_REGION = os.environ.get("AZURE_TRANSLATOR_REGION", "westeurope")

AOAI_EP = (os.environ.get("AZURE_OPENAI_ENDPOINT", "") or "").rstrip("/")
AOAI_DEP = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")
AOAI_VER = os.environ.get("AZURE_OPENAI_API_VERSION", "2023-07-01-preview")
AOAI_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")

STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT_NAME", "fiqueuploadstore")

def _require(cond: bool, msg: str):
    if not cond:
        raise HTTPException(500, msg)

# ----------------------------- Blob helpers -----------------------------
def get_blob_client() -> BlobServiceClient:
    _require(STORAGE_ACCOUNT, "Storage account not configured")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if connection_string:
        log.info("Using connection string for Blob Service Client")
        return BlobServiceClient.from_connection_string(connection_string)
    else:
        log.info("Using DefaultAzureCredential for Blob Service Client")
        credential = DefaultAzureCredential()
        return BlobServiceClient(f"https://{STORAGE_ACCOUNT}.blob.core.windows.net", credential=credential)

def ensure_container(blob_service: BlobServiceClient, name: str):
    try:
        blob_service.create_container(name)
        log.info({"op": "container-created", "name": name})
    except ResourceExistsError:
        pass  # already exists

# ----------------------------- Core helpers -----------------------------
def translate_texts(texts: List[str], to_lang: str = "en") -> List[str]:
    _require(TRN_EP and TRN_KEY and TRN_REGION, "Translator not configured")
    log.info({"op": "translate-input", "count": len(texts)})
    url = f"{TRN_EP}/translate?api-version=3.0&to={to_lang}"
    headers = {
        "Ocp-Apim-Subscription-Key": TRN_KEY,
        "Ocp-Apim-Subscription-Region": TRN_REGION,
        "Content-Type": "application/json",
    }
    out: List[str] = []
    for i in range(0, len(texts), 50):  # batch
        batch = texts[i : i + 50]
        payload = [{"Text": t or ""} for t in batch]
        with httpx.Client(timeout=60) as h:
            r = h.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
        out.extend([item["translations"][0]["text"] for item in data])
    return out

def extract_entities(text: str) -> dict:
    _require(AOAI_EP and AOAI_KEY and AOAI_DEP, "Azure OpenAI not configured")
    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
    prompt = f"""
    Extract entities from the following text and return them as JSON with fields: country, phone, book, language_mentioned, address.
    Book must be either "Gyan Ganga", "Way of Living", or empty string "".
    Use empty string "" for any field not found.
    Text: {text}
    Return format: {{"country":"", "phone":"", "book":"", "language_mentioned":"", "address":""}}
    """
    body = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 200, "temperature": 0.3}
    with httpx.Client(timeout=60) as h:
        r = h.post(url, headers=headers, json=body)
        r.raise_for_status()
        j = r.json()
    try:
        return json.loads(j["choices"][0]["message"]["content"])
    except Exception:
        log.error({"op": "parse-failed", "text": text[:120]})
        return {"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""}

def _read_dataframe_from_bytes(name: str, content: bytes) -> pd.DataFrame:
    try:
        if name.lower().endswith(".csv"):
            return pd.read_csv(io.BytesIO(content), encoding="utf-8")
        return pd.read_excel(io.BytesIO(content), engine="openpyxl")
    except Exception as e:
        raise HTTPException(400, f"Failed to read '{name}': {str(e)}") from e

def process_excel_blob(blob_name: str, text_column: Optional[str] = None, req_id: Optional[str] = None) -> Tuple[bytes, str]:
    """
    Reads from incoming/<file>, writes enriched CSV to processed/<file>_enriched.csv,
    returns (csv_bytes, 'processed/<name>_enriched.csv').
    """
    log.info({"op": "process-excel-start", "blob": blob_name, "req_id": req_id})
    blob_service = get_blob_client()
    ensure_container(blob_service, "incoming")
    ensure_container(blob_service, "processed")

    in_client = blob_service.get_blob_client(container="incoming", blob=blob_name.replace("incoming/", ""))
    log.info({"op": "blob-download-start", "blob": blob_name, "req_id": req_id})
    try:
        content = in_client.download_blob().readall()
        log.info({"op": "blob-download-complete", "blob": blob_name, "bytes": len(content), "req_id": req_id})
    except Exception as e:
        log.exception({"op": "blob-download-failed", "blob": blob_name, "error": str(e), "req_id": req_id})
        raise HTTPException(400, f"Failed to download '{blob_name}': {str(e)}")

    df = _read_dataframe_from_bytes(blob_name, content)

    # Choose text column
    if text_column and text_column in df.columns:
        col = text_column
    else:
        candidates = [c for c in df.columns if c.lower() in {"text", "message", "content", "description"}]
        col = candidates[0] if candidates else df.columns[0]

    # Robust mask (non-null & non-empty after strip)
    series = df[col]
    mask = series.notna() & (series.astype(str).str.strip() != "")
    valid_indices = series[mask].index.tolist()
    valid_texts = series[mask].astype(str).tolist()
    log.info({"op": "filtered-texts", "count": len(valid_texts), "column": col, "req_id": req_id})

    if valid_texts:
        translated = translate_texts(valid_texts, to_lang="en")
        rows, valid_translations, valid_row_indices = [], [], []
        for i, t in enumerate(translated):
            try:
                entities = extract_entities(t)
                if any(entities.get(k, "") for k in ("country", "phone", "book", "language_mentioned", "address")):
                    rows.append(entities)
                    valid_translations.append(t)
                    valid_row_indices.append(valid_indices[i])
            except Exception:
                log.exception({"op": "extract-failed", "text": t[:120], "req_id": req_id})
        # pad to length
        full_rows = [{"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""} for _ in range(len(df))]
        for i, idx in enumerate(valid_row_indices):
            if i < len(rows):
                full_rows[idx] = rows[i]
        translated_full = ["" for _ in range(len(df))]
        for i, idx in enumerate(valid_row_indices):
            translated_full[idx] = valid_translations[i] if i < len(valid_translations) else ""
    else:
        translated_full = ["" for _ in range(len(df))]
        full_rows = [{"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""} for _ in range(len(df))]

    # Build output DataFrame
    edf = df.copy()
    edf["translated_en"] = translated_full
    ents_df = pd.DataFrame(full_rows, index=df.index)
    out_df = pd.concat([edf, ents_df], axis=1)

    # Serialize CSV
    out = io.StringIO()
    out_df.to_csv(out, index=False, encoding="utf-8")
    out_bytes = out.getvalue().encode("utf-8")

    # Upload to processed/
    corrected = blob_name.replace("incoming/", "")
    processed_filename = corrected.rsplit(".", 1)[0] + "_enriched.csv"
    out_client = blob_service.get_blob_client(container="processed", blob=processed_filename)
    out_client.upload_blob(
        out_bytes,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv"),
    )
    processed_blob_full = f"processed/{processed_filename}"
    log.info({"op": "blob-upload", "blob": processed_blob_full, "bytes": len(out_bytes), "req_id": req_id})

    return out_bytes, processed_blob_full

# ----------------------------- Endpoints -----------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/debug-env")
async def debug_env():
    return {
        "AZURE_TRANSLATOR_ENDPOINT": os.environ.get("AZURE_TRANSLATOR_ENDPOINT"),
        "AZURE_TRANSLATOR_KEY": "REDACTED" if os.environ.get("AZURE_TRANSLATOR_KEY") else "MISSING",
        "AZURE_TRANSLATOR_REGION": os.environ.get("AZURE_TRANSLATOR_REGION"),
        "AZURE_OPENAI_ENDPOINT": os.environ.get("AZURE_OPENAI_ENDPOINT"),
        "AZURE_OPENAI_API_KEY": "REDACTED" if os.environ.get("AZURE_OPENAI_API_KEY") else "MISSING",
        "STORAGE_ACCOUNT_NAME": os.environ.get("STORAGE_ACCOUNT_NAME"),
        "AZURE_STORAGE_CONNECTION_STRING": "REDACTED" if os.environ.get("AZURE_STORAGE_CONNECTION_STRING") else "MISSING",
        "CORS_ALLOW_ORIGINS": allow_origins,
    }

@app.post("/translate")
async def translate_api(payload: dict):
    """
    payload: { 'texts': ['...', '...'], 'to': 'en' }
    """
    req_id = uuid.uuid4().hex[:8]
    texts = payload.get("texts") or []
    to = payload.get("to", "en")
    if not isinstance(texts, list) or len(texts) == 0:
        raise HTTPException(400, "Provide texts: []")
    t0 = time.time()
    out = translate_texts([str(x) for x in texts], to_lang=to)
    log.info({"op": "translate", "n": len(texts), "ms": int((time.time() - t0) * 1000), "req_id": req_id})
    return {"translations": out}

@app.post("/process-xlsx")
async def process_xlsx(blob_name: str, text_column: Optional[str] = None):
    """
    Process a file that ALREADY exists in 'incoming/' and return the enriched CSV.
    - blob_name must start with 'incoming/' and end with .xlsx/.xlsm/.xls/.csv
    """
    req_id = uuid.uuid4().hex[:8]
    if not blob_name.lower().endswith((".xlsx", ".xlsm", ".xls", ".csv")) or not blob_name.startswith("incoming/"):
        raise HTTPException(400, "Provide valid blob_name (e.g., 'incoming/sample.xlsx' or 'incoming/sample.csv')")
    try:
        t0 = time.time()
        content, processed_blob_name = process_excel_blob(blob_name, text_column, req_id=req_id)
        log.info(
            {
                "op": "process-xlsx",
                "blob": blob_name,
                "processed": processed_blob_name,
                "ms": int((time.time() - t0) * 1000),
                "req_id": req_id,
            }
        )
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{processed_blob_name.rsplit("/", 1)[-1]}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.exception({"op": "process-failed", "blob": blob_name, "error": str(e), "req_id": req_id})
        raise HTTPException(500, f"Failed to process '{blob_name}'")

@app.post("/process-upload")
async def process_upload(file: UploadFile = File(...), text_column: Optional[str] = Form(None)):
    """
    1) Save uploaded file to Azure Blob: incoming/<timestamped_safe_name>
    2) Reuse process_excel_blob(...) so it reads from incoming/ and writes to processed/
    3) Return the processed CSV (also stored in processed/)
    """
    req_id = uuid.uuid4().hex[:8]
    try:
        blob_service = get_blob_client()
        ensure_container(blob_service, "incoming")
        ensure_container(blob_service, "processed")

        # sanitize filename
        original = file.filename or "upload.csv"
        base = os.path.basename(original)
        safe_base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
        ts = time.strftime("%Y%m%d-%H%M%S")
        incoming_name = f"{ts}_{safe_base}"

        # content type for raw upload
        ctype = "text/csv" if incoming_name.lower().endswith(".csv") else \
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        # upload to incoming/
        data = await file.read()
        in_client = blob_service.get_blob_client(container="incoming", blob=incoming_name)
        in_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type=ctype))
        log.info({"op": "upload-to-incoming", "blob": f"incoming/{incoming_name}", "bytes": len(data), "req_id": req_id})

        # process from incoming/ -> writes to processed/ and returns bytes
        content, processed_blob_name = process_excel_blob(f"incoming/{incoming_name}", text_column, req_id=req_id)
        fname = os.path.basename(processed_blob_name)

        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.exception({"op": "process-upload-failed", "error": str(e), "req_id": req_id})
        raise HTTPException(500, "Failed to process uploaded file")
