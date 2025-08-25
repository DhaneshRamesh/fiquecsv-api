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
app = FastAPI(title="Fiquebot API", version="1.2.0")

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

# ----------------------------- Small utils -----------------------------
def _soft_require(cond: bool, msg: str) -> bool:
    """Log and return False instead of raising. Lets us degrade gracefully."""
    if not cond:
        log.warning({"op": "soft-require-failed", "msg": msg})
        return False
    return True

# ----------------------------- Dialing codes (lightweight mapping + NANP group) -----------------------------
_NANP = {
    "united states", "united states of america", "usa", "canada",
    "bahamas", "barbados", "bermuda", "jamaica", "dominican republic", "haiti",
    "trinidad and tobago", "puerto rico", "grenada", "saint lucia",
    "antigua and barbuda", "saint kitts and nevis", "saint vincent and the grenadines",
    "anguilla", "cayman islands", "turks and caicos islands", "dominica",
    "british virgin islands", "us virgin islands", "guam", "northern mariana islands"
}
_COUNTRY_TO_DIAL = {
    # Core/likely dataset countries
    "india": "+91", "kenya": "+254", "nepal": "+977", "bangladesh": "+880", "pakistan": "+92",
    "sri lanka": "+94", "australia": "+61", "new zealand": "+64", "united kingdom": "+44", "uk": "+44",
    "england": "+44", "ireland": "+353", "south africa": "+27", "nigeria": "+234", "ghana": "+233",
    "tanzania": "+255", "uganda": "+256", "rwanda": "+250", "ethiopia": "+251", "zambia": "+260",
    "zimbabwe": "+263", "botswana": "+267", "namibia": "+264", "morocco": "+212", "tunisia": "+216",
    "algeria": "+213", "egypt": "+20", "saudi arabia": "+966", "united arab emirates": "+971", "uae": "+971",
    "qatar": "+974", "oman": "+968", "bahrain": "+973", "iran": "+98", "iraq": "+964", "turkey": "+90",
    "israel": "+972", "jordan": "+962", "lebanon": "+961",
    "china": "+86", "japan": "+81", "south korea": "+82", "korea, republic of": "+82", "north korea": "+850",
    "hong kong": "+852", "macau": "+853", "taiwan": "+886", "vietnam": "+84", "thailand": "+66",
    "laos": "+856", "cambodia": "+855", "malaysia": "+60", "singapore": "+65", "indonesia": "+62",
    "philippines": "+63", "myanmar": "+95", "brunei": "+673", "mongolia": "+976", "afghanistan": "+93",
    "france": "+33", "germany": "+49", "italy": "+39", "spain": "+34", "portugal": "+351",
    "netherlands": "+31", "belgium": "+32", "luxembourg": "+352", "switzerland": "+41", "austria": "+43",
    "poland": "+48", "czech republic": "+420", "czechia": "+420", "slovakia": "+421", "hungary": "+36",
    "romania": "+40", "bulgaria": "+359", "greece": "+30", "croatia": "+385", "slovenia": "+386",
    "serbia": "+381", "bosnia and herzegovina": "+387", "north macedonia": "+389", "albania": "+355",
    "iceland": "+354", "norway": "+47", "sweden": "+46", "finland": "+358", "denmark": "+45",
    "estonia": "+372", "latvia": "+371", "lithuania": "+370", "ukraine": "+380", "belarus": "+375",
    "moldova": "+373", "georgia": "+995", "armenia": "+374", "azerbaijan": "+994", "russia": "+7", "kazakhstan": "+7",
    "mexico": "+52", "guatemala": "+502", "belize": "+501", "honduras": "+504", "el salvador": "+503",
    "nicaragua": "+505", "costa rica": "+506", "panama": "+507", "colombia": "+57", "venezuela": "+58",
    "ecuador": "+593", "peru": "+51", "bolivia": "+591", "paraguay": "+595", "chile": "+56",
    "argentina": "+54", "uruguay": "+598", "brazil": "+55", "cuba": "+53", "dominican republic": "+1",
    "jamaica": "+1", "trinidad and tobago": "+1", "barbados": "+1", "bahamas": "+1",
    "canada": "+1", "united states": "+1", "united states of america": "+1", "usa": "+1",
}

# Prefixed list for phone-based inference (sorted by length desc to prefer longest match)
_DIAL_PREFIXES = sorted({v.replace("+", "").replace("-", "") for v in _COUNTRY_TO_DIAL.values()},
                        key=lambda x: (-len(x), x))

def _norm_country(name: str) -> str:
    s = (name or "").strip().lower()
    # small canonicalizations
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    return s

def country_to_dial(country: str, phone: str = "") -> str:
    """Return +<code> from country; fallback to inferring from phone if present."""
    if not country and phone:
        code = _infer_from_phone(phone)
        return f"+{code}" if code else ""
    s = _norm_country(country)
    if s in _NANP:
        return "+1"
    if s in _COUNTRY_TO_DIAL:
        return _COUNTRY_TO_DIAL[s]
    # loose NANP heuristics
    if any(k in s for k in ["usa", "united states"]):
        return "+1"
    # fallback: try phone
    if phone:
        code = _infer_from_phone(phone)
        return f"+{code}" if code else ""
    return ""

def _infer_from_phone(phone: str) -> str:
    """Pick the longest matching country calling prefix from a phone string like '+97150...' or '0091...'."""
    if not phone:
        return ""
    digits = re.sub(r"[^\d]", "", phone)
    if phone.strip().startswith("00"):
        digits = digits[2:]  # strip IDD 00
    # if it started with +, digits already stripped '+'
    for pref in _DIAL_PREFIXES:
        if digits.startswith(pref):
            return pref
    return ""

# ----------------------------- Blob helpers -----------------------------
def get_blob_client() -> Optional[BlobServiceClient]:
    if not _soft_require(STORAGE_ACCOUNT, "Storage account not configured"):
        return None
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    try:
        if connection_string:
            log.info("Using connection string for Blob Service Client")
            return BlobServiceClient.from_connection_string(connection_string)
        else:
            log.info("Using DefaultAzureCredential for Blob Service Client")
            credential = DefaultAzureCredential()
            return BlobServiceClient(f"https://{STORAGE_ACCOUNT}.blob.core.windows.net", credential=credential)
    except Exception as e:
        log.warning({"op": "blob-client-fallback", "error": str(e)})
        return None

def ensure_container(blob_service: BlobServiceClient, name: str):
    try:
        blob_service.create_container(name)
        log.info({"op": "container-created", "name": name})
    except ResourceExistsError:
        pass  # already exists
    except Exception as e:
        log.warning({"op": "container-create-skip", "name": name, "error": str(e)})

# ----------------------------- Translation helpers (with language detection) -----------------------------
def translate_and_detect(texts: List[str], to_lang: str = "en") -> List[dict]:
    """
    Returns list of dicts: {'translated': str, 'lang': 'xx', 'confidence': float}
    Uses Microsoft Translator /translate, which includes detectedLanguage.
    If not configured or call fails, degrades to passthrough with lang='en'.
    """
    if not (TRN_EP and TRN_KEY and TRN_REGION):
        return [{"translated": t or "", "lang": "en", "confidence": 1.0} for t in texts]

    url = f"{TRN_EP}/translate?api-version=3.0&to={to_lang}"
    headers = {
        "Ocp-Apim-Subscription-Key": TRN_KEY,
        "Ocp-Apim-Subscription-Region": TRN_REGION,
        "Content-Type": "application/json",
    }
    out: List[dict] = []
    for i in range(0, len(texts), 50):  # batch
        batch = texts[i : i + 50]
        payload = [{"Text": t or ""} for t in batch]
        try:
            with httpx.Client(timeout=60) as h:
                r = h.post(url, headers=headers, json=payload)
                r.raise_for_status()
                data = r.json()
            for item in data:
                t_en = item["translations"][0]["text"]
                det = item.get("detectedLanguage") or {}
                lang = det.get("language", "") or ""
                conf = float(det.get("score", det.get("confidence", 0)) or 0)
                out.append({"translated": t_en, "lang": lang, "confidence": conf})
        except Exception as e:
            log.warning({"op": "translate-fallback", "error": str(e)})
            out.extend([{"translated": t or "", "lang": "en", "confidence": 1.0} for t in batch])
    return out

def translate_texts(texts: List[str], to_lang: str = "en") -> List[str]:
    res = translate_and_detect(texts, to_lang=to_lang)
    return [r["translated"] for r in res]

# ----------------------------- Entity extraction -----------------------------
def extract_entities(text: str) -> dict:
    """Degrade gracefully to empty entities if OpenAI isn't configured or call fails."""
    if not (AOAI_EP and AOAI_KEY and AOAI_DEP):
        return {"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""}

    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
    # Clean text to avoid control chars confusing LLM
    cleaned = "".join(c for c in text if c.isprintable() or c.isspace())
    prompt = f"""
    Extract entities from the following text and return them as JSON with fields:
    country, phone, book, language_mentioned, address.
    Book must be either "Gyan Ganga", "Way of Living", or empty string "".
    Use empty string "" for any field not found.
    Text: {cleaned}
    Return format: {{"country":"", "phone":"", "book":"", "language_mentioned":"", "address":""}}
    """
    body = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 200, "temperature": 0.3}
    try:
        with httpx.Client(timeout=60) as h:
            r = h.post(url, headers=headers, json=body)
            r.raise_for_status()
            j = r.json()
        return json.loads(j["choices"][0]["message"]["content"])
    except Exception as e:
        log.warning({"op": "extract-fallback", "error": str(e)})
        return {"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""}

# ----------------------------- IO helpers -----------------------------

def _try_read_csv(content: bytes) -> Optional[pd.DataFrame]:
    """Robust CSV reader with multiple fallbacks."""
    for enc in ["utf-8-sig", "utf-8", "latin1"]:
        try:
            df = pd.read_csv(
                io.BytesIO(content),
                sep=None,  # let pandas sniff delimiter
                engine="python",  # sniff + tolerate bad lines
                dtype=str,
                encoding=enc,
                on_bad_lines="skip",
            )
            return df
        except Exception as e:
            last = str(e)
    log.debug({"op": "csv-read-failed", "last_error": last})
    return None


def _try_read_excel(content: bytes, ext: str) -> Optional[pd.DataFrame]:
    """Try Excel via openpyxl (xlsx/xlsm) or xlrd (xls) with fallbacks."""
    try:
        if ext in (".xlsx", ".xlsm"):
            return pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)
        if ext == ".xls":
            return pd.read_excel(io.BytesIO(content), engine="xlrd", dtype=str)
        # unknown excel-like -> try openpyxl
        return pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)
    except Exception as e:
        log.debug({"op": "excel-read-failed", "error": str(e)})
        return None


def _read_dataframe_from_bytes(name: str, content: bytes) -> pd.DataFrame:
    """Best-effort reader that never raises: returns at least an empty DF."""
    name_lower = (name or "").lower()
    ext = ".csv" if name_lower.endswith(".csv") else os.path.splitext(name_lower)[1]

    df: Optional[pd.DataFrame] = None
    # Prefer CSV if extension says so
    if ext == ".csv":
        df = _try_read_csv(content)
        if df is None:
            # maybe it's actually excel; try anyway
            df = _try_read_excel(content, ".xlsx")
    else:
        # Try Excel-style first
        df = _try_read_excel(content, ext)
        if df is None:
            # fall back to CSV sniff
            df = _try_read_csv(content)

    if df is None:
        log.warning({"op": "read-fallback-empty", "name": name})
        return pd.DataFrame(columns=["text"])  # minimal frame

    # Normalize columns to strings
    df.columns = [str(c) if c is not None else "" for c in df.columns]
    # Coerce all cells to strings where possible
    try:
        df = df.applymap(lambda x: x if isinstance(x, str) else ("" if pd.isna(x) else str(x)))
    except Exception:
        pass

    # If no columns, create one
    if len(df.columns) == 0:
        df["text"] = ""

    return df

# ----------------------------- Core processing -----------------------------

def _choose_text_column(df: pd.DataFrame, requested: Optional[str]) -> str:
    if requested and requested in df.columns:
        return requested
    # common text-like names
    candidates = [c for c in df.columns if str(c).strip().lower() in {"text", "message", "content", "description", "body"}]
    if candidates:
        return candidates[0]
    # else: if there's only one column, use it; otherwise synthesize a 'text' column joining string columns
    if len(df.columns) == 1:
        return df.columns[0]
    # Synthesize a text column
    df["__synthetic_text__"] = df.astype(str).apply(lambda r: " ".join([v for v in r.values if v and v != "nan"]), axis=1)
    return "__synthetic_text__"


def process_excel_blob_from_bytes(name: str, content: bytes, text_column: Optional[str] = None, req_id: Optional[str] = None) -> Tuple[bytes, str]:
    """
    Process a file already in-memory and return (csv_bytes, suggested_name).
    This is used both for uploads and as a fallback when blob storage is unavailable.
    """
    df = _read_dataframe_from_bytes(name, content)

    # Choose text column (with synthesis if needed)
    col = _choose_text_column(df, text_column)

    # Robust mask (non-null & non-empty after strip)
    series = df[col].astype(str)
    mask = series.notna() & (series.astype(str).str.strip() != "")
    valid_indices = series[mask].index.tolist()
    valid_texts = series[mask].astype(str).tolist()
    log.info({"op": "filtered-texts", "count": len(valid_texts), "column": col, "req_id": req_id})

    # Allocate full-size columns for language/translation QA
    n = len(df)
    source_lang_full: List[str] = [""] * n
    trans_conf_full: List[float] = [0.0] * n
    was_translated_full: List[bool] = [False] * n
    needs_review_full: List[bool] = [False] * n

    translated_full = [""] * n
    full_rows = [{"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""} for _ in range(n)]

    if valid_texts:
        # translate with detection (degrades to passthrough if not configured)
        trans_results = translate_and_detect(valid_texts, to_lang="en")
        valid_row_indices = []
        for i, res in enumerate(trans_results):
            t_en = res.get("translated", "")
            lang = (res.get("lang", "") or "").lower()
            conf = float(res.get("confidence") or 0.0)

            src_idx = valid_indices[i]
            source_lang_full[src_idx] = lang
            trans_conf_full[src_idx] = conf

            was_trans = bool(lang and lang != "en")
            needs_review = bool(was_trans and conf < 0.60)
            was_translated_full[src_idx] = was_trans
            needs_review_full[src_idx] = needs_review

            translated_full[src_idx] = t_en

            try:
                entities = extract_entities(t_en)
            except Exception as e:
                log.warning({"op": "extract-catch", "error": str(e), "req_id": req_id})
                entities = {"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""}
            # Place entity row regardless of emptiness (stable schema)
            full_rows[src_idx] = {
                "country": entities.get("country", ""),
                "phone": entities.get("phone", ""),
                "book": entities.get("book", ""),
                "language_mentioned": entities.get("language_mentioned", ""),
                "address": entities.get("address", ""),
            }
            valid_row_indices.append(src_idx)

    # Build output DataFrame
    edf = df.copy()
    # If we synthesized a text col, keep original columns and add translated_en; don't leak synthetic name
    edf["translated_en"] = translated_full
    edf["source_lang"] = source_lang_full
    edf["translation_confidence"] = trans_conf_full
    edf["was_translated"] = was_translated_full
    edf["translation_needs_review"] = needs_review_full

    ents_df = pd.DataFrame(full_rows, index=df.index)

    # Add dialing_code based on the extracted country (fallback from phone if possible)
    dialing_codes: List[str] = []
    for idx in ents_df.index:
        ctry = str(ents_df.at[idx, "country"] or "")
        ph = str(ents_df.at[idx, "phone"] or "")
        dialing_codes.append(country_to_dial(ctry, ph))
    ents_df["dialing_code"] = dialing_codes

    out_df = pd.concat([edf, ents_df], axis=1)

    # Serialize CSV
    out = io.StringIO()
    out_df.to_csv(out, index=False, encoding="utf-8")
    out_bytes = out.getvalue().encode("utf-8")

    # suggest processed file name
    base = os.path.basename(name or "upload.csv")
    processed_filename = base.rsplit(".", 1)[0] + "_enriched.csv"
    return out_bytes, processed_filename


def process_excel_blob(blob_name: str, text_column: Optional[str] = None, req_id: Optional[str] = None) -> Tuple[bytes, str]:
    """
    Reads from incoming/<file>, writes enriched CSV to processed/<file>_enriched.csv if storage is available.
    If storage is unavailable, processes entirely in-memory and returns bytes without attempting to upload.
    Always returns CSV bytes and a path-like name; never raises 500 for data issues.
    """
    log.info({"op": "process-excel-start", "blob": blob_name, "req_id": req_id})

    blob_service = get_blob_client()
    if blob_service is None:
        # Storage not available: try to interpret blob_name as local path (dev) or fail soft
        log.warning({"op": "storage-missing-fallback", "blob": blob_name})
        # Soft fail: return minimal CSV
        return process_excel_blob_from_bytes(os.path.basename(blob_name), b"", text_column, req_id)

    try:
        ensure_container(blob_service, "incoming")
        ensure_container(blob_service, "processed")
    except Exception:
        pass

    in_client = blob_service.get_blob_client(container="incoming", blob=blob_name.replace("incoming/", ""))
    log.info({"op": "blob-download-start", "blob": blob_name, "req_id": req_id})
    try:
        content = in_client.download_blob().readall()
        log.info({"op": "blob-download-complete", "blob": blob_name, "bytes": len(content), "req_id": req_id})
    except Exception as e:
        log.warning({"op": "blob-download-failed", "blob": blob_name, "error": str(e), "req_id": req_id})
        # Soft fallback: empty content
        content = b""

    out_bytes, processed_filename = process_excel_blob_from_bytes(blob_name, content, text_column, req_id=req_id)

    # Try to upload to processed/ if we can
    try:
        out_client = blob_service.get_blob_client(container="processed", blob=processed_filename)
        out_client.upload_blob(
            out_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="text/csv"),
        )
        processed_blob_full = f"processed/{processed_filename}"
        log.info({"op": "blob-upload", "blob": processed_blob_full, "bytes": len(out_bytes), "req_id": req_id})
    except Exception as e:
        log.warning({"op": "blob-upload-skip", "error": str(e), "req_id": req_id})

    return out_bytes, f"processed/{processed_filename}"

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
    - keeps the old response shape for compatibility
    - degrades to passthrough if translator not configured
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
    - never returns 500 for data/format issues; always returns a CSV (may be minimal) on soft failures
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
        # processed_blob_name may be 'processed/<name>' or just a name; normalize filename for download header
        fname = os.path.basename(processed_blob_name)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        # Last-resort soft fail: return a minimal CSV with an error column
        log.exception({"op": "process-failed-soft", "blob": blob_name, "error": str(e), "req_id": req_id})
        out = io.StringIO()
        pd.DataFrame([{"error": str(e)}]).to_csv(out, index=False)
        return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8")), media_type="text/csv")

@app.post("/process-upload")
async def process_upload(file: UploadFile = File(...), text_column: Optional[str] = Form(None)):
    """
    1) If Azure Blob Storage is available, save uploaded file to 'incoming/' then process and also upload to 'processed/'.
    2) If storage is unavailable, process entirely in-memory.
    3) Always return the processed CSV (never 500 for data/format issues).
    """
    req_id = uuid.uuid4().hex[:8]
    try:
        # sanitize filename
        original = file.filename or "upload.csv"
        base = os.path.basename(original)
        safe_base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
        ts = time.strftime("%Y%m%d-%H%M%S")
        incoming_name = f"{ts}_{safe_base}"

        data = await file.read()

        blob_service = get_blob_client()
        if blob_service is not None:
            try:
                ensure_container(blob_service, "incoming")
                ensure_container(blob_service, "processed")

                # content type for raw upload
                ctype = "text/csv" if incoming_name.lower().endswith(".csv") else \
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

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
            except Exception as e:
                log.warning({"op": "blob-route-failed", "error": str(e), "req_id": req_id})
                # fall through to in-memory processing

        # In-memory processing path
        content, processed_name = process_excel_blob_from_bytes(incoming_name, data, text_column, req_id=req_id)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{processed_name}"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        log.exception({"op": "process-upload-failed-soft", "error": str(e), "req_id": req_id})
        out = io.StringIO()
        pd.DataFrame([{"error": str(e)}]).to_csv(out, index=False)
        return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8")), media_type="text/csv")
