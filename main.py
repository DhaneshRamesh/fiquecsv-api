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
app = FastAPI(title="Fiquebot API", version="1.3.1")

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
AOAI_DEP = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")  # currently using 35-turbo
AOAI_VER = os.environ.get("AZURE_OPENAI_API_VERSION", "2023-07-01-preview")
AOAI_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")

STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT_NAME", "fiqueuploadstore")

# Translation provider selection
TRANSLATE_PROVIDER = os.environ.get("TRANSLATE_PROVIDER", "llm").lower()  # default to LLM primary
LLM_MAX_BATCH = int(os.environ.get("LLM_MAX_BATCH", "150"))
LLM_SYS = "Return ONLY JSON Lines, one object per line. No commentary."

# ----------------------------- Small utils -----------------------------
def _soft_require(cond: bool, msg: str) -> bool:
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
_DIAL_PREFIXES = sorted({v.replace("+", "").replace("-", "") for v in _COUNTRY_TO_DIAL.values()},
                        key=lambda x: (-len(x), x))

def _norm_country(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    return s

def country_to_dial(country: str, phone: str = "") -> str:
    if not country and phone:
        code = _infer_from_phone(phone)
        return f"+{code}" if code else ""
    s = _norm_country(country)
    if s in _NANP:
        return "+1"
    if s in _COUNTRY_TO_DIAL:
        return _COUNTRY_TO_DIAL[s]
    if any(k in s for k in ["usa", "united states"]):
        return "+1"
    if phone:
        code = _infer_from_phone(phone)
        return f"+{code}" if code else ""
    return ""

def _infer_from_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"[^\d]", "", phone)
    if phone.strip().startswith("00"):
        digits = digits[2:]
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
        pass
    except Exception as e:
        log.warning({"op": "container-create-skip", "name": name, "error": str(e)})

# ----------------------------- Translator (MS) -----------------------------
def _translate_and_detect_ms(texts: List[str], to_lang: str = "en") -> List[dict]:
    """
    Returns list of dicts: {'translated': str, 'lang': 'xx', 'confidence': float}
    Uses Microsoft Translator /translate, which includes detectedLanguage.
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
    for i in range(0, len(texts), 50):
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
            log.warning({"op": "translate-fallback-ms-batch", "error": str(e)})
            out.extend([{"translated": t or "", "lang": "en", "confidence": 0.0} for t in batch])
    return out

# ----------------------------- LLM translation (Azure OpenAI) -----------------------------
def _llm_translate_batch(texts: List[str], to_lang: str) -> List[dict]:
    """
    One LLM call for a batch. Returns [{'translated': str, 'lang': 'xx', 'confidence': float}, ...]
    Uses JSONL to keep parsing robust. Aligns strictly to input order.
    """
    if not (AOAI_EP and AOAI_KEY and AOAI_DEP):
        # No LLM configured -> empty signal so caller can fallback
        raise RuntimeError("LLM not configured")

    rows = [{"id": i, "text": (t or "")} for i, t in enumerate(texts)]
    lines = [f'- id="{r["id"]}" text="{r["text"].replace("\\", "\\\\").replace("\"","\\\"")}"' for r in rows]

    user_prompt = f"""
You are a professional translator and language detector.
Translate each row's 'text' into {to_lang}.
For each input row, output exactly one JSON object on its own line with keys: id (int), translated (str), lang (e.g., "en","hi","es"), confidence (0..1).
Do not add extra keys. Do not add commentary. Preserve numbers and punctuation.

Rows:
{chr(10).join(lines)}

Example JSONL (format only):
{{"id": 0, "translated": "…", "lang": "en", "confidence": 0.99}}
"""

    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
    body = {
        "messages": [
            {"role": "system", "content": LLM_SYS},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
        "max_tokens": 3000,
    }

    # backoff retries
    for attempt in range(5):
        try:
            with httpx.Client(timeout=120) as h:
                r = h.post(url, headers=headers, json=body)
                r.raise_for_status()
                j = r.json()
            content = (j["choices"][0]["message"]["content"] or "").strip()
            out_map = {}
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    out_map[int(obj.get("id"))] = {
                        "translated": str(obj.get("translated", "")),
                        "lang": (obj.get("lang") or "").lower(),
                        "confidence": float(obj.get("confidence", 0.0) or 0.0),
                    }
                except Exception:
                    continue
            aligned = []
            for i in range(len(texts)):
                aligned.append(out_map.get(i, {"translated": texts[i] or "", "lang": "en", "confidence": 0.0}))
            return aligned
        except Exception as e:
            wait = min(2 ** attempt, 20)
            log.warning({"op": "llm-translate-retry", "attempt": attempt + 1, "wait": wait, "error": str(e)})
            time.sleep(wait)

    # Bubble up to let caller fallback to MS
    raise RuntimeError("LLM translation failed after retries")

def translate_and_detect_llm(texts: List[str], to_lang: str = "en") -> List[dict]:
    out: List[dict] = []
    for i in range(0, len(texts), LLM_MAX_BATCH):
        batch = texts[i:i + LLM_MAX_BATCH]
        res = _llm_translate_batch(batch, to_lang)
        out.extend(res)
    return out

# ----------------------------- Unified translation (LLM primary, MS fallback) -----------------------------
def translate_and_detect(texts: List[str], to_lang: str = "en", provider: Optional[str] = None) -> List[dict]:
    """
    provider: "llm" | "ms" | None (falls back to env TRANSLATE_PROVIDER).
    Behavior:
      - "llm": try LLM; on failure, fallback to MS (if configured), else passthrough.
      - "ms": use Microsoft Translator (existing behavior); on failure, passthrough.
      - None: use TRANSLATE_PROVIDER env ("llm" default here).
    """
    prov = (provider or TRANSLATE_PROVIDER or "llm").lower()

    if prov == "ms":
        try:
            return _translate_and_detect_ms(texts, to_lang=to_lang)
        except Exception as e:
            log.warning({"op": "ms-translate-hardfail", "error": str(e)})
            return [{"translated": t or "", "lang": "en", "confidence": 0.0} for t in texts]

    # prov == "llm" (primary) with fallback to MS
    try:
        return translate_and_detect_llm(texts, to_lang=to_lang)
    except Exception as e:
        log.warning({"op": "llm-primary-fallback-ms", "error": str(e)})
        try:
            return _translate_and_detect_ms(texts, to_lang=to_lang)
        except Exception as e2:
            log.warning({"op": "ms-fallback-failed", "error": str(e2)})
            return [{"translated": t or "", "lang": "en", "confidence": 0.0} for t in texts]

def translate_texts(texts: List[str], to_lang: str = "en", provider: Optional[str] = None) -> List[str]:
    res = translate_and_detect(texts, to_lang=to_lang, provider=provider)
    return [r["translated"] for r in res]

# ----------------------------- Entity extraction (Azure OpenAI) -----------------------------
def extract_entities(text: str) -> dict:
    """Degrade gracefully to empty entities if OpenAI isn't configured or call fails."""
    if not (AOAI_EP and AOAI_KEY and AOAI_DEP):
        return {"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""}

    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
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
    for enc in ["utf-8-sig", "utf-8", "latin1"]:
        try:
            df = pd.read_csv(
                io.BytesIO(content),
                sep=None,
                engine="python",
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
    try:
        if ext in (".xlsx", ".xlsm"):
            return pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)
        if ext == ".xls":
            return pd.read_excel(io.BytesIO(content), engine="xlrd", dtype=str)
        return pd.read_excel(io.BytesIO(content), engine="openpyxl", dtype=str)
    except Exception as e:
        log.debug({"op": "excel-read-failed", "error": str(e)})
        return None

def _read_dataframe_from_bytes(name: str, content: bytes) -> pd.DataFrame:
    name_lower = (name or "").lower()
    ext = ".csv" if name_lower.endswith(".csv") else os.path.splitext(name_lower)[1]

    df: Optional[pd.DataFrame] = None
    if ext == ".csv":
        df = _try_read_csv(content)
        if df is None:
            df = _try_read_excel(content, ".xlsx")
    else:
        df = _try_read_excel(content, ext)
        if df is None:
            df = _try_read_csv(content)

    if df is None:
        log.warning({"op": "read-fallback-empty", "name": name})
        return pd.DataFrame(columns=["text"])

    df.columns = [str(c) if c is not None else "" for c in df.columns]
    try:
        # pandas 2.3: DataFrame.applymap deprecated in favor of DataFrame.map
        df = df.map(lambda x: x if isinstance(x, str) else ("" if pd.isna(x) else str(x)))
    except Exception:
        pass

    if len(df.columns) == 1:
        # Single column is our text already
        return df

    if len(df.columns) == 0:
        df["text"] = ""

    return df

# ----------------------------- Core processing -----------------------------
def _choose_text_column(df: pd.DataFrame, requested: Optional[str]) -> str:
    if requested and requested in df.columns:
        return requested
    candidates = [c for c in df.columns if str(c).strip().lower() in {"text", "message", "content", "description", "body"}]
    if candidates:
        return candidates[0]
    if len(df.columns) == 1:
        return df.columns[0]
    df["__synthetic_text__"] = df.astype(str).apply(lambda r: " ".join([v for v in r.values if v and v != "nan"]), axis=1)
    return "__synthetic_text__"

def process_excel_blob_from_bytes(
    name: str,
    content: bytes,
    text_column: Optional[str] = None,
    req_id: Optional[str] = None,
    provider: Optional[str] = None
) -> Tuple[bytes, str]:
    """
    Process a file already in-memory and return (csv_bytes, suggested_name).
    Now does safe batching to avoid worker timeouts / OOM.
    """
    df = _read_dataframe_from_bytes(name, content)
    col = _choose_text_column(df, text_column)

    series = df[col].astype(str)
    mask = series.notna() & (series.astype(str).str.strip() != "")
    valid_indices = series[mask].index.tolist()
    valid_texts = series[mask].astype(str).tolist()
    log.info({"op": "filtered-texts", "count": len(valid_texts), "column": col, "req_id": req_id})

    n = len(df)
    source_lang_full: List[str] = [""] * n
    trans_conf_full: List[float] = [0.0] * n
    was_translated_full: List[bool] = [False] * n
    needs_review_full: List[bool] = [False] * n
    translated_full = [""] * n
    full_rows = [{"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""} for _ in range(n)]

    if valid_texts:
        BATCH = int(os.environ.get("LLM_MAX_BATCH", "200"))  # smaller batches = safer; env-configurable
        for start in range(0, len(valid_texts), BATCH):
            end = min(start + BATCH, len(valid_texts))
            chunk = valid_texts[start:end]
            chunk_idx = valid_indices[start:end]
            log.info({"op": "llm-chunk", "start": start, "end": end, "size": len(chunk), "req_id": req_id})

            try:
                trans_results = translate_and_detect(chunk, to_lang="en", provider=provider)
            except Exception as e:
                log.warning({"op": "translate-chunk-failed", "error": str(e), "req_id": req_id})
                trans_results = [{"translated": t, "lang": "en", "confidence": 0.0} for t in chunk]

            for i, res in enumerate(trans_results):
                src_idx = chunk_idx[i]
                t_en = res.get("translated", "")
                lang = (res.get("lang", "") or "").lower()
                conf = float(res.get("confidence") or 0.0)

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
                    log.warning({"op": "extract-chunk-failed", "error": str(e), "req_id": req_id})
                    entities = {"country": "", "phone": "", "book": "", "language_mentioned": "", "address": ""}

                full_rows[src_idx] = {
                    "country": entities.get("country", ""),
                    "phone": entities.get("phone", ""),
                    "book": entities.get("book", ""),
                    "language_mentioned": entities.get("language_mentioned", ""),
                    "address": entities.get("address", ""),
                }

    # Final dataframe assembly
    edf = df.copy()
    edf["translated_en"] = translated_full
    edf["source_lang"] = source_lang_full
    edf["translation_confidence"] = trans_conf_full
    edf["was_translated"] = was_translated_full
    edf["translation_needs_review"] = needs_review_full

    ents_df = pd.DataFrame(full_rows, index=df.index)

    dialing_codes: List[str] = []
    for idx in ents_df.index:
        ctry = str(ents_df.at[idx, "country"] or "")
        ph = str(ents_df.at[idx, "phone"] or "")
        dialing_codes.append(country_to_dial(ctry, ph))
    ents_df["dialing_code"] = dialing_codes

    out_df = pd.concat([edf, ents_df], axis=1)

    out = io.StringIO()
    out_df.to_csv(out, index=False, encoding="utf-8")
    out_bytes = out.getvalue().encode("utf-8")

    base = os.path.basename(name or "upload.csv")
    processed_filename = base.rsplit(".", 1)[0] + "_enriched.csv"
    return out_bytes, processed_filename

def process_excel_blob(
    blob_name: str,
    text_column: Optional[str] = None,
    req_id: Optional[str] = None,
    provider: Optional[str] = None
) -> Tuple[bytes, str]:
    """
    Reads from incoming/<file>, writes enriched CSV to processed/<file>_enriched.csv if storage is available.
    """
    log.info({"op": "process-excel-start", "blob": blob_name, "req_id": req_id})

    blob_service = get_blob_client()
    if blob_service is None:
        log.warning({"op": "storage-missing-fallback", "blob": blob_name})
        return process_excel_blob_from_bytes(os.path.basename(blob_name), b"", text_column, req_id=req_id, provider=provider)

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
        content = b""

    out_bytes, processed_filename = process_excel_blob_from_bytes(blob_name, content, text_column, req_id=req_id, provider=provider)

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
        "AZURE_OPENAI_DEPLOYMENT": AOAI_DEP,
        "TRANSLATE_PROVIDER": TRANSLATE_PROVIDER,
        "LLM_MAX_BATCH": LLM_MAX_BATCH,
        "STORAGE_ACCOUNT_NAME": os.environ.get("STORAGE_ACCOUNT_NAME"),
        "AZURE_STORAGE_CONNECTION_STRING": "REDACTED" if os.environ.get("AZURE_STORAGE_CONNECTION_STRING") else "MISSING",
        "CORS_ALLOW_ORIGINS": allow_origins,
    }

@app.post("/translate")
async def translate_api(payload: dict):
    """
    payload: { 'texts': ['...', '...'], 'to': 'en', 'provider': 'llm'|'ms' }
    - LLM primary (default via env), MS fallback when LLM fails
    """
    req_id = uuid.uuid4().hex[:8]
    texts = payload.get("texts") or []
    to = payload.get("to", "en")
    provider = (payload.get("provider") or "").lower() or None
    if not isinstance(texts, list) or len(texts) == 0:
        raise HTTPException(400, "Provide texts: []")
    t0 = time.time()
    out = translate_texts([str(x) for x in texts], to_lang=to, provider=provider)
    log.info({"op": "translate", "n": len(texts), "ms": int((time.time() - t0) * 1000), "provider": provider or TRANSLATE_PROVIDER, "req_id": req_id})
    return {"translations": out}

@app.post("/process-xlsx")
async def process_xlsx(blob_name: str, text_column: Optional[str] = None, provider: Optional[str] = None):
    """
    Process a file that ALREADY exists in 'incoming/' and return the enriched CSV.
    Accepts optional provider override: 'llm' or 'ms'.
    """
    req_id = uuid.uuid4().hex[:8]
    if not blob_name.lower().endswith((".xlsx", ".xlsm", ".xls", ".csv")) or not blob_name.startswith("incoming/"):
        raise HTTPException(400, "Provide valid blob_name (e.g., 'incoming/sample.xlsx' or 'incoming/sample.csv')")
    try:
        t0 = time.time()
        content, processed_blob_name = process_excel_blob(blob_name, text_column, req_id=req_id, provider=provider)
        log.info(
            {
                "op": "process-xlsx",
                "blob": blob_name,
                "processed": processed_blob_name,
                "ms": int((time.time() - t0) * 1000),
                "provider": provider or TRANSLATE_PROVIDER,
                "req_id": req_id,
            }
        )
        fname = os.path.basename(processed_blob_name)
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.exception({"op": "process-failed-soft", "blob": blob_name, "error": str(e), "req_id": req_id})
        out = io.StringIO()
        pd.DataFrame([{"error": str(e)}]).to_csv(out, index=False)
        return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8")), media_type="text/csv")

@app.post("/process-upload")
async def process_upload(file: UploadFile = File(...), text_column: Optional[str] = Form(None), provider: Optional[str] = Form(None)):
    """
    1) If Azure Blob Storage is available, save uploaded file to 'incoming/' then process and also upload to 'processed/'.
    2) If storage is unavailable, process entirely in-memory.
    3) Always return the processed CSV (never 500 for data/format issues).
    Optional 'provider' form field can be 'llm' or 'ms'.
    """
    req_id = uuid.uuid4().hex[:8]
    try:
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

                ctype = "text/csv" if incoming_name.lower().endswith(".csv") else \
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

                in_client = blob_service.get_blob_client(container="incoming", blob=incoming_name)
                in_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type=ctype))
                log.info({"op": "upload-to-incoming", "blob": f"incoming/{incoming_name}", "bytes": len(data), "req_id": req_id})

                content, processed_blob_name = process_excel_blob(f"incoming/{incoming_name}", text_column, req_id=req_id, provider=provider)
                fname = os.path.basename(processed_blob_name)

                return StreamingResponse(
                    io.BytesIO(content),
                    media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'},
                )
            except Exception as e:
                log.warning({"op": "blob-route-failed", "error": str(e), "req_id": req_id})
                # fall through to in-memory processing

        content, processed_name = process_excel_blob_from_bytes(incoming_name, data, text_column, req_id=req_id, provider=provider)
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
