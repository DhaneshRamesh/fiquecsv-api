import os, io, time, json, logging, re, uuid, csv, tempfile, gc
from typing import List, Tuple, Optional, Iterable, Dict, Any
from collections import deque

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import StreamingResponse, RedirectResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceExistsError

# Excel streaming
from openpyxl import load_workbook

# offload heavy sync steps to a thread so the event loop stays alive
import anyio

# ============================= In-memory log buffer =============================
LOG_CAPACITY = int(os.environ.get("LOG_BUFFER_CAPACITY", "1000"))
_LOG_DEQUE = deque(maxlen=LOG_CAPACITY)

class UiLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            try:
                msg = record.getMessage()
            except Exception:
                msg = str(record)
        _LOG_DEQUE.append(msg)

def _init_logging():
    # Base formatter with ISO-ish timestamps
    fmt = logging.Formatter(
        fmt="%(asctime)sZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    ui_handler = UiLogHandler()
    ui_handler.setLevel(logging.INFO)
    ui_handler.setFormatter(fmt)

    # Root logger
    root = logging.getLogger()
    # If uvicorn configured basic handlers, avoid duplicate lines
    if not any(isinstance(h, UiLogHandler) for h in root.handlers):
        root.addHandler(ui_handler)
    root.setLevel(logging.INFO)

    # Make sure our app logger exists too
    app_logger = logging.getLogger("fiquebot")
    app_logger.setLevel(logging.INFO)
    if not any(isinstance(h, UiLogHandler) for h in app_logger.handlers):
        app_logger.addHandler(ui_handler)

_init_logging()
log = logging.getLogger("fiquebot")
log.info("fiquebot backend starting with in-memory log buffer (capacity=%d)", LOG_CAPACITY)

# ============================= App init =============================
app = FastAPI(title="Fiquebot API", version="2.0.2")

# ============================= CORS =============================
_default_origins = [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "https://<YOUR-STATIC-WEB-APP>.azurestaticapps.net",
    # add your actual SWA hostname if different
]
_env_origins = os.environ.get("FRONTEND_ORIGINS") or os.environ.get("ALLOWED_ORIGINS")
allow_origins = [o.strip() for o in _env_origins.split(",") if o.strip()] if _env_origins else _default_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================= Serve /ui =============================
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

# ============================= Config =============================
TRN_EP = os.environ.get("AZURE_TRANSLATOR_ENDPOINT", "https://api.cognitive.microsofttranslator.com").rstrip("/")
TRN_KEY = os.environ.get("AZURE_TRANSLATOR_KEY", "")
TRN_REGION = os.environ.get("AZURE_TRANSLATOR_REGION", "westeurope")

AOAI_EP = (os.environ.get("AZURE_OPENAI_ENDPOINT", "") or "").rstrip("/")
AOAI_DEP = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")
AOAI_VER = os.environ.get("AZURE_OPENAI_API_VERSION", "2023-07-01-preview")
AOAI_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")

STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT_NAME", "fiqueuploadstore")

TRANSLATE_PROVIDER = os.environ.get("TRANSLATE_PROVIDER", "llm").lower()  # "llm" | "ms"
LLM_MAX_BATCH = int(os.environ.get("LLM_MAX_BATCH", "80"))  # slightly smaller to reduce peak memory
LLM_SYS = "Return ONLY JSON Lines, one object per line. No commentary."
ENTITY_CONF_THRESHOLD = float(os.environ.get("ENTITY_CONF_THRESHOLD", "0.60"))
CSV_CHUNK = int(os.environ.get("CSV_CHUNK", "10000"))

# ============================= Small utils =============================
def _soft_require(cond: bool, msg: str) -> bool:
    if not cond:
        log.warning({"op": "soft-require-failed", "msg": msg})
        return False
    return True

def is_truthy(s: Optional[str]) -> bool:
    return str(s or "").strip().lower() in {"1", "true", "yes", "y"}

# ============================= Dialing codes =============================
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
    "argentina": "+54", "uruguay": "+598", "brazil": "+55", "cuba": "+53",
    "dominican republic": "+1", "jamaica": "+1", "trinidad and tobago": "+1", "barbados": "+1", "bahamas": "+1",
    "canada": "+1", "united states": "+1", "united states of america": "+1", "usa": "+1",
}
_DIAL_PREFIXES = sorted({v.replace("+", "").replace("-", "") for v in _COUNTRY_TO_DIAL.values()},
                        key=lambda x: (-len(x), x))

def _norm_country(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    return s

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

def country_to_dial(country: str, phone: str = "") -> str:
    if not country and phone:
        code = _infer_from_phone(phone)
        return f"+{code}" if code else ""
    s = _norm_country(country)
    if s in _NANP or any(k in s for k in ["usa", "united states"]):
        return "+1"
    if s in _COUNTRY_TO_DIAL:
        return _COUNTRY_TO_DIAL[s]
    if phone:
        code = _infer_from_phone(phone)
        return f"+{code}" if code else ""
    return ""

# ============================= Blob helpers =============================
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

# ============================= HTTP helpers =============================
def httpx_client(timeout: int = 90) -> httpx.Client:
    return httpx.Client(timeout=timeout)

# ============================= Translator (MS) =============================
def _translate_and_detect_ms(texts: List[str], to_lang: str = "en") -> List[dict]:
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
            with httpx_client(90) as h:
                r = h.post(url, headers=headers, json=payload)
                r.raise_for_status()
                data = r.json()
            for item in data:
                t_en = item["translations"][0]["text"]
                det = item.get("detectedLanguage") or {}
                lang = (det.get("language", "") or "").lower()
                conf = float(det.get("score", det.get("confidence", 0)) or 0)
                out.append({"translated": t_en, "lang": lang, "confidence": conf})
        except Exception as e:
            log.warning({"op": "translate-fallback-ms-batch", "error": str(e)})
            out.extend([{"translated": t or "", "lang": "en", "confidence": 0.0} for t in batch])
    return out

# ============================= LLM batched (Azure OpenAI) =============================
def _llm_chat_jsonl(prompt: str, temperature: float = 0, max_tokens: int = 3300) -> List[Dict[str, Any]]:
    if not (AOAI_EP and AOAI_KEY and AOAI_DEP):
        raise RuntimeError("LLM not configured")
    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
    body = {
        "messages": [
            {"role": "system", "content": LLM_SYS},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    for attempt in range(5):
        try:
            with httpx_client(120) as h:
                r = h.post(url, headers=headers, json=body)
                r.raise_for_status()
                j = r.json()
            content = (j["choices"][0]["message"]["content"] or "").strip()
            out = []
            for line in content.splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    out.append(json.loads(s))
                except Exception:
                    continue
            return out
        except Exception as e:
            wait = min(2 ** attempt, 20)
            log.warning({"op": "llm-retry", "attempt": attempt + 1, "wait": wait, "error": str(e)})
            time.sleep(wait)
    raise RuntimeError("LLM call failed after retries")

def _batch_rows_to_lines(rows: List[Dict[str, Any]]) -> str:
    def esc(s: str) -> str:
        s = (s or "").replace("\\", "\\\\").replace('"', '\\"')
        return s
    return "\n".join([f'- id="{r["id"]}" text="{esc(r["text"])}"' for r in rows])

def llm_translate_and_extract_batch(texts: List[str], to_lang: str = "en") -> List[dict]:
    rows = [{"id": i, "text": (t or "")} for i, t in enumerate(texts)]
    lines = _batch_rows_to_lines(rows)
    prompt = f"""
You are a professional translator and information extractor.

For each input row:
1) Detect language of "text".
2) Translate "text" to {to_lang}.
3) Extract entities from the translated English text:
   - country, phone, book ("Gyan Ganga"|"Way of Living"|""), language_mentioned, address

Output exactly one JSON object per input row (JSONL).
Keys: id, translated, lang, confidence, country, phone, book, language_mentioned, address.

Rows:
{lines}
"""
    output = _llm_chat_jsonl(prompt, temperature=0, max_tokens=3300)
    out_map = {int(o.get("id", -1)): o for o in output if "id" in o}
    aligned: List[dict] = []
    for i, t in enumerate(texts):
        o = out_map.get(i) or {}
        aligned.append({
            "translated": str(o.get("translated", t or "")),
            "lang": str(o.get("lang", "")),
            "confidence": float(o.get("confidence", 0.0) or 0.0),
            "country": str(o.get("country", "")),
            "phone": str(o.get("phone", "")),
            "book": str(o.get("book", "")),
            "language_mentioned": str(o.get("language_mentioned", "")),
            "address": str(o.get("address", "")),
        })
    return aligned

def llm_entities_only_batch(texts_en: List[str]) -> List[dict]:
    rows = [{"id": i, "text": (t or "")} for i, t in enumerate(texts_en)]
    lines = _batch_rows_to_lines(rows)
    prompt = f"""
You are an information extractor. Each row "text" is English.

Extract: country, phone, book("Gyan Ganga"|"Way of Living"|""), language_mentioned, address.
Return one JSON object per line with keys: id,country,phone,book,language_mentioned,address.

Rows:
{lines}
"""
    output = _llm_chat_jsonl(prompt, temperature=0, max_tokens=2800)
    out_map = {int(o.get("id", -1)): o for o in output if "id" in o}
    aligned: List[dict] = []
    for i in range(len(texts_en)):
        o = out_map.get(i) or {}
        aligned.append({
            "country": str(o.get("country", "")),
            "phone": str(o.get("phone", "")),
            "book": str(o.get("book", "")),
            "language_mentioned": str(o.get("language_mentioned", "")),
            "address": str(o.get("address", "")),
        })
    return aligned

# ============================= Unified translation dispatcher =============================
def translate_and_detect(texts: List[str], to_lang: str = "en", provider: Optional[str] = None) -> List[dict]:
    prov = (provider or TRANSLATE_PROVIDER or "llm").lower()

    if prov == "ms":
        trans = _translate_and_detect_ms(texts, to_lang=to_lang)
        translated = [r.get("translated", "") for r in trans]
        ents_full: List[dict] = []
        for i in range(0, len(translated), LLM_MAX_BATCH):
            part = translated[i:i + LLM_MAX_BATCH]
            try:
                ents = llm_entities_only_batch(part)
            except Exception as e:
                log.warning({"op":"llm-entities-failed", "slice": [i, i+len(part)], "error": str(e)})
                ents = [{"country":"", "phone":"", "book":"", "language_mentioned":"", "address":""} for _ in part]
            ents_full.extend(ents)
            del part; gc.collect()

        out: List[dict] = []
        for r, e in zip(trans, ents_full):
            out.append({
                "translated": r.get("translated", ""),
                "lang": r.get("lang", ""),
                "confidence": float(r.get("confidence", 0.0) or 0.0),
                "country": e.get("country",""),
                "phone": e.get("phone",""),
                "book": e.get("book",""),
                "language_mentioned": e.get("language_mentioned",""),
                "address": e.get("address",""),
            })
        return out

    out_full: List[dict] = []
    for i in range(0, len(texts), LLM_MAX_BATCH):
        batch = texts[i:i + LLM_MAX_BATCH]
        log.info({"op":"llm-chunk", "start": i, "end": i+len(batch), "size": len(batch)})
        res = llm_translate_and_extract_batch(batch, to_lang=to_lang)
        out_full.extend(res)
        del batch, res; gc.collect()
    return out_full

# ============================= Text column choice =============================
TEXT_COL_CANDIDATES = {"text", "message", "content", "description", "body"}

def choose_text_col_from_header(headers: List[str], requested: Optional[str]) -> int:
    if requested:
        for idx, h in enumerate(headers):
            if h == requested or h.strip().lower() == requested.strip().lower():
                return idx
    lowered = [h.strip().lower() for h in headers]
    for i, h in enumerate(lowered):
        if h in TEXT_COL_CANDIDATES:
            return i
    return 0 if headers else 0

# ============================= Streaming readers =============================
def iter_csv_rows(path: str, text_column: Optional[str]) -> Iterable[List[str]]:
    first = True
    for chunk in pd.read_csv(
        path,
        chunksize=CSV_CHUNK,
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
        engine="python",
    ):
        for c in chunk.columns:
            chunk[c] = chunk[c].astype(str).fillna("")
        if first:
            headers = [str(c) if c is not None else "" for c in chunk.columns.tolist()]
            yield headers
            first = False
        for _, row in chunk.iterrows():
            yield [row.get(c, "") for c in chunk.columns]
        del chunk; gc.collect()

def iter_excel_rows(path: str, text_column: Optional[str]) -> Iterable[List[str]]:
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        rows = ws.iter_rows(values_only=True)
        headers = next(rows, None)
        if headers is None:
            yield ["text"]
            for r in rows:
                vals = [(v if v is not None else "") for v in (r or [])]
                text = " ".join([str(x) for x in vals if str(x).strip() != ""])
                yield [text]
            return
        headers = [str(h) if h is not None else "" for h in headers]
        yield headers
        for r in rows:
            vals = [(v if v is not None else "") for v in (r or [])]
            if len(vals) < len(headers):
                vals = list(vals) + [""] * (len(headers) - len(vals))
            else:
                vals = list(vals[:len(headers)])
            vals = [str(v) if v is not None else "" for v in vals]
            yield vals
    finally:
        wb.close()

# ============================= Core streaming processor =============================
def process_rows_streaming(
    rows_iter: Iterable[List[str]],
    requested_text_col: Optional[str],
    to_lang: str,
    provider: Optional[str],
    outfile: io.TextIOBase,
):
    writer = csv.writer(outfile, lineterminator="\n")
    header = next(rows_iter, None)
    if header is None:
        writer.writerow(["text","translated_en","source_lang","translation_confidence","was_translated",
                         "translation_needs_review","country","phone","book","language_mentioned","address","dialing_code"])
        return

    header = [str(h) if h is not None else "" for h in header]
    writer.writerow(header + ["translated_en","source_lang","translation_confidence","was_translated",
                              "translation_needs_review","country","phone","book","language_mentioned","address","dialing_code"])

    text_col_idx = choose_text_col_from_header(header, requested_text_col)

    buf_rows: List[List[str]] = []
    buf_texts: List[str] = []

    def flush_batch():
        if not buf_rows:
            return
        results = translate_and_detect(buf_texts, to_lang=to_lang, provider=provider)
        for original_row, res in zip(buf_rows, results):
            t_en = res.get("translated","")
            lang = (res.get("lang","") or "").lower()
            conf = float(res.get("confidence", 0.0) or 0.0)
            was_trans = bool(lang and lang != "en")
            needs_review = bool(was_trans and conf < ENTITY_CONF_THRESHOLD)

            country = res.get("country","")
            phone = res.get("phone","")
            book = res.get("book","")
            language_mentioned = res.get("language_mentioned","")
            address = res.get("address","")
            dial = country_to_dial(country, phone)

            writer.writerow(original_row + [
                t_en, lang, f"{conf:.3f}", "true" if was_trans else "false",
                "true" if needs_review else "false",
                country, phone, book, language_mentioned, address, dial
            ])
        buf_rows.clear(); buf_texts.clear()
        gc.collect()

    for row in rows_iter:
        if len(row) < len(header):
            row = list(row) + [""] * (len(header) - len(row))
        else:
            row = list(row[:len(header)])
        text = str(row[text_col_idx] or "").strip()
        buf_rows.append(row)
        buf_texts.append(text)
        if len(buf_rows) >= LLM_MAX_BATCH:
            flush_batch()

    flush_batch()

# ============================= File helpers =============================
def stream_file_iter(path: str, chunk_size: int = 1024*256):
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk

def guess_ext(name: str) -> str:
    nl = (name or "").lower()
    if nl.endswith(".csv"): return ".csv"
    if nl.endswith(".xlsx"): return ".xlsx"
    if nl.endswith(".xlsm"): return ".xlsm"
    if nl.endswith(".xls"): return ".xls"
    return os.path.splitext(nl)[1] or ".csv"

def process_local_file_to_csv(in_path: str, original_name: str, text_column: Optional[str], provider: Optional[str]) -> str:
    ext = guess_ext(original_name)
    out_fd, out_path = tempfile.mkstemp(prefix="enriched_", suffix=".csv")
    os.close(out_fd)
    with open(out_path, "w", encoding="utf-8", newline="") as out_io:
        rows = iter_csv_rows(in_path, text_column) if ext == ".csv" else iter_excel_rows(in_path, text_column)
        process_rows_streaming(rows_iter=rows, requested_text_col=text_column, to_lang="en", provider=provider, outfile=out_io)
    return out_path

def upload_processed_blob(blob_service: BlobServiceClient, processed_path: str, processed_filename: str) -> None:
    ensure_container(blob_service, "processed")
    client = blob_service.get_blob_client(container="processed", blob=processed_filename)
    with open(processed_path, "rb") as fh:
        client.upload_blob(fh, overwrite=True, content_settings=ContentSettings(content_type="text/csv"))
    log.info({"op":"blob-upload", "blob": f"processed/{processed_filename}", "bytes": os.path.getsize(processed_path)})

# ============================= Endpoints =============================
@app.get("/health")
async def health_check():
    # Plain text makes it easy for quick checks; still JSON is fine if you prefer.
    return PlainTextResponse("ok")

@app.get("/logs")
async def get_logs(limit: int = 500):
    """
    Return recent server logs for the UI. The buffer is process-local and capped.
    Query: ?limit=500 (default)
    """
    try:
        limit = max(1, min(limit, LOG_CAPACITY))
    except Exception:
        limit = 500
    lines = list(_LOG_DEQUE)[-limit:]
    # Disable caches
    headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
    }
    return JSONResponse({"logs": lines, "count": len(lines), "capacity": LOG_CAPACITY}, headers=headers)

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
        "CSV_CHUNK": CSV_CHUNK,
        "STORAGE_ACCOUNT_NAME": os.environ.get("STORAGE_ACCOUNT_NAME"),
        "AZURE_STORAGE_CONNECTION_STRING": "REDACTED" if os.environ.get("AZURE_STORAGE_CONNECTION_STRING") else "MISSING",
        "CORS_ALLOW_ORIGINS": allow_origins,
        "LOG_BUFFER_CAPACITY": LOG_CAPACITY,
    }

@app.post("/translate")
async def translate_api(payload: dict):
    req_id = uuid.uuid4().hex[:8]
    texts = payload.get("texts") or []
    to = payload.get("to", "en")
    provider = (payload.get("provider") or "").lower() or None
    if not isinstance(texts, list) or len(texts) == 0:
        raise HTTPException(400, "Provide texts: []")
    t0 = time.time()
    # do the heavy work off the event loop
    out = await anyio.to_thread.run_sync(lambda: translate_and_detect([str(x) for x in texts], to_lang=to, provider=provider))
    log.info({"op": "translate", "n": len(texts), "ms": int((time.time() - t0) * 1000), "provider": provider or TRANSLATE_PROVIDER, "req_id": req_id})
    return {"rows": out}

@app.post("/process-xlsx")
async def process_xlsx(
    blob_name: str = Query(..., description="e.g., 'incoming/sample.xlsx' or 'incoming/sample.csv'"),
    text_column: Optional[str] = Query(None),
    provider: Optional[str] = Query(None)
):
    req_id = uuid.uuid4().hex[:8]
    if not blob_name.lower().endswith((".xlsx", ".xlsm", ".xls", ".csv")) or not blob_name.startswith("incoming/"):
        raise HTTPException(400, "Provide valid blob_name (e.g., 'incoming/sample.xlsx' or 'incoming/sample.csv')")

    blob_service = get_blob_client()
    if blob_service is None:
        raise HTTPException(500, "Storage not configured")

    ensure_container(blob_service, "incoming")
    ensure_container(blob_service, "processed")

    in_tmp_fd, in_tmp_path = tempfile.mkstemp(prefix="incoming_", suffix=os.path.splitext(blob_name)[1])
    os.close(in_tmp_fd)
    try:
        # Download in a worker thread to avoid blocking the loop
        def _download():
            in_client = blob_service.get_blob_client(container="incoming", blob=blob_name.replace("incoming/", ""))
            log.info({"op": "blob-download-start", "blob": blob_name, "req_id": req_id})
            with open(in_tmp_path, "wb") as fh:
                downloader = in_client.download_blob()
                downloader.readinto(fh)
            log.info({"op": "blob-download-complete", "blob": blob_name, "bytes": os.path.getsize(in_tmp_path), "req_id": req_id})

        await anyio.to_thread.run_sync(_download)

        # Process file -> temp CSV (thread)
        t0 = time.time()
        out_path = await anyio.to_thread.run_sync(lambda: process_local_file_to_csv(in_tmp_path, original_name=blob_name, text_column=text_column, provider=provider))
        ms = int((time.time() - t0) * 1000)
        processed_filename = os.path.basename(blob_name).rsplit(".", 1)[0] + "_enriched.csv"
        log.info({"op": "process-xlsx", "blob": blob_name, "processed": processed_filename, "ms": ms, "provider": provider or TRANSLATE_PROVIDER, "req_id": req_id})

        # Upload processed (thread)
        await anyio.to_thread.run_sync(lambda: upload_processed_blob(blob_service, out_path, processed_filename))

        headers = {"Content-Disposition": f'attachment; filename="{processed_filename}"'}
        return StreamingResponse(stream_file_iter(out_path), media_type="text/csv", headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        log.exception({"op": "process-failed-soft", "blob": blob_name, "error": str(e), "req_id": req_id})
        return PlainTextResponse(f"error,{str(e)}\n", media_type="text/csv", status_code=200)
    finally:
        try:
            os.remove(in_tmp_path)
        except Exception:
            pass

@app.post("/process-upload")
async def process_upload(
    file: UploadFile = File(...),
    text_column: Optional[str] = Form(None),
    provider: Optional[str] = Form(None)
):
    req_id = uuid.uuid4().hex[:8]
    original = file.filename or "upload.csv"
    safe_base = re.sub(r"[^A-Za-z0-9_.-]", "_", os.path.basename(original))
    ts = time.strftime("%Y%m%d-%H%M%S")
    incoming_name = f"{ts}_{safe_base}"

    in_fd, in_path = tempfile.mkstemp(prefix="upload_", suffix=os.path.splitext(safe_base)[1] or ".csv")
    os.close(in_fd)
    try:
        data = await file.read()
        # minimal time on the loop
        await anyio.to_thread.run_sync(lambda: open(in_path, "wb").write(data))
        del data; gc.collect()

        blob_service = get_blob_client()

        if blob_service is not None:
            try:
                def _upload_incoming():
                    ensure_container(blob_service, "incoming")
                    ctype = "text/csv" if safe_base.lower().endswith(".csv") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    in_client = blob_service.get_blob_client(container="incoming", blob=incoming_name)
                    with open(in_path, "rb") as fh:
                        in_client.upload_blob(fh, overwrite=True, content_settings=ContentSettings(content_type=ctype))
                await anyio.to_thread.run_sync(_upload_incoming)
                log.info({"op": "upload-to-incoming", "blob": f"incoming/{incoming_name}", "bytes": os.path.getsize(in_path), "req_id": req_id})
            except Exception as e:
                log.warning({"op": "blob-route-failed", "error": str(e), "req_id": req_id})

        # Process streaming -> temp out (thread)
        t0 = time.time()
        out_path = await anyio.to_thread.run_sync(lambda: process_local_file_to_csv(in_path, original_name=safe_base, text_column=text_column, provider=provider))
        ms = int((time.time() - t0) * 1000)
        processed_filename = os.path.splitext(safe_base)[0] + "_enriched.csv"
        log.info({"op":"process-upload", "file": safe_base, "processed": processed_filename, "ms": ms, "provider": provider or TRANSLATE_PROVIDER, "req_id": req_id})

        # Upload processed (thread) if storage available
        if blob_service is not None:
            try:
                await anyio.to_thread.run_sync(lambda: upload_processed_blob(blob_service, out_path, processed_filename))
            except Exception as e:
                log.warning({"op":"processed-upload-skip", "error": str(e), "req_id": req_id})

        headers = {"Content-Disposition": f'attachment; filename="{processed_filename}"'}
        return StreamingResponse(stream_file_iter(out_path), media_type="text/csv", headers=headers)

    except HTTPException:
        raise
    except Exception as e:
        log.exception({"op":"process-upload-failed-soft", "error": str(e), "req_id": req_id})
        return PlainTextResponse(f"error,{str(e)}\n", media_type="text/csv", status_code=200)
    finally:
        try: os.remove(in_path)
        except Exception: pass
