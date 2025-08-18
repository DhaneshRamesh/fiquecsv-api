import os, io, time, json, logging
from typing import List
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# Custom logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fiquebot")

app = FastAPI()

# ---------- config ----------
TRN_EP = os.environ.get("AZURE_TRANSLATOR_ENDPOINT", "https://api.cognitive.microsofttranslator.com")
TRN_KEY = os.environ.get("AZURE_TRANSLATOR_KEY", "")
TRN_REGION = os.environ.get("AZURE_TRANSLATOR_REGION", "westeurope")
AOAI_EP = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
AOAI_DEP = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-35-turbo")
AOAI_VER = os.environ.get("AZURE_OPENAI_API_VERSION", "2023-07-01-preview")
AOAI_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")
STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT_NAME", "fiqueuploadstore")

def _require(cond, msg):
    if not cond: raise HTTPException(500, msg)

# ---------- blob storage client ----------
def get_blob_client():
    _require(STORAGE_ACCOUNT, "Storage account not configured")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if connection_string:
        log.info("Using connection string for Blob Service Client")
        return BlobServiceClient.from_connection_string(connection_string)
    else:
        log.error("Connection string not found, falling back to DefaultAzureCredential")
        credential = DefaultAzureCredential()
        return BlobServiceClient(f"https://{STORAGE_ACCOUNT}.blob.core.windows.net", credential=credential)

# ---------- helpers ----------
def translate_texts(texts: List[str], to_lang="en") -> List[str]:
    _require(TRN_EP and TRN_KEY and TRN_REGION, "Translator not configured")
    log.info({"op": "translate-input", "texts": texts})
    url = f"{TRN_EP}/translate?api-version=3.0&to={to_lang}"
    headers = {
        "Ocp-Apim-Subscription-Key": TRN_KEY,
        "Ocp-Apim-Subscription-Region": TRN_REGION,
        "Content-Type": "application/json"
    }
    translated = []
    for i in range(0, len(texts), 50):  # Batch in groups of 50
        batch = texts[i:i + 50]
        payload = [{"Text": t or ""} for t in batch]
        with httpx.Client(timeout=60) as h:
            r = h.post(url, headers=headers, json=payload)
            r.raise_for_status()
        data = r.json()
        translated.extend([item["translations"][0]["text"] for item in data])
    return translated

def extract_entities(text: str) -> dict:
    _require(AOAI_EP and AOAI_KEY and AOAI_DEP, "Azure OpenAI not configured")
    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
    prompt = f"""
    Extract entities from the following text and return them as JSON with fields: country, phone, book, language_mentioned, address.
    Book must be either "Gyan Ganga", "Way of Living", or empty string "". Use empty string "" for any field not found.
    Text: {text}
    Return format: {{"country":"", "phone":"", "book":"", "language_mentioned":"", "address":""}}
    """
    body = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200,
        "temperature": 0.3
    }
    with httpx.Client(timeout=60) as h:
        r = h.post(url, headers=headers, json=body)
        r.raise_for_status()
    j = r.json()
    try:
        return json.loads(j["choices"][0]["message"]["content"])
    except:
        log.error({"op":"parse-failed", "text":text[:80]})
        return {"country":"", "phone":"", "book":"", "language_mentioned":"", "address":""}

def process_excel_blob(blob_name: str, text_column: str | None = None) -> tuple[bytes, str]:
    log.info({"op": "process-excel-start", "blob": blob_name})
    blob_service = get_blob_client()
    container_name = "incoming"
    corrected_blob_name = blob_name.replace("incoming/", "")
    blob_client = blob_service.get_blob_client(container=container_name, blob=corrected_blob_name)
    log.info({"op": "blob-download-start", "blob": blob_name})
    try:
        content = blob_client.download_blob().readall()
        log.info({"op": "blob-download-complete", "blob": blob_name})
    except Exception as e:
        log.exception({"op": "blob-download-failed", "blob": blob_name, "error": str(e)})
        raise HTTPException(400, f"Failed to download {blob_name}: {str(e)}")
    try:
        if blob_name.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content), encoding="utf-8")
        else:
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
    except Exception as e:
        log.exception({"op":"file-read-failed", "blob":blob_name})
        raise HTTPException(400, f"Failed to read {blob_name}: {str(e)}")
    if text_column and text_column in df.columns:
        col = text_column
    else:
        candidates = [c for c in df.columns if c.lower() in {"text","message","content","description"}]
        col = candidates[0] if candidates else df.columns[0]
    texts = df[col].astype(str).tolist()
    texts = [t for t in texts if t != 'nan']
    log.info({"op": "filtered-texts", "count": len(texts)})
    translated = translate_texts(texts, to_lang="en")
    rows = []
    for i in range(0, len(translated), 10):  # Batch entity extraction in groups of 10
        batch = translated[i:i + 10]
        for t in batch:
            try:
                rows.append(extract_entities(t))
            except Exception as e:
                log.exception({"op":"extract-failed","text":t[:80]})
                rows.append({"country":"", "phone":"", "book":"", "language_mentioned":"", "address":""})
    edf = df.copy()
    edf["translated_en"] = translated
    ents_df = pd.json_normalize(rows)
    out_df = pd.concat([edf, ents_df], axis=1)
    out = io.StringIO()
    out_df.to_csv(out, index=False, encoding="utf-8")
    out_bytes = out.getvalue().encode("utf-8")
    out.seek(0)
    processed_blob_name = blob_name.replace("incoming/", "processed/").rsplit(".", 1)[0] + "_enriched.csv"
    blob_client = blob_service.get_blob_client(container="processed", blob=processed_blob_name)
    blob_client.upload_blob(out_bytes, overwrite=True)
    log.info({"op":"blob-upload", "blob":processed_blob_name})
    return out_bytes, processed_blob_name

# ---------- endpoints ----------
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
        "AZURE_STORAGE_CONNECTION_STRING": "REDACTED" if os.environ.get("AZURE_STORAGE_CONNECTION_STRING") else "MISSING"
    }

@app.post("/translate")
async def translate_api(payload: dict):
    """payload: { 'texts': ['...', '...'], 'to': 'en' }"""
    texts = payload.get("texts") or []
    to = payload.get("to", "en")
    if not isinstance(texts, list) or len(texts) == 0:
        raise HTTPException(400, "Provide texts: []")
    t0 = time.time()
    out = translate_texts([str(x) for x in texts], to_lang=to)
    log.info({"op":"translate","n":len(texts),"ms":int((time.time()-t0)*1000)})
    return {"translations": out}

@app.post("/process-xlsx")
async def process_xlsx(blob_name: str, text_column: str | None = None):
    """
    Process an Excel or CSV file from incoming/, save enriched file as CSV to processed/, and return it.
    blob_name: Path to file in incoming container (e.g., 'incoming/sample.xlsx').
    """
    if not blob_name.lower().endswith((".xlsx",".xlsm",".xls",".csv")) or not blob_name.startswith("incoming/"):
        raise HTTPException(400, "Provide valid blob_name (e.g., 'incoming/sample.xlsx' or 'incoming/sample.csv')")
    try:
        t0 = time.time()
        content, processed_blob_name = process_excel_blob(blob_name, text_column)
        log.info({"op":"process-xlsx","blob":blob_name,"processed":processed_blob_name,"ms":int((time.time()-t0)*1000)})
        return StreamingResponse(
            io.BytesIO(content),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{processed_blob_name.rsplit("/",1)[-1]}"'}
        )
    except Exception as e:
        log.exception({"op":"process-failed","blob":blob_name})
        raise HTTPException(500, f"Failed to process {blob_name}")
