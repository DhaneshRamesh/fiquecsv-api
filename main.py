import os, io, time, json, logging
from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
import httpx
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.eventgrid import EventGridEvent

app = FastAPI()
log = logging.getLogger("uvicorn.error")

# ---------- config ----------
TRN_EP = os.environ.get("TRANSLATOR_ENDPOINT", "").rstrip("/")
TRN_KEY = os.environ.get("TRANSLATOR_KEY", "")
AOAI_EP = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
AOAI_DEP = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "")
AOAI_VER = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
AOAI_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")
STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT_NAME", "")

def _require(cond, msg): 
    if not cond: raise HTTPException(500, msg)

# ---------- blob storage client ----------
def get_blob_client():
    _require(STORAGE_ACCOUNT, "Storage account not configured")
    credential = DefaultAzureCredential()  # Managed Identity
    return BlobServiceClient(f"https://{STORAGE_ACCOUNT}.blob.core.windows.net", credential=credential)

# ---------- helpers ----------
def translate_texts(texts: List[str], to_lang="en") -> List[str]:
    _require(TRN_EP and TRN_KEY, "Translator not configured")
    url = f"{TRN_EP}/translate?api-version=3.0&to={to_lang}"
    headers = {"Content-Type":"application/json; charset=UTF-8",
               "Ocp-Apim-Subscription-Key": TRN_KEY}
    payload = [{"Text": t or ""} for t in texts]
    with httpx.Client(timeout=60) as h:
        r = h.post(url, headers=headers, json=payload)
        r.raise_for_status()
    data = r.json()
    return [item["translations"][0]["text"] for item in data]

def extract_entities(text: str) -> dict:
    _require(AOAI_EP and AOAI_KEY and AOAI_DEP, "Azure OpenAI not configured")
    url = f"{AOAI_EP}/openai/deployments/{AOAI_DEP}/chat/completions?api-version={AOAI_VER}"
    headers = {"Content-Type": "application/json", "api-key": AOAI_KEY}
    schema = {
        "type":"object",
        "properties":{
            "country":{"type":"string"},
            "phone":{"type":"string"},
            "book":{"type":"string", "enum":["Gyan Ganga", "Way of Living", ""]},
            "language_mentioned":{"type":"string"}
        },
        "required":[]
    }
    body = {
        "messages": [{"role": "user", "content": f"Extract the fields from this text. Text: {text}"}],
        "response_format":{"type":"json_schema", "json_schema":{"name":"entities","schema":schema}}
    }
    with httpx.Client(timeout=60) as h:
        r = h.post(url, headers=headers, json=body)
        r.raise_for_status()
    j = r.json()
    return json.loads(j["choices"][0]["message"]["content"])

def process_excel_blob(blob_name: str) -> tuple[bytes, str]:
    blob_service = get_blob_client()
    container_name = "incoming"
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    
    # Download and read Excel
    content = blob_client.download_blob().readall()
    df = pd.read_excel(io.BytesIO(content))

    # Pick a text column (best-effort guess)
    candidates = [c for c in df.columns if c.lower() in {"text","message","content","description"}]
    col = candidates[0] if candidates else df.columns[0]

    # Translate and extract entities
    texts = df[col].astype(str).tolist()
    translated = translate_texts(texts, to_lang="en")
    rows = []
    for t in translated:
        try:
            rows.append(extract_entities(t))
        except Exception as e:
            log.exception({"op":"extract-failed","text":t[:80]})
            rows.append({})

    # Enrich DataFrame
    edf = df.copy()
    edf["translated_en"] = translated
    ents_df = pd.json_normalize(rows)
    out_df = pd.concat([edf, ents_df], axis=1)

    # Save to BytesIO
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        out_df.to_excel(w, index=False)
    out.seek(0)

    # Upload to processed/
    processed_blob_name = blob_name.replace("incoming/", "processed/").rsplit(".", 1)[0] + "_enriched.xlsx"
    blob_client = blob_service.get_blob_client(container="processed", blob=processed_blob_name)
    blob_client.upload_blob(out.getvalue(), overwrite=True)
    log.info({"op":"blob-upload", "blob":processed_blob_name})

    return out.getvalue(), processed_blob_name

# ---------- endpoints ----------
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

@app.post("/webhook")
async def webhook(request: Request):
    """
    Handle Event Grid events for new blobs in incoming/.
    Process Excel and save enriched output to processed/.
    """
    events = await request.json()
    for event in events:
        if event.get("eventType") == "Microsoft.Storage.BlobCreated":
            blob_name = event["data"]["blobUrl"].split(f"{STORAGE_ACCOUNT}.blob.core.windows.net/")[1]
            if blob_name.startswith("incoming/"):
                try:
                    t0 = time.time()
                    content, processed_blob_name = process_excel_blob(blob_name)
                    log.info({"op":"webhook-process","blob":blob_name,"processed":processed_blob_name,"ms":int((time.time()-t0)*1000)})
                except Exception as e:
                    log.exception({"op":"webhook-failed","blob":blob_name})
                    raise HTTPException(500, f"Failed to process {blob_name}")
    return {"status": "ok"}

@app.post("/process-xlsx")
async def process_xlsx(file: UploadFile = File(...), text_column: str | None = None):
    """
    Manual upload endpoint for testing: process Excel, return enriched file, and save to processed/.
    """
    if not file.filename.lower().endswith((".xlsx",".xls")):
        raise HTTPException(400, "Upload an .xlsx/.xls file")
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content))

    # Pick a text column
    if text_column and text_column in df.columns:
        col = text_column
    else:
        candidates = [c for c in df.columns if c.lower() in {"text","message","content","description"}]
        col = candidates[0] if candidates else df.columns[0]

    texts = df[col].astype(str).tolist()
    translated = translate_texts(texts, to_lang="en")
    rows = []
    for t in translated:
        try:
            rows.append(extract_entities(t))
        except Exception as e:
            log.exception({"op":"extract-failed","text":t[:80]})
            rows.append({})

    edf = df.copy()
    edf["translated_en"] = translated
    ents_df = pd.json_normalize(rows)
    out_df = pd.concat([edf, ents_df], axis=1)

    # Save to BytesIO
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        out_df.to_excel(w, index=False)
    out.seek(0)

    # Upload to processed/
    enriched_filename = f"processed/{file.filename.rsplit('.',1)[0]}_enriched.xlsx"
    blob_service = get_blob_client()
    blob_client = blob_service.get_blob_client(container="processed", blob=enriched_filename)
    blob_client.upload_blob(out.getvalue(), overwrite=True)
    log.info({"op":"blob-upload","blob":enriched_filename})

    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{enriched_filename.rsplit("/",1)[-1]}"'}
    )
