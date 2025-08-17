from fastapi import FastAPI, Request
import logging, time

app = FastAPI()
log = logging.getLogger("uvicorn.error")

@app.middleware("http")
async def log_req_res(request: Request, call_next):
    t0 = time.time()
    try:
        resp = await call_next(request)
        log.info({"path": request.url.path, "status": resp.status_code, "ms": int((time.time()-t0)*1000)})
        return resp
    except Exception as e:
        log.exception({"path": request.url.path, "error": str(e)})
        raise

@app.get("/")
def home():
    return {"message": "FastAPI on Azure App Service (F1) is alive."}

@app.get("/healthz")
def healthz():
    return {"ok": True}
