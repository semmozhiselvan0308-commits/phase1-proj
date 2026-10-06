from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from phase4.api1.app.api.routes.network import router as network_router


app = FastAPI(
    title="Network Intelligence Service",
    description="FastAPI service exposing curated telecom network intelligence.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(network_router)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "network-intelligence-api",
    }