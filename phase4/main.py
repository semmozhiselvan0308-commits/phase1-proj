from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from phase4.api1.app.api.routes.network import router as summary_router
from phase4.api2.app.api.routes.grid import router as grid_router
from phase4.api3.app import app as api3_app
from phase4.api5.router import router as risk_router
from phase4.api6.router import router as support_router


app = FastAPI(
    title="Network Intelligence API",
    version="1.0.0",
    description="Unified API for network analytics, alerts, risk, and pipeline support.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "http://localhost:5175",
        "http://127.0.0.1:5175",
        "http://localhost:5176",
        "http://127.0.0.1:5176",
        "http://localhost:5177",
        "http://127.0.0.1:5177",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(summary_router)
app.include_router(grid_router)
app.include_router(api3_app.router)
app.include_router(risk_router)
app.include_router(support_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "network-intelligence-unified-api",
    }