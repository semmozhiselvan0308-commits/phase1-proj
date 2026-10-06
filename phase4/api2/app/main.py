from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from phase4.api2.app.api.routes.grid import router as grid_router


app = FastAPI(
    title="Network Intelligence Grid Activity API",
    description="FastAPI service exposing grid-level telecom activity time series.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5175",
        "http://127.0.0.1:5175",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(grid_router)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "network-intelligence-grid-api",
    }
