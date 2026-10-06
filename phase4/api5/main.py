from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from phase4.api5.router import router

app = FastAPI(
    title="API5 - Network Risk Prediction API",
    version="1.0.0",
    description="Contract-first network risk prediction endpoint.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5177",
        "http://127.0.0.1:5177",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "api5"}
