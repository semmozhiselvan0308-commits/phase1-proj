from fastapi import FastAPI

from phase4.api6.router import router

app = FastAPI(
    title="API6 - Operational Support API",
    version="1.0.0",
    description="Evidence endpoints for pipeline health and grid geography.",
)

app.include_router(router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "api6"}
