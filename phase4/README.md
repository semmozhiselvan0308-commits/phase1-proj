# Unified API

The project uses one FastAPI process for the phase 4 services. The unified app registers API1 summary, API2 grid activity, API3 hotspots and alerts, API5 risk prediction, and API6 operational support routes.

## Setup

From the project root:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r phase4\api1\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r phase4\api2\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r phase4\api3\requirements.txt
.\.venv\Scripts\python.exe -m pip install -r phase4\api5\requirements.txt
.\.venv\Scripts\python.exe -m uvicorn phase4.main:app --reload --port 8000
```

Check the unified service at `http://127.0.0.1:8000/health`.

All frontend API clients use `http://127.0.0.1:8000`. Start the required frontend dev servers separately, then open only the RE1 page at `http://127.0.0.1:5173/`.
