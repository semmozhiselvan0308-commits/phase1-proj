# Phase 5 React Dashboard

Phase 5 is one React application. The root shell is in `src/App.jsx`, shared styling is in `src/App.css`, and each RE is a component with its own stylesheet:

- `src/components/re1.jsx` and `re1.css`: overview and `/network/summary`
- `src/components/re2.jsx` and `re2.css`: grid explorer and `/network/grid/{grid_id}`
- `src/components/re3.jsx` and `re3.css`: hotspots, alerts, and Milan GeoJSON map
- `src/components/re4.jsx` and `re4.css`: pipeline status
- `src/components/re5.jsx` and `re5.css`: predictive risk

The application consumes the unified FastAPI service. The only frontend file read is the static map reference at `public/reference/milano-grid.geojson`.

## Run

Start the unified API from the project root:

```powershell
.\.venv\Scripts\python.exe -m uvicorn phase4.main:app --reload --port 8000
```

Then start the single frontend from `phase5`:

```powershell
npm.cmd install
npm.cmd run dev
```

Open `http://localhost:5173/`. Navigation in the Signal Room shell switches between RE1, RE2, RE3, RE4, and RE5 without separate frontend servers.
