import { useEffect, useState } from "react";
import { Activity, Gauge, Globe2, MapPinned, RefreshCw } from "lucide-react";
import "./re1.css";

const API = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";
const number = (value) => new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);

export default function Re1() {
  const [state, setState] = useState({ status: "loading" });
  const load = async () => { setState({ status: "loading" }); try { const response = await fetch(`${API}/network/summary`); if (!response.ok) throw new Error(`Network API returned ${response.status}.`); setState({ status: "success", data: await response.json() }); } catch (error) { setState({ status: "error", message: error.message }); } };
  useEffect(() => { void load(); }, []);
  return <div className="page-body re1-page"><section className="page-intro"><div><div className="eyebrow"><span className="eyebrow-line" /> Operations brief / 01</div><h1>Good morning,<br /><em>network team.</em></h1><p className="intro-copy">A clear view of Milan activity, tuned for the decisions that matter in the next hour.</p></div><button className="refresh-button" type="button" onClick={load}><RefreshCw size={16} /> Refresh</button></section>{state.status === "loading" && <div className="state-panel panel">Reading network pulse...</div>}{state.status === "error" && <div className="state-panel error-state panel"><strong>Dashboard unavailable</strong><span>{state.message}</span><button onClick={load} type="button">Try again</button></div>}{state.status === "success" && <Overview data={state.data} />}</div>;
}

function Overview({ data }) { return <><section className="metric-grid"><Metric icon={<Activity size={19} />} label="Total activity" value={number(data.total_activity)} unit="events" /><Metric icon={<Globe2 size={19} />} label="Active grids" value={number(data.active_grids)} unit="cells" /><Metric icon={<Gauge size={19} />} label="Peak hour" value={new Date(data.peak_hour).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} unit="API local time" /><Metric icon={<MapPinned size={19} />} label="Top grid" value={String(data.top_grid)} unit="grid ID" /></section><section className="lower-grid"><div className="panel activity-panel"><span className="section-kicker">Network pulse</span><h2>Activity across the window</h2><div className="chart-bars">{[38,52,46,68,61,76,66,84,73,92,78,64,57,69,48,59,43,51,35,42,31,38,27,34].map((height, index) => <i key={index} style={{ height: `${height}%` }} />)}</div><div className="chart-footer"><span>Volume signal</span><strong>AS_OF {data.as_of}</strong></div></div><div className="panel brief-panel"><span className="section-kicker">Operator brief</span><h2>What stands out</h2><p>Grid {data.top_grid} leads current activity.</p><p>{number(data.active_grids)} grids contributed to the latest summary.</p><p>Reporting timestamp: {data.as_of}</p></div></section></>; }
function Metric({ icon, label, value, unit }) { return <article className="metric-card"><span className="metric-icon">{icon}</span><span className="metric-label">{label}</span><strong>{value}</strong><small>{unit}</small></article>; }
