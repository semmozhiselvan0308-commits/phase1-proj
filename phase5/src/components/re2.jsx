import { useEffect, useState } from "react";
import { MapPinned } from "lucide-react";
import "./re2.css";

const API = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";
const format = (value) => Number(value).toLocaleString("en-US", { maximumFractionDigits: 2 });

export default function Re2() {
  const [grid, setGrid] = useState("4821"); const [points, setPoints] = useState([]); const [message, setMessage] = useState("Loading grid 4821...");
  const load = async (event) => { event?.preventDefault(); setMessage("Loading activity..."); try { const response = await fetch(`${API}/network/grid/${Number(grid)}`); if (response.status === 404) throw new Error(`Grid ${grid} was not found.`); if (!response.ok) throw new Error(`Grid API returned ${response.status}.`); setPoints(await response.json()); setMessage(""); } catch (error) { setPoints([]); setMessage(error.message); } };
  useEffect(() => { void load(); }, []);
  return <div className="page-body re2-page"><section className="page-intro"><div><div className="eyebrow"><span className="eyebrow-line" /> Grid watch / 02</div><h1>Grid explorer.</h1><p className="intro-copy">Inspect hourly SMS, call, internet, and total activity for one grid.</p></div><form onSubmit={load} className="grid-form"><label>Grid ID<input type="number" min="1" value={grid} onChange={(event) => setGrid(event.target.value)} /></label><button type="submit"><MapPinned size={15} /> Explore</button></form></section>{points.length ? <section className="panel data-panel"><div className="panel-heading"><div><span className="section-kicker">Activity window</span><h2>Grid {grid}</h2></div><span className="small-status">{points.length} hourly points</span></div><div className="table-scroll"><table><thead><tr><th>Time</th><th>SMS activity</th><th>Call activity</th><th>Internet activity</th><th>Total activity</th></tr></thead><tbody>{points.map((point) => <tr key={point.timestamp}><td>{point.timestamp}</td><td>{format(point.sms_activity)}</td><td>{format(point.call_activity)}</td><td>{format(point.internet_activity)}</td><td><strong>{format(point.total_activity)}</strong></td></tr>)}</tbody></table></div></section> : <div className="state-panel panel">{message}</div>}</div>;
}
