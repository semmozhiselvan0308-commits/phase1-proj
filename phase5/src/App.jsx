import { useState } from "react";
import { LayoutDashboard, MapPinned, Radio, ShieldCheck } from "lucide-react";
import Re1 from "./components/re1.jsx";
import Re2 from "./components/re2.jsx";
import Re3 from "./components/re3.jsx";
import Re5 from "./components/re5.jsx";

const pages = [
  ["re1", "Overview", LayoutDashboard],
  ["re2", "Grid watch", MapPinned],
  ["re3", "Risk signals", ShieldCheck],
  ["re5", "Predictive risk", ShieldCheck],
];

export default function App() {
  const [page, setPage] = useState("re1");
  const active = pages.find(([id]) => id === page);
  const Component = { re1: Re1, re2: Re2, re3: Re3, re5: Re5 }[page];

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-lockup">
          <div className="brand-mark"><Radio size={18} strokeWidth={2.5} /></div>
          <div><p className="brand-name">SIGNAL ROOM</p><p className="brand-subtitle">Network operations</p></div>
        </div>
        <div className="sidebar-section-label">Workspace</div>
        <nav className="primary-nav" aria-label="Primary navigation">
          {pages.map(([id, label, Icon]) => (
            <button className={`nav-item ${page === id ? "active" : ""}`} key={id} onClick={() => setPage(id)} type="button">
              <Icon size={17} /><span>{label}</span>{page === id && <span className="nav-chevron">›</span>}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom"><div className="system-card"><div className="system-card-heading"><span className="status-dot" /> API connection</div><strong>Unified workspace</strong><span className="system-card-caption">http://127.0.0.1:8000</span></div><div className="user-chip"><div className="avatar">NO</div><div><strong>Network ops</strong><span>Read-only workspace</span></div></div></div>
      </aside>
      <main className="main-content">
        <header className="topbar"><div className="breadcrumb"><span>Workspace</span><b>›</b><strong>{active[1]}</strong></div><div className="topbar-actions"><span className="live-label"><span className="live-pulse" /> Live monitor</span><span className="date-label">07 Nov 2013 data window</span></div></header>
        <Component onNavigate={setPage} />
      </main>
    </div>
  );
}
