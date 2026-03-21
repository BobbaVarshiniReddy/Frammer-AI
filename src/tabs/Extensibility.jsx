import { useState, useEffect, useRef } from "react";
import "./Extensibility.css";

// ─── Constants ────────────────────────────────────────────────────────────────
const STATES = ["MAPPING", "QUALITY", "CLASSIFY", "KPI_GEN", "COMPLETE"];

const SAMPLE_COLUMNS = [
  { raw: "vid_dur",    suggestion: "Duration",    confidence: 0.62 },
  { raw: "ch_name",   suggestion: "Channel",      confidence: 0.91 },
  { raw: "pub_stat",  suggestion: "Published",    confidence: 0.55 },
  { raw: "lang_cd",   suggestion: "Language",     confidence: 0.88 },
  { raw: "inp_typ",   suggestion: "Input Type",   confidence: 0.48 },
];

const SAMPLE_CHART_DATA = [
  { label: "YouTube",   value: 42 },
  { label: "LinkedIn",  value: 27 },
  { label: "Instagram", value: 18 },
  { label: "TikTok",    value: 13 },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────
function PulseBar({ active }) {
  return (
    <span className={`ext-pulse-dot ${active ? "ext-pulse-active" : ""}`} />
  );
}

function StateMachineHeader({ currentState, aborted }) {
  return (
    <div className="ext-state-header">
      {STATES.map((s, i) => {
        const idx      = STATES.indexOf(currentState);
        const isDone   = !aborted && i < idx;
        const isActive = !aborted && s === currentState;
        // eslint-disable-next-line no-unused-vars
        const isFuture = aborted || i > idx;
        return (
          <div key={s} className="ext-state-step">
            <div
              className={`ext-state-pill ${
                aborted && isActive
                  ? "ext-state-aborted"
                  : isDone
                  ? "ext-state-done"
                  : isActive
                  ? "ext-state-active"
                  : "ext-state-future"
              }`}
            >
              {isActive && !aborted && <PulseBar active />}
              Step {i + 1}: {s}
            </div>
            {i < STATES.length - 1 && (
              <div className={`ext-state-arrow ${isDone ? "ext-arrow-done" : ""}`}>→</div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function ConfusionCard({ col, onAccept, onDrop, onAbort }) {
  return (
    <div className="ext-confusion-card">
      <div className="ext-confusion-header">
        <span className="ext-confusion-icon">🤔</span>
        <span className="ext-confusion-title">Agent needs your input</span>
        <span className="ext-confidence-badge">
          Confidence: {Math.round(col.confidence * 100)}%
        </span>
      </div>
      <p className="ext-confusion-body">
        I found <code className="ext-code">{col.raw}</code>. It looks like{" "}
        <strong>{col.suggestion}</strong>. Should I map it?
      </p>
      <div className="ext-confusion-actions">
        <button className="ext-btn ext-btn-accept" onClick={onAccept}>
          ✅ Accept — Yes, this is {col.suggestion}
        </button>
        <button className="ext-btn ext-btn-drop" onClick={onDrop}>
          ❌ Drop — No, discard this column
        </button>
        <button className="ext-btn ext-btn-abort" onClick={onAbort}>
          🛑 Abort — Stop, this file is bad
        </button>
      </div>
    </div>
  );
}

function MiniBarChart({ data }) {
  const max = Math.max(...data.map((d) => d.value));
  return (
    <div className="ext-mini-chart">
      <div className="ext-mini-chart-title">Channel Distribution (Preview)</div>
      <div className="ext-bars">
        {data.map((d) => (
          <div key={d.label} className="ext-bar-row">
            <span className="ext-bar-label">{d.label}</span>
            <div className="ext-bar-track">
              <div
                className="ext-bar-fill"
                style={{ width: `${(d.value / max) * 100}%` }}
              />
            </div>
            <span className="ext-bar-value">{d.value}%</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DataPulseSidebar({ currentState, validCols, warnings, blockers, showChart }) {
  return (
    <aside className="ext-sidebar">
      <div className="ext-sidebar-title">📊 Data Pulse</div>

      <div className="ext-pulse-summary">
        <span className="ext-pulse-stat ext-stat-valid">
          Valid Cols: {validCols}
        </span>
        <span className="ext-divider">|</span>
        <span className="ext-pulse-stat ext-stat-warn">
          Warnings: {warnings}
        </span>
        <span className="ext-divider">|</span>
        <span className="ext-pulse-stat ext-stat-block">
          Critical Blockers: {blockers}
        </span>
      </div>

      {showChart && <MiniBarChart data={SAMPLE_CHART_DATA} />}

      {!showChart && (
        <div className="ext-sidebar-placeholder">
          Chart will appear once data reaches{" "}
          <span className="ext-highlight">QUALITY</span> state.
        </div>
      )}

      <div className="ext-sidebar-log">
        <div className="ext-log-title">Agent Log</div>
        {STATES.slice(0, STATES.indexOf(currentState) + 1).map((s) => (
          <div key={s} className="ext-log-entry">
            <span className="ext-log-dot" />
            {s} — {s === currentState ? "in progress…" : "completed"}
          </div>
        ))}
      </div>
    </aside>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
export default function Extensibility() {
  const [stateIdx,    setStateIdx]    = useState(0);
  const [aborted,     setAborted]     = useState(false);
  const [completed,   setCompleted]   = useState(false);
  const [colQueue,    setColQueue]    = useState([...SAMPLE_COLUMNS]);
  const [currentCol,  setCurrentCol]  = useState(null);
  const [validCols,   setValidCols]   = useState(0);
  const [warnings,    setWarnings]    = useState(0);
  const [blockers,    setBlockers]    = useState(0);
  const [running,     setRunning]     = useState(false);
  const timerRef = useRef(null);

  const currentState = aborted ? STATES[stateIdx] : STATES[Math.min(stateIdx, STATES.length - 1)];
  const showChart    = stateIdx >= STATES.indexOf("QUALITY");

  // Auto-advance through states unless waiting for HITL input
  useEffect(() => {
    if (aborted || completed || !running || currentCol) return;

    if (stateIdx >= STATES.length - 1) {
      setCompleted(true);
      setRunning(false);
      return;
    }

    // In MAPPING state: pop next low-confidence column
    if (STATES[stateIdx] === "MAPPING" && colQueue.length > 0) {
      const next = colQueue[0];
      if (next.confidence < 0.65) {
        timerRef.current = setTimeout(() => {
          setCurrentCol(next);
          setColQueue((q) => q.slice(1));
        }, 900);
        return;
      } else {
        // auto-accept high confidence
        timerRef.current = setTimeout(() => {
          setValidCols((v) => v + 1);
          setColQueue((q) => q.slice(1));
        }, 500);
        return;
      }
    }

    // Move to next state when queue empty or not in MAPPING
    if (STATES[stateIdx] === "MAPPING" && colQueue.length === 0) {
      timerRef.current = setTimeout(() => setStateIdx((i) => i + 1), 1200);
      return;
    }

    timerRef.current = setTimeout(() => setStateIdx((i) => i + 1), 1800);

    return () => clearTimeout(timerRef.current);
  }, [stateIdx, running, currentCol, colQueue, aborted, completed]);

  const handleStart = () => {
    setStateIdx(0);
    setAborted(false);
    setCompleted(false);
    setColQueue([...SAMPLE_COLUMNS]);
    setCurrentCol(null);
    setValidCols(0);
    setWarnings(0);
    setBlockers(0);
    setRunning(true);
  };

  const handleAccept = () => {
    setValidCols((v) => v + 1);
    setCurrentCol(null);
  };

  const handleDrop = () => {
    setWarnings((w) => w + 1);
    setCurrentCol(null);
  };

  const handleAbort = () => {
    setBlockers((b) => b + 1);
    setAborted(true);
    setRunning(false);
    setCurrentCol(null);
    clearTimeout(timerRef.current);
  };

  return (
    <div className="ext-root">
      {/* ── State Machine Header ── */}
      <StateMachineHeader currentState={currentState} aborted={aborted} />

      {/* ── Body ── */}
      <div className="ext-body">
        {/* ── Main Workspace ── */}
        <main className="ext-workspace">
          <div className="ext-workspace-header">
            <h2 className="ext-title">Extensibility</h2>
            <p className="ext-subtitle">
              Human-in-the-Loop Preprocessing Agent
            </p>
          </div>

          {/* Start / Reset button */}
          {!running && !completed && !aborted && (
            <button className="ext-btn ext-btn-start" onClick={handleStart}>
              ▶ Start Agent Pipeline
            </button>
          )}

          {(completed || aborted) && (
            <button className="ext-btn ext-btn-start" onClick={handleStart}>
              🔄 Restart Pipeline
            </button>
          )}

          {/* Completed banner */}
          {completed && (
            <div className="ext-banner ext-banner-complete">
              ✅ Pipeline complete! All states processed successfully.
            </div>
          )}

          {/* Aborted banner */}
          {aborted && (
            <div className="ext-banner ext-banner-abort">
              🛑 Pipeline aborted. The file was flagged as bad. Please restart with a clean file.
            </div>
          )}

          {/* HITL Confusion Card */}
          {currentCol && !aborted && (
            <ConfusionCard
              col={currentCol}
              onAccept={handleAccept}
              onDrop={handleDrop}
              onAbort={handleAbort}
            />
          )}

          {/* Running indicator */}
          {running && !currentCol && !completed && (
            <div className="ext-running">
              <span className="ext-spinner" />
              Processing <strong>{currentState}</strong> state…
            </div>
          )}

          {/* Column resolution log */}
          {(validCols > 0 || warnings > 0) && (
            <div className="ext-resolution-log">
              <div className="ext-log-title">Resolution Log</div>
              {validCols > 0 && (
                <div className="ext-log-accepted">✅ {validCols} column(s) accepted</div>
              )}
              {warnings > 0 && (
                <div className="ext-log-dropped">❌ {warnings} column(s) dropped</div>
              )}
            </div>
          )}
        </main>

        {/* ── Data Pulse Sidebar ── */}
        <DataPulseSidebar
          currentState={currentState}
          validCols={validCols}
          warnings={warnings}
          blockers={blockers}
          showChart={showChart}
        />
      </div>
    </div>
  );
}