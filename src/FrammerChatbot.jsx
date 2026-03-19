// FrammerChatbot.jsx
// ------------------
// Drop-in chatbot sidebar for Frammer AI — Executive Summary tab.
// Calls POST http://localhost:8000/query  { question: "..." }
// Renders charts (PNG / HTML), data tables, SQL viewer.

import { useState, useRef, useEffect, useCallback } from "react";
import "./FrammerChatbot.css";

// ─── Config ───────────────────────────────────────────────────────────────────
const API_URL = "http://localhost:8000/query";

const SUGGESTED_QUESTIONS = [
  "Which channel has the biggest drop-off?",
  "Show monthly upload & publish trend",
  "Top 5 users by uploaded hours",
  "What is the publish rate by input type?",
  "Which language has the most content?",
  "Show overall KPIs",
];

// ─── LoadingDots ──────────────────────────────────────────────────────────────
function LoadingDots() {
  return (
    <div className="frammer-loading-wrap">
      <div className="frammer-dot" />
      <div className="frammer-dot" />
      <div className="frammer-dot" />
      <span className="frammer-loading-text">querying…</span>
    </div>
  );
}

// ─── ChartDisplay ─────────────────────────────────────────────────────────────
function ChartDisplay({ chart }) {
  if (!chart) return null;

  // PNG base64
  if (chart.startsWith("data:image/png")) {
    return (
      <div className="frammer-chart-wrap">
        <img src={chart} alt="Query result chart" className="frammer-chart-img" />
      </div>
    );
  }

  // HTML base64 — render in sandboxed iframe
  if (chart.startsWith("data:text/html")) {
    const html = atob(chart.split(",")[1]);
    const blob = new Blob([html], { type: "text/html" });
    const url  = URL.createObjectURL(blob);
    return (
      <div className="frammer-chart-wrap">
        <iframe
          src={url}
          className="frammer-chart-iframe"
          title="Interactive chart"
          sandbox="allow-scripts"
        />
      </div>
    );
  }

  return null;
}

// ─── DataTable ────────────────────────────────────────────────────────────────
function DataTable({ data }) {
  if (!data || !data.rows || data.rows.length === 0) return null;
  const { columns, rows } = data;
  const displayRows = rows.slice(0, 20);

  return (
    <div className="frammer-table-wrap">
      <table className="frammer-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col} className="frammer-th">{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row, ri) => (
            <tr key={ri} className="frammer-tr">
              {row.map((cell, ci) => (
                <td key={ci} className={`frammer-td${cell === null ? " frammer-td--null" : ""}`}>
                  {cell === null ? "—" : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Message ──────────────────────────────────────────────────────────────────
function Message({ msg }) {
  const [showSQL, setShowSQL] = useState(false);

  // User bubble
  if (msg.role === "user") {
    return (
      <div className="frammer-msg-row frammer-msg-row--user">
        <span className="frammer-msg-label frammer-msg-label--user">you</span>
        <div className="frammer-bubble frammer-bubble--user">{msg.content}</div>
      </div>
    );
  }

  // Loading state
  if (msg.loading) {
    return (
      <div className="frammer-msg-row frammer-msg-row--assistant">
        <span className="frammer-msg-label frammer-msg-label--assistant">frammer ai</span>
        <LoadingDots />
      </div>
    );
  }

  // Error state
  if (msg.error) {
    return (
      <div className="frammer-msg-row frammer-msg-row--assistant">
        <span className="frammer-msg-label frammer-msg-label--assistant">frammer ai</span>
        <div className="frammer-bubble--error">⚠ {msg.error}</div>
      </div>
    );
  }

  // Normal assistant response
  return (
    <div className="frammer-msg-row frammer-msg-row--assistant">
      <span className="frammer-msg-label frammer-msg-label--assistant">frammer ai</span>

      {/* Text */}
      <div className="frammer-bubble frammer-bubble--assistant">{msg.content}</div>

      {/* Chart — PNG or interactive HTML */}
      {msg.chart && <ChartDisplay chart={msg.chart} />}

      {/* Table — shown when no chart */}
      {msg.data && !msg.chart && <DataTable data={msg.data} />}

      {/* Stats pills */}
      {msg.explanation && (
        <div className="frammer-stats-row">
          {msg.explanation.rows_returned !== undefined && (
            <span className="frammer-stat-pill">
              {msg.explanation.rows_returned} rows
            </span>
          )}
          {msg.explanation.tables_used?.map((t) => (
            <span key={t} className="frammer-stat-pill">
              {t.toLowerCase()}
            </span>
          ))}
          {msg.retried && (
            <span className="frammer-stat-pill frammer-stat-pill--retried">
              retried
            </span>
          )}
        </div>
      )}

      {/* SQL toggle */}
      {msg.sql && (
        <>
          <button
            className="frammer-sql-toggle"
            onClick={() => setShowSQL((v) => !v)}
          >
            {showSQL ? "hide sql ▲" : "view sql ▼"}
          </button>
          {showSQL && (
            <pre className="frammer-sql-block">{msg.sql}</pre>
          )}
        </>
      )}
    </div>
  );
}

// ─── FrammerChatbot (main export) ─────────────────────────────────────────────
export default function FrammerChatbot() {
  const [messages, setMessages] = useState([]);
  const [input,    setInput]    = useState("");
  const [loading,  setLoading]  = useState(false);
  const bottomRef  = useRef(null);
  const textareaRef = useRef(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Send a question to the backend
  const sendQuestion = useCallback(async (question) => {
    const q = question.trim();
    if (!q || loading) return;

    setInput("");

    // Append user message + loading placeholder
    setMessages((prev) => [
      ...prev,
      { id: Date.now(),     role: "user",      content: q },
      { id: Date.now() + 1, role: "assistant", loading: true },
    ]);
    setLoading(true);

    try {
      const res = await fetch(API_URL, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ question: q }),
      });

      if (!res.ok) throw new Error(`Server error ${res.status}`);
      const data = await res.json();

      // Handle pipeline error from backend
      if (data.error) {
        setMessages((prev) => [
          ...prev.slice(0, -1),
          { id: Date.now() + 2, role: "assistant", error: data.error },
        ]);
        setLoading(false);
        return;
      }

      // Build reply text
      const rows = data.data?.rows?.length ?? 0;
      let replyText = "";

      if (rows === 0) {
        replyText = "No data found for that query.";
      } else if (rows === 1) {
        // Single row → key: value format
        const cols = data.data.columns;
        const row  = data.data.rows[0];
        replyText  = cols.map((c, i) => `${c}: ${row[i] ?? "—"}`).join("\n");
      } else {
        replyText = `Found ${rows} result${rows > 1 ? "s" : ""}.${
          data.chart ? " Chart generated below." : ""
        }`;
      }

      setMessages((prev) => [
        ...prev.slice(0, -1),
        {
          id:          Date.now() + 2,
          role:        "assistant",
          content:     replyText,
          chart:       data.chart       || null,
          data:        data.data        || null,
          sql:         data.sql         || null,
          explanation: data.explanation || null,
          retried:     data.retried     || false,
        },
      ]);
    } catch (err) {
      setMessages((prev) => [
        ...prev.slice(0, -1),
        {
          id:   Date.now() + 2,
          role: "assistant",
          error: `Connection error: ${err.message}. Is the backend running on port 8000?`,
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [loading]);

  // Enter to send, Shift+Enter for newline
  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendQuestion(input);
    }
  };

  return (
    <div className="frammer-sidebar">

      {/* ── Header ── */}
      <div className="frammer-header">
        <div className="frammer-header-dot" />
        <div className="frammer-header-text">
          <div className="frammer-header-title">Analytics Chat</div>
          <div className="frammer-header-sub">ask anything about your data</div>
        </div>
        {messages.length > 0 && (
          <button
            className="frammer-clear-btn"
            onClick={() => setMessages([])}
          >
            CLEAR
          </button>
        )}
      </div>

      {/* ── Messages ── */}
      <div className="frammer-messages-area">

        {/* Suggested questions — shown only when chat is empty */}
        {messages.length === 0 && (
          <div className="frammer-suggested-wrap">
            <div className="frammer-suggested-label">Try asking</div>
            {SUGGESTED_QUESTIONS.map((q) => (
              <button
                key={q}
                className="frammer-suggested-btn"
                onClick={() => sendQuestion(q)}
              >
                {q}
              </button>
            ))}
          </div>
        )}

        {/* Chat messages */}
        {messages.map((msg) => (
          <Message key={msg.id} msg={msg} />
        ))}

        {/* Scroll anchor */}
        <div ref={bottomRef} />
      </div>

      {/* ── Input ── */}
      <div className="frammer-input-area">
        <div className="frammer-input-row">
          <textarea
            ref={textareaRef}
            className="frammer-textarea"
            placeholder="Ask about channels, users, trends…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={loading}
            rows={1}
          />
          <button
            className="frammer-send-btn"
            onClick={() => sendQuestion(input)}
            disabled={loading || !input.trim()}
            aria-label="Send"
          >
            {/* Send icon */}
            <svg
              width="16" height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="white"
              strokeWidth="2.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <line x1="22" y1="2" x2="11" y2="13" />
              <polygon points="22 2 15 22 11 13 2 9 22 2" />
            </svg>
          </button>
        </div>
        <div className="frammer-input-hint">
          ENTER to send &nbsp;·&nbsp; SHIFT+ENTER for newline
        </div>
      </div>

    </div>
  );
}