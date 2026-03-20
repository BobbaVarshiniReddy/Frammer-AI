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

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Builds a direct, human-readable answer from the query result.
 * - 0 rows  → "No data found."
 * - 1 row   → key: value pairs
 * - many    → direct answer using the first row as the "winner",
 *             e.g. "Channel A has the biggest drop-off with 4,654."
 */
function buildReplyText(question, data) {
  if (!data || !data.rows || data.rows.length === 0) {
    return "No data found for that query.";
  }

  const { columns, rows } = data;
  const firstRow = rows[0];

  // Single row → show all key: value pairs
  if (rows.length === 1) {
    return columns
      .map((col, i) => `${col.replace(/_/g, " ")}: ${firstRow[i] ?? "—"}`)
      .join("\n");
  }

  // Multiple rows → build a direct answer from the top result
  // Find the "name" column (first text-looking column)
  const nameIdx = columns.findIndex((c) =>
    /name|type|language|month|user|channel|input|output/i.test(c)
  );

  // Find the "metric" column (first numeric-looking column after nameIdx)
  const metricIdx = columns.findIndex(
    (c, i) => i !== nameIdx && /count|hours|rate|pct|ratio|drop|total|avg/i.test(c)
  );

  if (nameIdx !== -1 && metricIdx !== -1) {
    const topName   = firstRow[nameIdx];
    const topMetric = firstRow[metricIdx];
    const metricLabel = columns[metricIdx].replace(/_/g, " ");
    return `${topName} leads with ${topMetric} ${metricLabel}.\n\nFull results below ↓`;
  }

  // Fallback
  return `Found ${rows.length} results. Full breakdown below ↓`;
}

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

  if (chart.startsWith("data:image/png")) {
    return (
      <div className="frammer-chart-wrap">
        <img src={chart} alt="Query result chart" className="frammer-chart-img" />
      </div>
    );
  }

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
              <th key={col} className="frammer-th">
                {col.replace(/_/g, " ")}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row, ri) => (
            <tr key={ri} className="frammer-tr">
              {row.map((cell, ci) => (
                <td
                  key={ci}
                  className={`frammer-td${cell === null ? " frammer-td--null" : ""}`}
                >
                  {cell === null ? "—" : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 20 && (
        <div className="frammer-table-more">
          showing 20 of {rows.length} rows
        </div>
      )}
    </div>
  );
}

// ─── Message ──────────────────────────────────────────────────────────────────
function Message({ msg }) {
  const [showSQL, setShowSQL] = useState(false);

  if (msg.role === "user") {
    return (
      <div className="frammer-msg-row frammer-msg-row--user">
        <span className="frammer-msg-label frammer-msg-label--user">you</span>
        <div className="frammer-bubble frammer-bubble--user">{msg.content}</div>
      </div>
    );
  }

  if (msg.loading) {
    return (
      <div className="frammer-msg-row frammer-msg-row--assistant">
        <span className="frammer-msg-label frammer-msg-label--assistant">frammer ai</span>
        <LoadingDots />
      </div>
    );
  }

  if (msg.error) {
    return (
      <div className="frammer-msg-row frammer-msg-row--assistant">
        <span className="frammer-msg-label frammer-msg-label--assistant">frammer ai</span>
        <div className="frammer-bubble--error">⚠ {msg.error}</div>
      </div>
    );
  }

  return (
    <div className="frammer-msg-row frammer-msg-row--assistant">
      <span className="frammer-msg-label frammer-msg-label--assistant">frammer ai</span>

      {/* Direct answer text */}
      <div className="frammer-bubble frammer-bubble--assistant">{msg.content}</div>

      {/* Chart — shown if available */}
      {msg.chart && <ChartDisplay chart={msg.chart} />}

      {/* Table — ALWAYS shown (whether or not chart exists) */}
      {msg.data && <DataTable data={msg.data} />}

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
          {showSQL && <pre className="frammer-sql-block">{msg.sql}</pre>}
        </>
      )}
    </div>
  );
}

// ─── FrammerChatbot (main export) ─────────────────────────────────────────────
export default function FrammerChatbot() {
  const [messages,  setMessages]  = useState([]);
  const [input,     setInput]     = useState("");
  const [loading,   setLoading]   = useState(false);
  const bottomRef   = useRef(null);
  const textareaRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendQuestion = useCallback(async (question) => {
    const q = question.trim();
    if (!q || loading) return;

    setInput("");
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

      if (data.error) {
        setMessages((prev) => [
          ...prev.slice(0, -1),
          { id: Date.now() + 2, role: "assistant", error: data.error },
        ]);
        setLoading(false);
        return;
      }

      // Build a direct, meaningful answer
      const replyText = buildReplyText(q, data.data);

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
          id:    Date.now() + 2,
          role:  "assistant",
          error: `Connection error: ${err.message}. Is the backend running on port 8000?`,
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [loading]);

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

        {messages.map((msg) => (
          <Message key={msg.id} msg={msg} />
        ))}

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