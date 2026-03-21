import React, { useEffect, useState } from "react";
import CustomPlot from "../Components/CustomPlot";

export default function UsageTrends() {

  const plotOptions = [
    { value: "total_created_vs_month",    label: "Total Created vs Month",         endpoint: "trends" },
    { value: "total_uploaded_vs_month",   label: "Total Uploaded vs Month",        endpoint: "trends" },
    { value: "total_published_vs_month",  label: "Total Published vs Month",       endpoint: "trends" },
    { value: "mom_increase_published",    label: "MoM Increase in Published",      endpoint: "trends" },
    { value: "mom_increase_uploaded",     label: "MoM Increase in Uploaded",       endpoint: "trends" },
    { value: "total_duration_by_channel", label: "Total Duration by Channel",      endpoint: "trends" },
    { value: "channel_total_duration",    label: "Channel Total Duration",         endpoint: "trends" },
    { value: "top5_uploaded_duration",    label: "Top 5 Uploaded Months (Hours)",  endpoint: "trends" },
    { value: "top5_published_duration",   label: "Top 5 Published Months (Hours)", endpoint: "trends" },
    { value: "uploaded_duration",         label: "Uploaded Duration vs Month",     endpoint: "duration" },
    { value: "created_duration",          label: "Created Duration vs Month",      endpoint: "duration" },
    { value: "published_duration",        label: "Published Duration vs Month",    endpoint: "duration" },
  ];

  const kpiOptions = [
    {
      value  : "monthly_created_duration_trend",
      label  : "Monthly Created Duration Trend",
      formula: "Monthly Created Duration (hours)\n= SUM(Total Created Duration converted to hours) per Month",
    },
    {
      value  : "mom_duration_growth",
      label  : "MoM Duration Growth (%)",
      formula: "MoM Duration Growth (%)\n= ((This Month Hours − Last Month Hours) ÷ Last Month Hours) × 100",
    },
    {
      value  : "total_content_hours_processed",
      label  : "Total Content Hours Processed",
      formula: "Total Content Hours (hours)\n= SUM(Total Created Duration converted to hours)\n(hours = hh + mm/60 + ss/3600)",
    },
  ];

  const [plotSelected,  setPlotSelected]  = useState(plotOptions[0]);
  const [plotData,      setPlotData]      = useState([]);
  const [plotChartType, setPlotChartType] = useState("line");
  const [plotLoading,   setPlotLoading]   = useState(false);
  const [plotError,     setPlotError]     = useState(null);

  const [kpiSelected,   setKpiSelected]   = useState(kpiOptions[0]);
  const [kpiData,       setKpiData]       = useState([]);
  const [kpiChartType,  setKpiChartType]  = useState("bar");
  const [kpiLoading,    setKpiLoading]    = useState(false);
  const [kpiError,      setKpiError]      = useState(null);

  // ── Fetch Plot ──────────────────────────────────────────────────────────────
  useEffect(() => {
    setPlotLoading(true);
    setPlotError(null);
    const url = plotSelected.endpoint === "duration"
      ? `https://frammer-ai.onrender.com/trends/plot/duration/${plotSelected.value}`
      : `https://frammer-ai.onrender.com/trends/plot/${plotSelected.value}`;
    fetch(url)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) { setPlotError(resData.error); setPlotData([]); }
        else { setPlotData(resData.data || []); setPlotChartType(resData.chart_type || "line"); }
        setPlotLoading(false);
      })
      .catch((err) => { setPlotError(err.message); setPlotLoading(false); });
  }, [plotSelected]);

  // ── Fetch KPI ───────────────────────────────────────────────────────────────
  useEffect(() => {
    setKpiLoading(true);
    setKpiError(null);
    fetch(`https://frammer-ai.onrender.com/trends/kpi/${kpiSelected.value}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) { setKpiError(resData.error); setKpiData([]); }
        else { setKpiData(resData.data || []); setKpiChartType(resData.chart_type || "bar"); }
        setKpiLoading(false);
      })
      .catch((err) => { setKpiError(err.message); setKpiLoading(false); });
  }, [kpiSelected]);

  // ── Styles ──────────────────────────────────────────────────────────────────
  const pageStyle = {
    padding         : "30px",
    color           : "white",
    fontFamily      : "'Inter', Arial, sans-serif",
    backgroundColor : "#0D0D0D",
    minHeight       : "100vh",
  };

  const titleStyle = {
    marginBottom : "24px",
    fontSize     : "26px",
    fontWeight   : 700,
    color        : "#FF4649",
    borderLeft   : "4px solid #FF4649",
    paddingLeft  : "12px",
  };

  const sectionLabelStyle = {
    marginBottom : "10px",
    color        : "#888888",
    fontSize     : "12px",
    textTransform: "uppercase",
    letterSpacing: "0.08em",
    fontWeight   : 600,
  };

  const selectStyle = {
    width        : "100%",
    maxWidth     : "380px",
    display      : "block",
    boxSizing    : "border-box",
    padding      : "8px 12px",
    background   : "#111111",
    color        : "white",
    border       : "1px solid #2A2A2A",
    borderRadius : "8px",
    cursor       : "pointer",
    fontSize     : "13px",
  };

  const panelStyle = {
    flex            : "1 1 520px",
    minWidth        : "520px",
    backgroundColor : "#111111",
    border          : "1px solid #2A2A2A",
    borderRadius    : "12px",
    padding         : "20px",
  };

  const formulaCardStyle = {
    marginTop    : "16px",
    background   : "#0D0D0D",
    border       : "1px solid #2A2A2A",
    borderRadius : "10px",
    padding      : "14px 16px",
    borderLeft   : "4px solid #FF4649",
  };

  const statusStyle = (color) => ({
    color    : color,
    fontSize : "13px",
    marginTop: "8px",
  });

  return (
    <div style={pageStyle}>
      <h2 style={titleStyle}>Usage & Trends</h2>

      <div style={{ display: "flex", gap: "20px", alignItems: "flex-start", flexWrap: "wrap" }}>

        {/* ── Plots Panel ── */}
        <div style={panelStyle}>
          <div style={sectionLabelStyle}>Plots</div>
          <select
            value={plotSelected.value}
            onChange={(e) => setPlotSelected(plotOptions.find((o) => o.value === e.target.value))}
            style={selectStyle}
          >
            {plotOptions.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>

          <div style={{ marginTop: "16px" }}>
            {plotLoading && <p style={statusStyle("#888888")}>Loading...</p>}
            {plotError   && <p style={statusStyle("#FF4649")}>{plotError}</p>}
            {!plotLoading && !plotError && plotData.length > 0 && (
              <CustomPlot data={plotData} chartType={plotChartType} title={plotSelected.label} />
            )}
            {!plotLoading && !plotError && plotData.length === 0 && (
              <p style={statusStyle("#888888")}>No data available</p>
            )}
          </div>
        </div>

        {/* ── KPIs Panel ── */}
        <div style={panelStyle}>
          <div style={sectionLabelStyle}>KPIs</div>
          <select
            value={kpiSelected.value}
            onChange={(e) => setKpiSelected(kpiOptions.find((o) => o.value === e.target.value))}
            style={selectStyle}
          >
            {kpiOptions.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>

          <div style={{ marginTop: "16px" }}>
            {kpiLoading && <p style={statusStyle("#888888")}>Loading...</p>}
            {kpiError   && <p style={statusStyle("#FF4649")}>{kpiError}</p>}
            {!kpiLoading && !kpiError && kpiData.length > 0 && (
              <CustomPlot data={kpiData} chartType={kpiChartType} title={kpiSelected.label} />
            )}
            {!kpiLoading && !kpiError && kpiData.length === 0 && (
              <p style={statusStyle("#888888")}>No data available</p>
            )}

            {/* Formula Card */}
            <div style={formulaCardStyle}>
              <div style={{ color: "#888888", fontSize: "11px", marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.08em" }}>
                Formula
              </div>
              <div style={{ color: "white", whiteSpace: "pre-line", fontSize: "13px", lineHeight: 1.6 }}>
                {kpiSelected.formula || "Formula not available"}
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}