import React, { useEffect, useState } from "react";
import CustomPlot from "../Components/CustomPlot";
import "./Funnel.css";

export default function Funnel() {

  const plotOptions = [
    { value: "input_type_publish_rate",         label: "Input Type Publish Rate" },
    { value: "input_type_amplification_ratio",  label: "Input Type Amplification Ratio" },
    { value: "output_format_publish_rate",      label: "Output Format Publish Rate" },
    { value: "published_vs_content_type",       label: "Published vs Content Type" },
    { value: "published_pct_by_type",           label: "Published % by Type" },
    { value: "content_flow_counts",             label: "Content Flow Counts" },
    { value: "created_vs_published_efficiency", label: "Created vs Published Efficiency" },
    { value: "published_count_per_platform",    label: "Published Count per Platform" },
    { value: "platform_distribution_pct",       label: "Platform Distribution %" },
  ];

  const kpiOptions = [
    {
      value  : "input_type_publish_rate",
      label  : "KPI-09 · Input Type Publish Rate",
      formula: "Input Type Publish Rate (%)\n= SUM(Published) ÷ SUM(Created) × 100\nGrouped by Input Type",
    },
    {
      value  : "input_type_amplification_ratio",
      label  : "KPI-10 · Input Type Amplification Ratio",
      formula: "Input Type Amplification Ratio\n= SUM(Created) ÷ SUM(Uploaded)\nGrouped by Input Type",
    },
    {
      value  : "unknown_input_type_rate",
      label  : "KPI-19 · Unknown Input Type Rate",
      formula: "Unknown Input Type Rate (%)\n= Uploaded (Input Type='unknown') ÷ Total Uploaded × 100",
    },
    {
      value  : "output_format_publish_rate",
      label  : "KPI-11 · Output Format Publish Rate",
      formula: "Output Format Publish Rate (%)\n= SUM(Published) ÷ SUM(Created) × 100\nGrouped by Output Format",
    },
    {
      value  : "output_format_mix_distribution",
      label  : "KPI-12 · Output Format Mix Distribution",
      formula: "Output Mix Distribution (%)\n= Created Count ÷ SUM(Total Created) × 100\nGrouped by Output Format",
    },
  ];

  const supportedChartTypes = new Set([
    "bar", "line", "heatmap", "kpi_card", "grouped_bar", "small_multiples", "scatter"
  ]);

  const [plotSelected,  setPlotSelected]  = useState(plotOptions[0]);
  const [plotData,      setPlotData]      = useState([]);
  const [plotChartType, setPlotChartType] = useState(null);
  const [plotLoading,   setPlotLoading]   = useState(false);
  const [plotError,     setPlotError]     = useState(null);

  const [kpiSelected,   setKpiSelected]   = useState(kpiOptions[0]);
  const [kpiData,       setKpiData]       = useState([]);
  const [kpiChartType,  setKpiChartType]  = useState(null);
  const [kpiLoading,    setKpiLoading]    = useState(false);
  const [kpiError,      setKpiError]      = useState(null);

  // ── Fetch Plot ──────────────────────────────────────────────────────────────
  useEffect(() => {
    setPlotLoading(true);
    setPlotError(null);
    fetch(`https://frammer-ai.onrender.com/funnel/plot/${plotSelected.value}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) { setPlotError(resData.error); setPlotData([]); setPlotChartType(null); }
        else { setPlotData(resData.data || []); setPlotChartType(resData.chart_type ?? null); }
        setPlotLoading(false);
      })
      .catch((err) => { setPlotError(err.message); setPlotLoading(false); });
  }, [plotSelected]);

  // ── Fetch KPI ───────────────────────────────────────────────────────────────
  useEffect(() => {
    setKpiLoading(true);
    setKpiError(null);
    fetch(`https://frammer-ai.onrender.com/funnel/kpi/${kpiSelected.value}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) { setKpiError(resData.error); setKpiData([]); setKpiChartType(null); }
        else { setKpiData(resData.data || []); setKpiChartType(resData.chart_type ?? null); }
        setKpiLoading(false);
      })
      .catch((err) => { setKpiError(err.message); setKpiLoading(false); });
  }, [kpiSelected]);

  // ── Styles ──────────────────────────────────────────────────────────────────
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

  const sectionLabelStyle = {
    marginBottom : "10px",
    color        : "#888888",
    fontSize     : "12px",
    textTransform: "uppercase",
    letterSpacing: "0.08em",
    fontWeight   : 600,
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

  // ── Render Content ───────────────────────────────────────────────────────────
  const renderContent = (data, chartType, loading, error, title) => {
    if (loading) return <p style={statusStyle("#888888")}>Loading...</p>;
    if (error)   return <p style={statusStyle("#FF4649")}>{error}</p>;
    if (!data || data.length === 0) return <p style={statusStyle("#888888")}>No data available</p>;

    if (supportedChartTypes.has(chartType))
      return <CustomPlot data={data} chartType={chartType} title={title} />;

    // Fallback table
    const headers = Object.keys(data[0] || {});
    return (
      <div style={{ background: "#111111", borderRadius: "8px", border: "1px solid #2A2A2A", overflow: "auto" }}>
        <table style={{ width: "100%", color: "white", borderCollapse: "collapse", fontSize: "13px" }}>
          <thead>
            <tr style={{ background: "#1A1A1A" }}>
              {headers.map((h) => (
                <th key={h} style={{
                  border       : "1px solid #2A2A2A",
                  padding      : "8px 12px",
                  color        : "#FF4649",
                  textAlign    : "left",
                  textTransform: "uppercase",
                  fontSize     : "12px",
                  letterSpacing: "0.05em",
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 50).map((row, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "#111111" : "#151515" }}>
                {headers.map((h) => (
                  <td key={h} style={{ border: "1px solid #2A2A2A", padding: "6px 12px" }}>{row[h]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {data.length > 50 && (
          <p style={{ color: "#888888", padding: "6px 12px", fontSize: "12px" }}>
            Showing first 50 rows
          </p>
        )}
      </div>
    );
  };

  return (
    <div className="funnel-dashboard">
      <h1 className="title">Type & Funnel</h1>

      

      <div style={{ display: "flex", gap: "20px", alignItems: "flex-start", flexWrap: "wrap", marginTop: "24px" }}>

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
            {renderContent(plotData, plotChartType, plotLoading, plotError, plotSelected.label)}
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
            {renderContent(kpiData, kpiChartType, kpiLoading, kpiError, kpiSelected.label)}

            {/* Formula Card */}
            <div style={formulaCardStyle}>
              <div style={{
                color        : "#888888",
                fontSize     : "11px",
                marginBottom : "8px",
                textTransform: "uppercase",
                letterSpacing: "0.08em",
              }}>
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