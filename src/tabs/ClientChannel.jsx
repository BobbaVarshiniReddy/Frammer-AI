import React, { useState, useEffect } from "react";
import CustomPlot from "../Components/CustomPlot";
import "./ClientChannel.css";

export default function ClientChannel() {

  const plotOptions = [
    { value: "created_vs_published_by_channel",        label: "Created vs Published by Channel" },
    { value: "upload_volume_by_channel",               label: "Upload Volume by Channel" },
    { value: "top3_uploaders_per_channel",             label: "Top 3 Uploaders per Channel" },
    { value: "top3_user_publish_rate_per_channel",     label: "Top 3 User Publish Rate per Channel" },
    { value: "top_users_publish_rate_across_channels", label: "Top Users Publish Rate across Channels" },
    { value: "user_contribution_per_channel",          label: "User Contribution % per Channel" },
    { value: "top10_users_by_publish_rate",            label: "Top 10 Users by Publish Rate" },
    { value: "plot_publish_rate",                      label: "User Publish Rate Plot" },
    { value: "plot_top_uploaders",                     label: "Top Uploaders Plot" },
    { value: "top_channels_by_publishes",              label: "Top Channels by Publishes" },
    { value: "platform_diversity_per_channel",         label: "Platform Diversity per Channel" },
    { value: "channel_publish_share_per_platform",     label: "Channel Publish Share per Platform" },
    { value: "language_publish_rate",                  label: "Language Publish Rate" },
    { value: "language_upload_share",                  label: "Language Upload Share" },
    { value: "published_vs_team",                      label: "Published vs Team" },
    { value: "top_uploaders",                          label: "Top Uploaders" },
    { value: "published_pct_by_uploader",              label: "Published % by Uploader" },
    { value: "platform_duration_distribution",         label: "Platform Duration Distribution" },
    { value: "absolute_duration_by_platform",          label: "Absolute Duration by Platform" },
    { value: "top_channels_by_duration",               label: "Top Channels by Duration" },
    { value: "platform_share_per_channel",             label: "Platform Share per Channel" },
  ];

  const kpiOptions = [
    {
      value  : "publish_rate_by_channel",
      label  : "KPI-06 · Publish Rate by Channel",
      formula: "Publish Rate (%)\n= SUM(Published) ÷ SUM(Created) × 100\nGrouped by Channel",
    },
    {
      value  : "user_publish_rate_within_channel",
      label  : "KPI-16 · User Publish Rate within Channel",
      formula: "User Publish Rate (%)\n= User Published ÷ User Created × 100\nRanked within each Channel",
    },
    {
      value  : "user_upload_volume_by_channel",
      label  : "KPI-17 · User Upload Volume by Channel",
      formula: "Upload Volume\n= SUM(Uploaded Count) per User per Channel\nRanked within Channel",
    },
    {
      value  : "channel_user_cross_dimension",
      label  : "KPI-18 · Channel × User Cross Dimension",
      formula: "Cross Dimension Metric\n= Published ÷ Created × 100 per Channel-User pair\nReveals same user behaving differently across channels",
    },
    {
      value  : "language_publish_rate",
      label  : "KPI-13 · Language Publish Rate",
      formula: "Language Publish Rate (%)\n= Published Count ÷ Created Count × 100\nGrouped by Language",
    },
    {
      value  : "language_upload_share",
      label  : "KPI-15 · Language Upload Share",
      formula: "Language Upload Share (%)\n= Uploaded Count ÷ SUM(Total Uploaded) × 100\nGrouped by Language",
    },
    {
      value  : "published_rate_per_uploader",
      label  : "KPI-16 · Published Rate per Uploader",
      formula: "Published Rate per Uploader (%)\n= Published Count ÷ Created Count × 100\nGrouped by Uploader",
    },
    {
      value  : "top_uploaders_kpi",
      label  : "KPI-17 · Top Uploaders",
      formula: "Top Uploaders\n= ORDER BY Uploaded Count DESC",
    },
    {
      value  : "zero_publish_channel_count",
      label  : "KPI-08 · Zero Publish Channel Count",
      formula: "Zero Publish Channel Count\n= COUNT(Channels WHERE Published = 0)",
    },
  ];

  const [plotSelected,  setPlotSelected]  = useState(plotOptions[0]);
  const [plotData,      setPlotData]      = useState([]);
  const [plotChartType, setPlotChartType] = useState("bar");
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
    // fetch(`https://frammer-ai.onrender.com/channel/plot/${plotSelected.value}`)
    fetch(`https://frammer-ai-1-spo5.onrender.com/channel/plot/${plotSelected.value}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) { setPlotError(resData.error); setPlotData([]); }
        else { setPlotData(resData.data || []); setPlotChartType(resData.chart_type || "bar"); }
        setPlotLoading(false);
      })
      .catch((err) => { setPlotError(err.message); setPlotLoading(false); });
  }, [plotSelected]);

  // ── Fetch KPI ───────────────────────────────────────────────────────────────
  useEffect(() => {
    setKpiLoading(true);
    setKpiError(null);
    fetch(`https://frammer-ai.onrender.com/channel/kpi/${kpiSelected.value}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) { setKpiError(resData.error); setKpiData([]); }
        else { setKpiData(resData.data || []); setKpiChartType(resData.chart_type || "bar"); }
        setKpiLoading(false);
      })
      .catch((err) => { setKpiError(err.message); setKpiLoading(false); });
  }, [kpiSelected]);

  // ── Shared Styles ───────────────────────────────────────────────────────────
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

  return (
    <div className="client-dashboard">
      <h2 className="title">Client & Channel Analysis</h2>

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
