import React, { useEffect, useState } from "react";
import CustomPlot from "../Components/CustomPlot";
import "./Funnel.css";

export default function Funnel() {
  const plotOptions = [
    { value: "output_format_publish_rate", label: "Output Format Publish Rate" },
    { value: "output_format_mix_distribution", label: "Output Format Mix Distribution" },
    { value: "language_publish_rate", label: "Language Publish Rate" },
    { value: "language_upload_share", label: "Language Upload Share" },
    { value: "input_type_publish_rate", label: "Input Type Publish Rate" },
    { value: "input_type_amplification_ratio", label: "Input Type Amplification Ratio" },
    { value: "unknown_input_type_contribution", label: "Unknown Input Type Contribution" },
  ];

  const kpiOptions = [
    { value: "output_format_publish_rate", label: "Output Format Publish Rate (KPI)" },
    { value: "output_format_mix_distribution", label: "Output Format Mix Distribution (KPI)" },
    { value: "language_publish_rate", label: "Language Publish Rate (KPI)" },
    { value: "language_upload_share", label: "Language Upload Share (KPI)" },
    { value: "input_type_publish_rate", label: "Input Type Publish Rate (KPI)" },
    { value: "input_type_amplification_ratio", label: "Input Type Amplification Ratio (KPI)" },
    { value: "unknown_input_type_rate", label: "Unknown Input Type Rate (KPI)" },
  ];

  const kpiFormulas = {
    output_format_publish_rate:
      "Publish Rate (%)\n= 100 * SUM(Published Count) / NULLIF(SUM(Created Count), 0)",
    output_format_mix_distribution:
      "Mix Distribution (%)\n= 100 * Created Count / NULLIF(SUM(Created Count) over all, 0)",
    language_publish_rate:
      "Language Publish Rate (%)\n= 100 * Published Count / NULLIF(Created Count, 0)",
    language_upload_share:
      "Language Upload Share (%)\n= 100 * Uploaded Count / NULLIF(SUM(Uploaded Count) over all, 0)",
    input_type_publish_rate:
      "Input Type Publish Rate (%)\n= 100 * SUM(Published Count) / NULLIF(SUM(Created Count), 0)",
    input_type_amplification_ratio:
      "Input Type Amplification Ratio\n= 1.0 * SUM(Created Count) / NULLIF(SUM(Uploaded Count), 0)",
    unknown_input_type_rate:
      "Unknown Input Type Rate (%)\n= 100 * Uploaded Count (Input Type='unknown') / NULLIF(Total Uploaded, 0)",
  };

  const supportedChartTypes = new Set(["bar", "line", "heatmap", "kpi_card"]);

  const renderTable = (data) => {
    if (!data || data.length === 0) return <p style={{ color: "gray" }}>No data available</p>;
    const limitedData = data.slice(0, 50);
    const headers = Object.keys(limitedData[0] || {});
    return (
      <div className="table-container">
        <table>
          <thead>
            <tr>
              {headers.map((h) => (
                <th key={h}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {limitedData.map((row, i) => (
              <tr key={i}>
                {headers.map((h) => (
                  <td key={h}>{row[h]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {data.length > 50 && <p className="table-note">Showing first 50 rows</p>}
      </div>
    );
  };

  const [plotSelectedItem, setPlotSelectedItem] = useState(plotOptions[0].value);
  const [plotData, setPlotData] = useState([]);
  const [plotChartType, setPlotChartType] = useState(null);
  const [plotLoading, setPlotLoading] = useState(false);
  const [plotError, setPlotError] = useState(null);

  const [kpiSelectedItem, setKpiSelectedItem] = useState(kpiOptions[0].value);
  const [kpiData, setKpiData] = useState([]);
  const [kpiChartType, setKpiChartType] = useState(null);
  const [kpiLoading, setKpiLoading] = useState(false);
  const [kpiError, setKpiError] = useState(null);

  useEffect(() => {
    setPlotLoading(true);
    setPlotError(null);
    fetch(`http://127.0.0.1:8000/funnel/plot/${plotSelectedItem}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) {
          setPlotError(resData.error);
          setPlotData([]);
          setPlotChartType(null);
        } else {
          setPlotData(resData.data || []);
          setPlotChartType(resData.chart_type ?? null);
        }
        setPlotLoading(false);
      })
      .catch((err) => {
        setPlotError(err.message);
        setPlotLoading(false);
      });
  }, [plotSelectedItem]);

  useEffect(() => {
    setKpiLoading(true);
    setKpiError(null);
    fetch(`http://127.0.0.1:8000/funnel/kpi/${kpiSelectedItem}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) {
          setKpiError(resData.error);
          setKpiData([]);
          setKpiChartType(null);
        } else {
          setKpiData(resData.data || []);
          setKpiChartType(resData.chart_type ?? null);
        }
        setKpiLoading(false);
      })
      .catch((err) => {
        setKpiError(err.message);
        setKpiLoading(false);
      });
  }, [kpiSelectedItem]);

  return (
    <div className="funnel-dashboard">
      <h1 className="title">Type & Funnel</h1>

      <div className="funnel-grid">
        <div className="funnel-card">
          <h3>Processing → Publishing Funnel</h3>
          <div className="funnel-steps">
            <div className="step upload">Uploaded</div>
            <div className="step process">Processed</div>
            <div className="step publish">Published</div>
          </div>
        </div>
      </div>

      <div style={{ display: "flex", gap: "18px", alignItems: "flex-start", flexWrap: "wrap", marginTop: "22px" }}>
        <div style={{ flex: "1 1 520px", minWidth: "520px" }}>
          <div style={{ marginBottom: "10px", color: "#bbb", fontSize: "14px" }}>Plots</div>
          <select
            value={plotSelectedItem}
            onChange={(e) => setPlotSelectedItem(e.target.value)}
            style={{
              width: "100%",
              maxWidth: "380px",
              display: "block",
              boxSizing: "border-box",
              padding: "8px 10px",
              background: "#111",
              color: "white",
              border: "1px solid #333",
              borderRadius: "8px",
            }}
          >
            {plotOptions.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>

          <div style={{ marginTop: "10px" }}>
            {plotLoading && <p>Loading...</p>}
            {plotError && <p style={{ color: "red" }}>{plotError}</p>}
            {!plotLoading && !plotError && plotData.length > 0 && supportedChartTypes.has(plotChartType) ? (
              <CustomPlot data={plotData} chartType={plotChartType} />
            ) : null}
            {!plotLoading && !plotError && plotData.length > 0 && !supportedChartTypes.has(plotChartType) ? renderTable(plotData) : null}
            {!plotLoading && !plotError && plotData.length === 0 ? <p>No data available</p> : null}
          </div>
        </div>

        <div style={{ flex: "1 1 520px", minWidth: "520px" }}>
          <div style={{ marginBottom: "10px", color: "#bbb", fontSize: "14px" }}>KPIs</div>
          <select
            value={kpiSelectedItem}
            onChange={(e) => setKpiSelectedItem(e.target.value)}
            style={{
              width: "100%",
              maxWidth: "380px",
              display: "block",
              boxSizing: "border-box",
              padding: "8px 10px",
              background: "#111",
              color: "white",
              border: "1px solid #333",
              borderRadius: "8px",
            }}
          >
            {kpiOptions.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>

          <div style={{ marginTop: "10px" }}>
            {kpiLoading && <p>Loading...</p>}
            {kpiError && <p style={{ color: "red" }}>{kpiError}</p>}
            {!kpiLoading && !kpiError && kpiData.length > 0 && supportedChartTypes.has(kpiChartType) ? (
              <CustomPlot data={kpiData} chartType={kpiChartType} />
            ) : null}
            {!kpiLoading && !kpiError && kpiData.length > 0 && !supportedChartTypes.has(kpiChartType) ? renderTable(kpiData) : null}
            {!kpiLoading && !kpiError && kpiData.length === 0 ? <p>No data available</p> : null}

            <div
              style={{
                marginTop: "12px",
                background: "#111",
                border: "1px solid #333",
                borderRadius: "12px",
                padding: "14px 16px",
              }}
            >
              <div style={{ color: "#bbb", fontSize: "13px", marginBottom: "8px" }}>Formula</div>
              <div
                style={{
                  color: "white",
                  whiteSpace: "pre-line",
                  fontSize: "13px",
                  lineHeight: 1.45,
                }}
              >
                {kpiFormulas[kpiSelectedItem] || "Formula not available"}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}