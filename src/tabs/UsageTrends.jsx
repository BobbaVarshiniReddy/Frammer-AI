import React, { useEffect, useState } from "react";
import CustomPlot from "../Components/CustomPlot";

export default function UsageTrends() {
  const plotOptions = [
    { value: "uploaded_duration_vs_month", label: "Uploaded Duration vs Month (Hours)" },
    { value: "created_duration_vs_month", label: "Created Duration vs Month (Hours)" },
    { value: "published_duration_vs_month", label: "Published Duration vs Month (Hours)" },
    { value: "top5_uploaded_duration", label: "Top 5 Uploaded Months (Hours)" },
    { value: "top5_published_duration", label: "Top 5 Published Months (Hours)" },
  ];

  const kpiOptions = [
    { value: "total_content_hours_processed", label: "Total Content Hours Processed" },
    { value: "monthly_created_duration_trend", label: "Monthly Created Duration Trend" },
    { value: "mom_duration_growth", label: "MoM Duration Growth (%)" },
  ];

  const kpiFormulas = {
    total_content_hours_processed:
      "Total Content Hours (hours)\n= SUM(Total Created Duration converted to hours)\n(hours = hh + mm/60 + ss/3600)",
    monthly_created_duration_trend:
      "Monthly Created Duration (hours)\n= SUM(Total Created Duration converted to hours) per Month",
    mom_duration_growth:
      "MoM Duration Growth (%)\n= ((This Month Hours - Last Month Hours) / Last Month Hours) × 100\n(where Month Hours = SUM(Total Created Duration converted to hours))",
  };

  const [plotSelectedItem, setPlotSelectedItem] = useState(plotOptions[0].value);
  const [kpiSelectedItem, setKpiSelectedItem] = useState(kpiOptions[0].value);

  const [plotData, setPlotData] = useState([]);
  const [plotChartType, setPlotChartType] = useState("line");
  const [plotLoading, setPlotLoading] = useState(false);
  const [plotError, setPlotError] = useState(null);

  const [kpiData, setKpiData] = useState([]);
  const [kpiChartType, setKpiChartType] = useState("bar");
  const [kpiLoading, setKpiLoading] = useState(false);
  const [kpiError, setKpiError] = useState(null);

  useEffect(() => {
    setPlotLoading(true);
    setPlotError(null);
    fetch(`http://127.0.0.1:8000/monthly/plot/custom_plot?plot_name=${plotSelectedItem}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) {
          setPlotError(resData.error);
          setPlotData([]);
          setPlotChartType("bar");
        } else {
          setPlotData(resData.data || []);
          setPlotChartType(resData.chart_type || "bar");
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
    fetch(`http://127.0.0.1:8000/monthly/kpi/${kpiSelectedItem}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) {
          setKpiError(resData.error);
          setKpiData([]);
          setKpiChartType("bar");
        } else {
          setKpiData(resData.data || []);
          setKpiChartType(resData.chart_type || "bar");
        }
        setKpiLoading(false);
      })
      .catch((err) => {
        setKpiError(err.message);
        setKpiLoading(false);
      });
  }, [kpiSelectedItem]);

  return (
    <div className="client-dashboard">
      <h2 className="title">Usage & Trends</h2>

      <div style={{ display: "flex", gap: "18px", alignItems: "flex-start", flexWrap: "wrap" }}>
        <div style={{ flex: "1 1 520px", minWidth: "520px" }}>
          <div style={{ marginBottom: "10px", color: "#bbb", fontSize: "14px" }}>Plots</div>
          <select
            value={plotSelectedItem}
            onChange={(e) => setPlotSelectedItem(e.target.value)}
            style={{
              width: "100%",
              maxWidth: "380px",
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
            {!plotLoading && !plotError && plotData.length > 0 && (
              <CustomPlot data={plotData} chartType={plotChartType} />
            )}
            {!plotLoading && !plotError && plotData.length === 0 && <p>No data available</p>}
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
            {!kpiLoading && !kpiError && kpiData.length > 0 && (
              <CustomPlot data={kpiData} chartType={kpiChartType} />
            )}
            {!kpiLoading && !kpiError && kpiData.length === 0 && <p>No data available</p>}

            <div
              style={{
                marginTop: "12px",
                background: "#111",
                border: "1px solid #333",
                borderRadius: "12px",
                padding: "14px 16px",
              }}
            >
              <div style={{ color: "#bbb", fontSize: "13px", marginBottom: "8px" }}>
                Formula
              </div>
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

 