// import React, { useState, useEffect } from "react";
// import CustomPlot from "../Components/CustomPlot";

// export default function ClientChannel() {
//   const [plotName, setPlotName] = useState("published_vs_content_type");
//   const [plotData, setPlotData] = useState([]);
//   const [plotType, setPlotType] = useState("bar");
//   const [loading, setLoading] = useState(false);
//   const [error, setError] = useState(null);

//   const plotOptions = [
//     { value: "published_vs_content_type", label: "Published vs Content Type" },
//     { value: "published_vs_team", label: "Published vs Team" },
//     { value: "data_completeness_heatmap", label: "Data Completeness Heatmap" },
//     { value: "top_uploaders", label: "Top Uploaders" },
//     { value: "published_pct_by_type", label: "Published % by Type" },
//     { value: "published_pct_by_uploader", label: "Published % by User" },
//   ];

//   useEffect(() => {
//     setLoading(true);
//     setError(null);

//     fetch(`http://127.0.0.1:8000/plot/custom_plot?plot_name=${plotName}`)
//       .then((res) => res.json())
//       .then((data) => {
//         if (data.error) {
//           setError(data.error);
//           setPlotData([]);
//         } else {
//           setPlotData(data.data || []);
//           setPlotType(data.chart_type || "bar");
//         }
//         setLoading(false);
//       })
//       .catch((err) => {
//         setError(err.message);
//         setLoading(false);
//       });
//   }, [plotName]);

//   return (
//     <div className="client-dashboard">
//       <h2 className="title">Interactive Graphs</h2>

//       <div style={{ marginBottom: "16px" }}>
//         <label>Select Plot: </label>
//         <select value={plotName} onChange={(e) => setPlotName(e.target.value)}>
//           {plotOptions.map((opt) => (
//             <option key={opt.value} value={opt.value}>
//               {opt.label}
//             </option>
//           ))}
//         </select>
//       </div>

//       {loading && <p>Loading plot...</p>}
//       {error && <p style={{ color: "red" }}>{error}</p>}
//       {!loading && !error && plotData.length > 0 && (
//         <CustomPlot data={plotData} chartType={plotType} />
//       )}
//       {!loading && !error && plotData.length === 0 && <p>No data for this plot</p>}
//     </div>
//   );
// }
// src/pages/ClientChannel.jsx
import React, { useState, useEffect } from "react";
import CustomPlot from "../Components/CustomPlot";

export default function ClientChannel() {
  const plotOptions = [
    { value: "published_vs_content_type", label: "Published vs Content Type" },
    { value: "published_vs_team", label: "Published vs Team" },
    { value: "data_completeness_heatmap", label: "Data Completeness Heatmap" },
    { value: "top_uploaders", label: "Top Uploaders" },
    { value: "published_pct_by_type", label: "Published % by Type" },
    { value: "published_pct_by_uploader", label: "Published % by User" },
  ];

  const kpiOptions = [
    { value: "platform_distribution", label: "Published Platform Distribution" },
    { value: "published_rate_per_uploader", label: "Published Rate per Uploader" },
    { value: "top_uploaders", label: "Top Uploaders KPI" },
    { value: "unknown_team_rate", label: "Unknown Team Rate" },
    { value: "published_url_completeness", label: "Published URL Completeness" },
    { value: "overall_published_rate", label: "Overall Published Rate" },
    { value: "duplicate_video_id_count", label: "Duplicate Video ID Count" },
  ];

  const [plotSelectedItem, setPlotSelectedItem] = useState(plotOptions[0].value);
  const [plotData, setPlotData] = useState([]);
  const [plotChartType, setPlotChartType] = useState("bar");
  const [plotLoading, setPlotLoading] = useState(false);
  const [plotError, setPlotError] = useState(null);

  const [kpiSelectedItem, setKpiSelectedItem] = useState(kpiOptions[0].value);
  const [kpiData, setKpiData] = useState([]);
  const [kpiChartType, setKpiChartType] = useState("bar");
  const [kpiLoading, setKpiLoading] = useState(false);
  const [kpiError, setKpiError] = useState(null);

  useEffect(() => {
    setPlotLoading(true);
    setPlotError(null);
    fetch(`http://127.0.0.1:8000/plot/custom_plot?plot_name=${plotSelectedItem}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) {
          setPlotError(resData.error);
          setPlotData([]);
        } else {
          setPlotData(resData.data || []);
          setPlotChartType(resData.chart_type);
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
    fetch(`http://127.0.0.1:8000/kpi/${kpiSelectedItem}`)
      .then((res) => res.json())
      .then((resData) => {
        if (resData.error) {
          setKpiError(resData.error);
          setKpiData([]);
        } else {
          setKpiData(resData.data || []);
          setKpiChartType(resData.chart_type);
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
      <h2 className="title">Interactive Graphs & KPIs</h2>

      <div style={{ display: "flex", gap: "18px", alignItems: "flex-start", flexWrap: "wrap" }}>
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
            {!kpiLoading && !kpiError && kpiData.length > 0 && (
              <CustomPlot data={kpiData} chartType={kpiChartType} />
            )}
            {!kpiLoading && !kpiError && kpiData.length === 0 && <p>No data available</p>}
          </div>
        </div>
      </div>
    </div>
  );
}