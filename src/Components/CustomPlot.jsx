// import React from "react";
// import Plot from "react-plotly.js";

// export default function CustomPlot({ data, chartType }) {
//   if (!data || data.length === 0) return <p>No data to display</p>;

//   // ------------------ Heatmap ------------------
//   if (chartType === "heatmap") {
//     const x = data.map((d) => d.x ?? "N/A"); // column names
//     const y = ["Completeness"]; // single row
//     const z = [data.map((d) => Number(d.value ?? 0))]; // 2D array

//     return (
//       <div
//         style={{
//           marginLeft: "20px",        // shift container right from page edge
//           backgroundColor: "black",
//           display: "inline-block",
//           marginBottom: "20px",
//           paddingLeft: "10px",       // optional inner padding
//         }}
//       >
//         <Plot
//           data={[
//             {
//               x,
//               y,
//               z,
//               type: "heatmap",
//               colorscale: [
//                 [0, "rgb(0,0,0)"],
//                 [1, "rgb(255,71,87)"],
//               ],
//               showscale: true,
//               text: z[0].map((v) => `${v}%`),
//               hoverinfo: "x+text",
//             },
//           ]}
//           layout={{
//             title: "Data Completeness Heatmap",
//             autosize: false,
//             width: 600,
//             height: 400,
//             margin: { t: 50, l: 80, r: 50, b: 100 },
//             plot_bgcolor: "black",
//             paper_bgcolor: "black",
//             xaxis: { tickfont: { color: "white" } },
//             yaxis: { tickfont: { color: "white" }, automargin: true },
//             coloraxis: { colorbar: { tickfont: { color: "white" } } },
//             font: { color: "white" },
//           }}
//           style={{ width: "100%", height: "400px" }}
//           config={{
//             displayModeBar: true,
//             modeBarButtonsToRemove: ["resetScale2d", "autoScale2d", "pan2d"],
//           }}
//         />
//       </div>
//     );
//   }

//   // ------------------ Bar / Line ------------------
//   const xKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "string");
//   const yKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "number");

//   const labels =
//     xKeys.length > 0
//       ? data.map((d, i) => d[xKeys[0]] ?? `Row ${i + 1}`)
//       : data.map((_, idx) => idx + 1);

//   const values =
//     yKeys.length > 0
//       ? data.map((d) => Number(d[yKeys[0]] ?? 0))
//       : data.map(() => 0);

//   const plotData = [
//     {
//       x: labels,
//       y: values,
//       type: chartType === "line" ? "scatter" : "bar",
//       mode: chartType === "line" ? "lines+markers" : undefined,
//       marker: { color: "rgb(255,71,87)" },
//       line: chartType === "line" ? { color: "rgb(255,71,87)" } : undefined,
//     },
//   ];

//   return (
//     <div
//       style={{
//         marginLeft: "20px",        // shift container right from page edge
//         backgroundColor: "black",
//         display: "inline-block",
//         marginBottom: "20px",
//         paddingLeft: "10px",       // optional inner padding
//       }}
//     >
//       <Plot
//         data={plotData}
//         layout={{
//           title: chartType.charAt(0).toUpperCase() + chartType.slice(1) + " Chart",
//           autosize: false,
//           width: 600,
//           height: 400,
//           margin: { t: 50, l: 80, r: 50, b: 100 },
//           plot_bgcolor: "black",
//           paper_bgcolor: "black",
//           xaxis: { tickangle: -45, tickfont: { color: "white" } },
//           yaxis: { tickfont: { color: "white" }, automargin: true },
//           font: { color: "white" },
//         }}
//         style={{ width: "100%", height: "400px" }}
//         config={{
//           displayModeBar: true,
//           modeBarButtonsToRemove: ["resetScale2d", "autoScale2d", "pan2d"],
//         }}
//       />
//     </div>
//   );
// }
// src/components/CustomPlot.jsx
// src/components/CustomPlot.jsx
// src/components/CustomPlot.jsx
import React from "react";
import Plot from "react-plotly.js";

export default function CustomPlot({ data, chartType }) {
  if (!data || data.length === 0) return <p>No data to display</p>;

  // ------------------ KPI Card ------------------
  if (chartType === "kpi_card") {
    const row = data[0] || {};
    const value =
      typeof row.value === "number"
        ? row.value
        : (() => {
            const numericKeys = Object.keys(row).filter((k) => typeof row[k] === "number");
            return numericKeys.length > 0 ? row[numericKeys[0]] : row.value;
          })();

    return (
      <div
        style={{
          marginLeft: "20px",
          backgroundColor: "#111",
          display: "inline-block",
          marginBottom: "20px",
          padding: "18px 22px",
          borderRadius: "12px",
          border: "1px solid #333",
          minWidth: "320px",
        }}
      >
        <div style={{ color: "#bbb", fontSize: "14px", marginBottom: "6px" }}>
          KPI
        </div>
        <div style={{ fontSize: "40px", color: "white", fontWeight: 700, lineHeight: 1.1 }}>
          {value ?? "N/A"}
        </div>
      </div>
    );
  }

  // ------------------ Table ------------------
  if (!chartType) {
    // Display only first 50 rows
    const limitedData = data.slice(0, 50);
    const headers = Object.keys(limitedData[0] || {});
    return (
      <div style={{ background: "#111", padding: "10px", borderRadius: "8px" }}>
        <table style={{ width: "100%", color: "white", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              {headers.map((h) => (
                <th key={h} style={{ border: "1px solid #333", padding: "5px" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {limitedData.map((row, idx) => (
              <tr key={idx}>
                {headers.map((h) => (
                  <td key={h} style={{ border: "1px solid #333", padding: "5px" }}>{row[h]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {data.length > 50 && <p style={{ color: "gray", marginTop: "5px" }}>Showing first 50 rows</p>}
      </div>
    );
  }

  // ------------------ Heatmap ------------------
  if (chartType === "heatmap") {
    const x = data.map((d) => d.x ?? "N/A");
    const y = ["Completeness"];
    const z = [data.map((d) => Number(d.value ?? 0))];

    return (
      <Plot
        data={[{
          x, y, z,
          type: "heatmap",
          colorscale: [[0, "rgb(0,0,0)"], [1, "rgb(255,71,87)"]],
          showscale: true,
          text: z[0].map((v) => `${v}%`),
          hoverinfo: "x+text",
        }]}
        layout={{
          title: "Data Completeness Heatmap",
          autosize: false,
          margin: { t: 50, l: 80, r: 50, b: 100 },
          plot_bgcolor: "black",
          paper_bgcolor: "black",
          xaxis: { tickfont: { color: "white" } },
          yaxis: { tickfont: { color: "white" }, automargin: true },
          coloraxis: { colorbar: { tickfont: { color: "white" } } },
        }}
        style={{ width: "100%", height: "400px" }}
      />
    );
  }

  // ------------------ Bar / Line ------------------
  const xKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "string");
  const yKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "number");

  const labels = xKeys.length > 0 ? data.map((d) => d[xKeys[0]]) : data.map((_, i) => i + 1);
  const values = yKeys.length > 0 ? data.map((d) => Number(d[yKeys[0]])) : data.map(() => 0);

  return (
    <Plot
      data={[{
        x: labels,
        y: values,
        type: chartType === "line" ? "scatter" : "bar",
        mode: chartType === "line" ? "lines+markers" : undefined,
        marker: { color: "rgb(255,71,87)" },
      }]}
      layout={{
        title: chartType.charAt(0).toUpperCase() + chartType.slice(1),
        autosize: false,
        width: "100%",
        height: 400,
        margin: { t: 50, l: 80, r: 50, b: 100 },
        plot_bgcolor: "black",
        paper_bgcolor: "black",
        xaxis: { tickangle: -45, tickfont: { color: "white" } },
        yaxis: { tickfont: { color: "white" }, automargin: true },
        font: { color: "white" },
      }}
      style={{ width: "100%", height: "400px" }}
    />
  );
}