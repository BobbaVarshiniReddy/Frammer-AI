// import Plot from "react-plotly.js";

// export default function CustomPlot({ data, chartType }) {
//   if (!data || data.length === 0) return <p>No data to display</p>;

//   // ------------------ KPI Card ------------------
//   if (chartType === "kpi_card") {
//     const row = data[0] || {};
//     const value =
//       typeof row.value === "number"
//         ? row.value
//         : (() => {
//             const numericKeys = Object.keys(row).filter((k) => typeof row[k] === "number");
//             return numericKeys.length > 0 ? row[numericKeys[0]] : row.value;
//           })();

//     return (
//       <div
//         style={{
//           marginLeft: "20px",
//           backgroundColor: "#111",
//           display: "inline-block",
//           marginBottom: "20px",
//           padding: "18px 22px",
//           borderRadius: "12px",
//           border: "1px solid #333",
//           minWidth: "320px",
//         }}
//       >
//         <div style={{ color: "#bbb", fontSize: "14px", marginBottom: "6px" }}>
//           KPI
//         </div>
//         <div style={{ fontSize: "40px", color: "white", fontWeight: 700, lineHeight: 1.1 }}>
//           {value ?? "N/A"}
//         </div>
//       </div>
//     );
//   }

//   // ------------------ Table ------------------
//   if (!chartType) {
//     // Display only first 50 rows
//     const limitedData = data.slice(0, 50);
//     const headers = Object.keys(limitedData[0] || {});
//     return (
//       <div style={{ background: "#111", padding: "10px", borderRadius: "8px" }}>
//         <table style={{ width: "100%", color: "white", borderCollapse: "collapse" }}>
//           <thead>
//             <tr>
//               {headers.map((h) => (
//                 <th key={h} style={{ border: "1px solid #333", padding: "5px" }}>{h}</th>
//               ))}
//             </tr>
//           </thead>
//           <tbody>
//             {limitedData.map((row, idx) => (
//               <tr key={idx}>
//                 {headers.map((h) => (
//                   <td key={h} style={{ border: "1px solid #333", padding: "5px" }}>{row[h]}</td>
//                 ))}
//               </tr>
//             ))}
//           </tbody>
//         </table>
//         {data.length > 50 && <p style={{ color: "gray", marginTop: "5px" }}>Showing first 50 rows</p>}
//       </div>
//     );
//   }

//   // ------------------ Heatmap ------------------
//   if (chartType === "heatmap") {
//     const x = data.map((d) => d.x ?? "N/A");
//     const y = ["Completeness"];
//     const z = [data.map((d) => Number(d.value ?? 0))];

//     return (
//       <Plot
//         data={[{
//           x, y, z,
//           type: "heatmap",
//           colorscale: [[0, "rgb(0,0,0)"], [1, "rgb(255,71,87)"]],
//           showscale: true,
//           text: z[0].map((v) => `${v}%`),
//           hoverinfo: "x+text",
//         }]}
//         layout={{
//           title: "Data Completeness Heatmap",
//           autosize: false,
//           margin: { t: 50, l: 80, r: 50, b: 100 },
//           plot_bgcolor: "black",
//           paper_bgcolor: "black",
//           xaxis: { tickfont: { color: "white" } },
//           yaxis: { tickfont: { color: "white" }, automargin: true },
//           coloraxis: { colorbar: { tickfont: { color: "white" } } },
//         }}
//         style={{ width: "100%", height: "400px" }}
//       />
//     );
//   }

//   // ------------------ Bar / Line ------------------
//   const xKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "string");
//   const yKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "number");

//   const labels = xKeys.length > 0 ? data.map((d) => d[xKeys[0]]) : data.map((_, i) => i + 1);
//   const values = yKeys.length > 0 ? data.map((d) => Number(d[yKeys[0]])) : data.map(() => 0);

//   return (
//     <Plot
//       data={[{
//         x: labels,
//         y: values,
//         type: chartType === "line" ? "scatter" : "bar",
//         mode: chartType === "line" ? "lines+markers" : undefined,
//         marker: { color: "rgb(255,71,87)" },
//       }]}
//       layout={{
//         title: chartType.charAt(0).toUpperCase() + chartType.slice(1),
//         autosize: false,
//         width: "100%",
//         height: 400,
//         margin: { t: 50, l: 80, r: 50, b: 100 },
//         plot_bgcolor: "black",
//         paper_bgcolor: "black",
//         xaxis: { tickangle: -45, tickfont: { color: "white" } },
//         yaxis: { tickfont: { color: "white" }, automargin: true },
//         font: { color: "white" },
//       }}
//       style={{ width: "100%", height: "400px" }}
//     />
//   );
// }

import Plot from "react-plotly.js";

// ── Frammer Design Tokens ──────────────────────────────────────────────────
const COLORS = {
  primary : "#FF4649",
  white   : "#FFFFFF",
  bg      : "#0D0D0D",
  card    : "#111111",
  border  : "#2A2A2A",
  muted   : "#888888",
};

const HEATMAP_COLORSCALE = [
  [0.0,  "#7A0000"],
  [0.25, "#CC0000"],
  [0.5,  "#FF4649"],
  [0.75, "#FF9A9A"],
  [1.0,  "#FFD6D6"],
];

const BASE_LAYOUT = {
  plot_bgcolor : COLORS.bg,
  paper_bgcolor: COLORS.bg,
  font         : { color: COLORS.white, family: "Inter, sans-serif", size: 12 },
  margin       : { t: 50, l: 70, r: 30, b: 100 },
  hoverlabel   : {
    bgcolor    : "#1A1A1A",
    bordercolor: COLORS.primary,
    font       : { color: COLORS.white, size: 12 },
  },
  xaxis: {
    tickfont  : { color: COLORS.white, size: 11 },
    linecolor : COLORS.border,
    gridcolor : COLORS.border,
    tickangle : -45,
    automargin: true,
  },
  yaxis: {
    tickfont  : { color: COLORS.white, size: 11 },
    linecolor : COLORS.border,
    gridcolor : COLORS.border,
    automargin: true,
  },
};

export default function CustomPlot({ data, chartType, title }) {
  if (!data || data.length === 0)
    return (
      <div style={{
        color       : COLORS.muted,
        textAlign   : "center",
        padding     : "40px",
        background  : COLORS.card,
        borderRadius: "12px",
        border      : `1px solid ${COLORS.border}`,
        fontSize    : "14px",
      }}>
        No data to display
      </div>
    );

  // ── KPI Card ──────────────────────────────────────────────────────────────
  if (chartType === "kpi_card") {
    const row         = data[0] || {};
    const numericKeys = Object.keys(row).filter((k) => typeof row[k] === "number");
    const value       = numericKeys.length > 0 ? row[numericKeys[0]] : "N/A";
    const label       = numericKeys.length > 0 ? numericKeys[0] : "KPI";

    return (
      <div style={{
        backgroundColor: COLORS.card,
        display        : "inline-block",
        padding        : "24px 28px",
        borderRadius   : "14px",
        border         : `1px solid ${COLORS.border}`,
        minWidth       : "240px",
        margin         : "10px",
        boxShadow      : `0 0 20px rgba(255,70,73,0.08)`,
      }}>
        <div style={{
          height         : "3px",
          width          : "40px",
          backgroundColor: COLORS.primary,
          borderRadius   : "2px",
          marginBottom   : "14px",
        }} />
        <div style={{
          color        : COLORS.muted,
          fontSize     : "12px",
          marginBottom : "8px",
          letterSpacing: "0.08em",
          textTransform: "uppercase",
        }}>
          {label.replace(/_/g, " ")}
        </div>
        <div style={{
          fontSize  : "42px",
          color     : COLORS.white,
          fontWeight: 700,
          lineHeight: 1.1,
        }}>
          {typeof value === "number" ? value.toLocaleString() : value}
        </div>
      </div>
    );
  }

  // ── Table ─────────────────────────────────────────────────────────────────
  if (!chartType) {
    const limitedData = data.slice(0, 50);
    const headers     = Object.keys(limitedData[0] || {});

    return (
      <div style={{
        background  : COLORS.card,
        borderRadius: "12px",
        border      : `1px solid ${COLORS.border}`,
        overflow    : "auto",
      }}>
        <table style={{
          width          : "100%",
          color          : COLORS.white,
          borderCollapse : "collapse",
          fontSize       : "13px",
        }}>
          <thead>
            <tr style={{ backgroundColor: "#1A1A1A" }}>
              {headers.map((h) => (
                <th key={h} style={{
                  border       : `1px solid ${COLORS.border}`,
                  padding      : "10px 14px",
                  textAlign    : "left",
                  color        : COLORS.primary,
                  fontWeight   : 600,
                  letterSpacing: "0.05em",
                  fontSize     : "12px",
                  textTransform: "uppercase",
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {limitedData.map((row, idx) => (
              <tr key={idx} style={{
                backgroundColor: idx % 2 === 0 ? COLORS.card : "#151515",
              }}>
                {headers.map((h) => (
                  <td key={h} style={{
                    border : `1px solid ${COLORS.border}`,
                    padding: "8px 14px",
                  }}>{row[h]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {data.length > 50 && (
          <p style={{ color: COLORS.muted, padding: "8px 14px", fontSize: "12px" }}>
            Showing first 50 of {data.length} rows
          </p>
        )}
      </div>
    );
  }

  // ── AFTER ─────────────────────────────────────────────────────────────────
if (chartType === "heatmap") {

    const stringKeys  = Object.keys(data[0]).filter((k) => typeof data[0][k] === "string");
    const numericKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "number");

    const xKey    = stringKeys[stringKeys.length - 1];
    const yKey    = stringKeys[0];
    const zKey    = numericKeys[0];

    const xValues = [...new Set(data.map((d) => d[xKey] ?? "N/A"))];
    const yValues = [...new Set(data.map((d) => d[yKey] ?? "N/A"))];

    const z = yValues.map((yVal) =>
        xValues.map((xVal) => {
            const match = data.find(
                (d) => d[yKey] === yVal && d[xKey] === xVal
            );
            // ✅ Use 0 instead of null — renders as darkest red not black
            return match ? Number(match[zKey]) : 0;
        })
    );

    // ✅ Build custom text — show value or "N/A" for missing
    const text = yValues.map((yVal) =>
        xValues.map((xVal) => {
            const match = data.find(
                (d) => d[yKey] === yVal && d[xKey] === xVal
            );
            return match ? `${Number(match[zKey]).toFixed(1)}%` : "—";
        })
    );

    return (
        <Plot
            data={[{
                type         : "heatmap",
                x            : xValues,
                y            : yValues,
                z            : z,
                colorscale   : HEATMAP_COLORSCALE,
                showscale    : true,
                hoverongaps  : false,
                text         : text,
                texttemplate : "%{text}",
                textfont     : { size: 9, color: COLORS.white },
                hovertemplate: `<b>${yKey}:</b> %{y}<br><b>${xKey}:</b> %{x}<br><b>Value:</b> %{text}<extra></extra>`,
                zmin         : 0,      // ✅ Anchor colorscale at 0
                colorbar: {
                    tickfont : { color: COLORS.white },
                    titlefont: { color: COLORS.white },
                    title    : "%",
                },
            }]}
            layout={{
                ...BASE_LAYOUT,
                title : { text: title || "Heatmap", font: { color: COLORS.primary, size: 14 } },
                margin: { t: 60, l: 160, r: 50, b: 80 },
                xaxis : { ...BASE_LAYOUT.xaxis, tickangle: 0 },
                yaxis : { ...BASE_LAYOUT.yaxis, tickangle: 0 },
                height: Math.max(400, yValues.length * 35 + 120),
            }}
            style={{ width: "100%" }}
            config={{ responsive: true, displayModeBar: false }}
        />
    );
}

  // ── Grouped Bar ───────────────────────────────────────────────────────────
  if (chartType === "grouped_bar") {
    const xKey  = Object.keys(data[0]).find((k) => typeof data[0][k] === "string");
    const yKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "number");

    return (
      <Plot
        data={yKeys.map((key) => ({
          type         : "bar",
          name         : key.replace(/_/g, " "),
          x            : data.map((d) => d[xKey]),
          y            : data.map((d) => Number(d[key])),
          marker       : { color: COLORS.primary },
          hovertemplate: `<b>%{x}</b><br>${key.replace(/_/g, " ")}: %{y}<extra></extra>`,
        }))}
        layout={{
          ...BASE_LAYOUT,
          title  : { text: title || "Grouped Bar", font: { color: COLORS.primary, size: 14 } },
          barmode: "group",
          legend : {
            font       : { color: COLORS.white },
            bgcolor    : "#1A1A1A",
            bordercolor: COLORS.border,
            borderwidth: 1,
          },
          height: 450,
        }}
        style={{ width: "100%" }}
        config={{ responsive: true, displayModeBar: false }}
      />
    );
  }

  // ── Small Multiples ───────────────────────────────────────────────────────
  if (chartType === "small_multiples") {
    const stringKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "string");
    const groupKey   = stringKeys[0];
    const subKey     = stringKeys[1];
    const valueKey   = Object.keys(data[0]).find((k) => typeof data[0][k] === "number");
    const groups     = [...new Set(data.map((d) => d[groupKey]))];

    return (
      <div style={{
        display            : "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(250px, 1fr))",
        gap                : "16px",
        padding            : "8px",
      }}>
        {groups.map((group) => {
          const sub = data.filter((d) => d[groupKey] === group);
          return (
            <div key={group} style={{
              background  : COLORS.card,
              borderRadius: "10px",
              border      : `1px solid ${COLORS.border}`,
              padding     : "8px",
            }}>
              <div style={{
                color       : COLORS.primary,
                fontWeight  : 700,
                fontSize    : "13px",
                marginBottom: "6px",
                paddingLeft : "4px",
              }}>
                {groupKey === "Channel" ? `Channel ${group}` : group}
              </div>
              <Plot
                data={[{
                  type         : "bar",
                  x            : sub.map((d) => d[subKey] ?? d[groupKey]),
                  y            : sub.map((d) => Number(d[valueKey])),
                  marker       : { color: COLORS.primary },
                  text         : sub.map((d) => `${Number(d[valueKey]).toFixed(1)}`),
                  textposition : "outside",
                  textfont     : { color: COLORS.white, size: 9 },
                  hovertemplate: `<b>%{x}</b><br>${valueKey.replace(/_/g, " ")}: %{y:.1f}<extra></extra>`,
                }]}
                layout={{
                  ...BASE_LAYOUT,
                  margin    : { t: 10, l: 40, r: 10, b: 80 },
                  height    : 200,
                  showlegend: false,
                  xaxis     : { ...BASE_LAYOUT.xaxis, tickangle: -30, tickfont: { color: COLORS.white, size: 8 } },
                  yaxis     : { ...BASE_LAYOUT.yaxis, tickfont: { color: COLORS.white, size: 8 } },
                }}
                style={{ width: "100%" }}
                config={{ responsive: true, displayModeBar: false }}
              />
            </div>
          );
        })}
      </div>
    );
  }

  // ── Bar / Line ────────────────────────────────────────────────────────────
  const xKey  = Object.keys(data[0]).find((k) => typeof data[0][k] === "string");
  const yKeys = Object.keys(data[0]).filter((k) => typeof data[0][k] === "number");
  const isLine = chartType === "line";

  return (
    <Plot
      data={yKeys.map((key) => ({
        x            : data.map((d) => (xKey ? d[xKey] : "")),
        y            : data.map((d) => Number(d[key])),
        type         : isLine ? "scatter" : "bar",
        mode         : isLine ? "lines+markers" : undefined,
        name         : key.replace(/_/g, " "),
        marker       : { color: COLORS.primary, size: isLine ? 7 : undefined },
        line         : isLine ? { color: COLORS.primary, width: 2.5 } : undefined,
        hovertemplate: `<b>%{x}</b><br>${key.replace(/_/g, " ")}: %{y}<extra></extra>`,
      }))}
      layout={{
        ...BASE_LAYOUT,
        title : { text: title || "", font: { color: COLORS.primary, size: 14 } },
        height: 420,
        legend: {
          font       : { color: COLORS.white },
          bgcolor    : "#1A1A1A",
          bordercolor: COLORS.border,
        },
      }}
      style={{ width: "100%" }}
      config={{ responsive: true, displayModeBar: false }}
    />
  );
}