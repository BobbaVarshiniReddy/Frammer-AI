import "./ExecutiveSummary.css";

function ExecutiveSummary() {
  return (
    <div className="dashboard">

      <h1 className="title">Executive Summary</h1>

      {/* ---------------- KPI CARDS ---------------- */}
      <div className="kpi-grid">

        <div className="card">
          <p>Total Uploads</p>
          <h2>4,453</h2>
        </div>

        <div className="card">
          <p>Total Created</p>
          <h2>14,916</h2>
        </div>

        <div className="card">
          <p>Total Published</p>
          <h2>111</h2>
        </div>

        <div className="card">
          <p>Amplification</p>
          <h2>3.35×</h2>
        </div>

        <div className="card">
          <p>Publish Rate</p>
          <h2>0.74%</h2>
        </div>

        <div className="card">
          <p>Channels</p>
          <h2>18</h2>
        </div>

        <div className="card">
          <p>Zero Publish Channels</p>
          <h2>13 (72%)</h2>
        </div>

        <div className="card">
          <p>Total Users</p>
          <h2>44</h2>
        </div>

        <div className="card">
          <p>Top User Uploads</p>
          <h2>489</h2>
        </div>

        <div className="card">
          <p>Content Hours</p>
          <h2>~1054 hrs</h2>
        </div>

      </div>

      {/* ---------------- LOWER SECTION ---------------- */}
      <div className="lower-section">

        {/* -------- Summary Box -------- */}
        <div className="trend-box">

          <h3>Overall Summary</h3>

          <div className="summary-content">
            <p><strong>Uploads:</strong> 4,453 videos uploaded</p>
            <p><strong>Created:</strong> 14,916 content pieces generated</p>
            <p><strong>Published:</strong> Only 111 made it live</p>
            <p><strong>Conversion:</strong> Very low publish rate (0.74%)</p>
          </div>

        </div>

        {/* -------- Insights -------- */}
        <div className="insights">

          <h3>Key Insights</h3>

          <ul>
            <li>Only <b>0.74%</b> of created content gets published</li>
            <li><b>72%</b> channels are inactive (zero publish)</li>
            <li>High amplification (3.35×) but poor conversion</li>
            <li>A few users dominate uploads (top = 489)</li>
            <li>Over <b>1000+ hours</b> of content processed</li>
          </ul>

        </div>

      </div>

    </div>
  );
}

export default ExecutiveSummary;