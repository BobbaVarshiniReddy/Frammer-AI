import "./ExecutiveSummary.css";

function ExecutiveSummary() {

  return (
    <div className="dashboard">

      <h1 className="title">Executive Summary</h1>

      <div className="kpi-grid">

        <div className="card">
          <p>Uploaded Videos</p>
          <div className="bar"></div>
        </div>

        <div className="card">
          <p>Processed Videos</p>
          <div className="bar"></div>
        </div>

        <div className="card">
          <p>Published Videos</p>
          <div className="bar"></div>
        </div>

        <div className="card">
          <p>Publish Conversion</p>
          <div className="bar"></div>
        </div>

      </div>

      <div className="lower-section">

        <div className="trend-box">

          <h3>Upload & Publish Trend</h3>

          <div className="fake-chart">

            <div style={{height:"40px"}}></div>
            <div style={{height:"70px"}}></div>
            <div style={{height:"60px"}}></div>
            <div style={{height:"80px"}}></div>
            <div style={{height:"50px"}}></div>
            <div style={{height:"90px"}}></div>

          </div>

        </div>

        <div className="insights">

          <h3>Key Insights</h3>

          <ul>
            <li>Insight 1</li>
            <li>Insight 2</li>
            <li>Insight 3</li>
            <li>Insight 4</li>
          </ul>

        </div>

      </div>

    </div>
  );
}

export default ExecutiveSummary;