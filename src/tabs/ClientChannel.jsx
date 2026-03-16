import "./ClientChannel.css";

function ClientChannel() {

  return (
    <div className="client-dashboard">

      <h1 className="title">Client & Channel Analysis</h1>

      {/* Filters */}
      <div className="filters">

        <select>
          <option>Select Client</option>
          <option>Client A</option>
          <option>Client B</option>
        </select>

        <select>
          <option>Select Channel</option>
          <option>YouTube</option>
          <option>Instagram</option>
          <option>TikTok</option>
        </select>

        <select>
          <option>Select Region</option>
          <option>India</option>
          <option>USA</option>
        </select>

      </div>

      {/* Charts Section */}
      <div className="analysis-grid">

        <div className="analysis-card">
          <h3>Channel Performance</h3>

          <div className="fake-chart">

            <div style={{height:"60px"}}></div>
            <div style={{height:"90px"}}></div>
            <div style={{height:"70px"}}></div>
            <div style={{height:"100px"}}></div>

          </div>

        </div>

        <div className="analysis-card">

          <h3>Top Performing Channels</h3>

          <ul className="channel-list">
            <li>YouTube</li>
            <li>Instagram</li>
            <li>TikTok</li>
            <li>LinkedIn</li>
          </ul>

        </div>

      </div>

    </div>
  );
}

export default ClientChannel;