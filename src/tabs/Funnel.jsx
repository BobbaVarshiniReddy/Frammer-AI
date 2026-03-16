import "./Funnel.css";

function Funnel() {

  return (
    <div className="funnel-dashboard">

      <h1 className="title">Type & Funnel</h1>

      {/* Funnel Section */}

      <div className="funnel-grid">

        {/* Publishing Funnel */}

        <div className="funnel-card">

          <h3>Processing → Publishing Funnel</h3>

          <div className="funnel-steps">

            <div className="step upload">
              <p>Uploaded</p>
            </div>

            <div className="step process">
              <p>Processed</p>
            </div>

            <div className="step publish">
              <p>Published</p>
            </div>

          </div>

        </div>


        {/* Content Type Distribution */}

        <div className="funnel-card">

          <h3>Content Type Distribution</h3>

          <div className="type-list">

            <div className="type-item">
              <span>Shorts</span>
              <div className="type-bar" style={{width:"80%"}}></div>
            </div>

            <div className="type-item">
              <span>Long Videos</span>
              <div className="type-bar" style={{width:"60%"}}></div>
            </div>

            <div className="type-item">
              <span>Reels</span>
              <div className="type-bar" style={{width:"70%"}}></div>
            </div>

            <div className="type-item">
              <span>Clips</span>
              <div className="type-bar" style={{width:"50%"}}></div>
            </div>

          </div>

        </div>

      </div>

    </div>
  );
}

export default Funnel;