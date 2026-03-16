import { useState } from "react";
import "./multidim.css";

function MultiDimension() {

  const [dimension1, setDimension1] = useState("");
  const [dimension2, setDimension2] = useState("");

  const getGraph = () => {

    if (dimension1 === "Channel" && dimension2 === "Input Type") {
      return "/graphs/channel_input.png";
    }

    if (dimension1 === "Channel" && dimension2 === "Language") {
      return "/graphs/channel_language.png";
    }

    if (dimension1 === "User" && dimension2 === "Output Type") {
      return "/graphs/user_output.png";
    }

    if (dimension1 === "Team" && dimension2 === "Platform") {
      return "/graphs/team_platform.png";
    }

    if (dimension1 === "Channel" && dimension2 === "Published Status") {
      return "/graphs/channel_publish.png";
    }

    return null;
  };

  return (
    <div className="multi-container">

      <h1>Multi-Dimension Analysis</h1>

      {/* Dimension selectors */}

      <div className="dimension-select">

        <select
          value={dimension1}
          onChange={(e) => setDimension1(e.target.value)}
        >
          <option value="">Select Dimension 1</option>
          <option>Channel</option>
          <option>User</option>
          <option>Team</option>
        </select>

        <select
          value={dimension2}
          onChange={(e) => setDimension2(e.target.value)}
        >
          <option value="">Select Dimension 2</option>
          <option>Input Type</option>
          <option>Language</option>
          <option>Output Type</option>
          <option>Platform</option>
          <option>Published Status</option>
        </select>

      </div>

      {/* Graph display */}

      <div className="graph-area">

        {getGraph() && (
          <img
            src={getGraph()}
            alt="dimension graph"
            className="graph-image"
          />
        )}

      </div>

    </div>
  );
}

export default MultiDimension;