import { useState } from "react";
import "./App.css";
import MultiDimension from "./tabs/multidim";
import ExecutiveSummary from "./tabs/ExecutiveSummary";
import UsageTrends from "./tabs/UsageTrends";
import ClientChannel from "./tabs/ClientChannel";
import Funnel from "./tabs/Funnel";
import VideoExplorer from "./tabs/VideoExplorer";
import FrammerChatbot from "./FrammerChatbot";

function App() {
  const [activeTab, setActiveTab] = useState("executive");

  return (
    <div className="app">
      <div className="navbar">
        <img
          className="logo"
          src="https://framerusercontent.com/images/RsoFE6JlucifteONYuSPCIt0A8.png"
          alt="Frammer AI Logo"
        />
        <div className="tabs">
          <button onClick={() => setActiveTab("executive")}>Executive Summary</button>
          <button onClick={() => setActiveTab("usage")}>Usage & Trends</button>
          <button onClick={() => setActiveTab("client")}>Client & Channel Analysis</button>
          <button onClick={() => setActiveTab("funnel")}>Type & Funnel</button>
          <button onClick={() => setActiveTab("video")}>Video Explorer</button>
          <button onClick={() => setActiveTab("dimension")}>Multi Dimension</button>
        </div>
      </div>

      {/* Executive Summary — chatbot sidebar on the right */}
      {activeTab === "executive" && (
        <div className="executive-layout">
          <div className="executive-content">
            <ExecutiveSummary />
          </div>
          <FrammerChatbot />
        </div>
      )}

      {activeTab === "usage"     && <UsageTrends />}
      {activeTab === "client"    && <ClientChannel />}
      {activeTab === "funnel"    && <Funnel />}
      {activeTab === "video"     && <VideoExplorer />}
      {activeTab === "dimension" && <MultiDimension />}
    </div>
  );
}

export default App;