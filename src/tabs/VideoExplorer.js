import React, { useState } from "react";
import "./VideoExplorer.css";

const videos = [
  {
    id: 1,
    title: "Election Interview Highlights",
    channel: "Politics Channel",
    duration: "0:45",
    type: "Reel",
    language: "Hindi",
    platform: "Instagram",
    status: "Published",
    thumbnail: "https://img.youtube.com/vi/tgbNymZ7vqY/0.jpg",
  },
  {
    id: 2,
    title: "Cricket Match Analysis",
    channel: "Sports Channel",
    duration: "1:10",
    type: "Short",
    language: "English",
    platform: "YouTube",
    status: "Not Published",
    thumbnail: "https://img.youtube.com/vi/tgbNymZ7vqY/0.jpg",
  },
  {
    id: 3,
    title: "AI Conference Speech",
    channel: "Tech Channel",
    duration: "2:05",
    type: "Summary",
    language: "English",
    platform: "YouTube",
    status: "Published",
    thumbnail: "https://img.youtube.com/vi/tgbNymZ7vqY/0.jpg",
  },
];

export default function VideoExplorer() {
  const [search, setSearch] = useState("");
  const [gridView, setGridView] = useState(true);
  const [typeFilter, setTypeFilter] = useState("All");

  const filteredVideos = videos.filter((video) => {
    const matchesSearch = video.title
      .toLowerCase()
      .includes(search.toLowerCase());

    const matchesType =
      typeFilter === "All" || video.type === typeFilter;

    return matchesSearch && matchesType;
  });

  return (
    <div className="videoExplorerContainer">

      <h1 className="pageTitle">Video Explorer</h1>

      {/* Top Controls */}
      <div className="explorerControls">

        <input
          type="text"
          placeholder="Search videos..."
          className="searchInput"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />

        <select
          className="filterSelect"
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
        >
          <option value="All">All Types</option>
          <option value="Reel">Reels</option>
          <option value="Short">Shorts</option>
          <option value="Summary">Summary</option>
        </select>

        <div className="viewToggle">
          <button
            className={gridView ? "activeBtn" : ""}
            onClick={() => setGridView(true)}
          >
            Grid
          </button>

          <button
            className={!gridView ? "activeBtn" : ""}
            onClick={() => setGridView(false)}
          >
            List
          </button>
        </div>

      </div>

      {/* Video Section */}

      <div className={gridView ? "videoGrid" : "videoList"}>

        {filteredVideos.map((video) => (
          <div key={video.id} className="videoCard">

            <img
              src={video.thumbnail}
              alt="thumbnail"
              className="thumbnail"
            />

            <div className="videoDetails">

              <h3>{video.title}</h3>

              <p className="channelName">{video.channel}</p>

              <div className="videoMeta">

                <span>{video.duration}</span>

                <span>{video.type}</span>

                <span>{video.language}</span>

                <span className={
                  video.status === "Published"
                    ? "published"
                    : "notPublished"
                }>
                  {video.status}
                </span>

              </div>

            </div>

          </div>
        ))}

      </div>

    </div>
  );
}