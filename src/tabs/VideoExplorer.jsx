import React, { useState } from "react";
import "./VideoExplorer.css";
import { BsGrid3X3Gap, BsGrid1X2, BsGrid } from "react-icons/bs";

const sampleVideos = [
  {
    id: 1,
    title: "Election Interview Highlights",
    channel: "Politics",
    duration: "0:45",
    type: "Reel",
    language: "Hindi",
    status: "Published",
    thumbnail: "https://picsum.photos/400/200?1"
  },
  {
    id: 2,
    title: "Cricket Match Analysis",
    channel: "Sports",
    duration: "1:10",
    type: "Short",
    language: "English",
    status: "Not Published",
    thumbnail: "https://picsum.photos/400/200?2"
  },
  {
    id: 3,
    title: "AI Conference Speech",
    channel: "Tech",
    duration: "2:05",
    type: "Summary",
    language: "English",
    status: "Published",
    thumbnail: "https://picsum.photos/400/200?3"
  }
];

export default function VideoExplorer() {
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState({
    language: "",
    type: "",
    status: ""
  });
  const [gridSize, setGridSize] = useState(3);
  const [selectedVideo, setSelectedVideo] = useState(null);

  const filteredVideos = sampleVideos.filter((video) => {
    return (
      video.title.toLowerCase().includes(search.toLowerCase()) &&
      (!filters.language || video.language === filters.language) &&
      (!filters.type || video.type === filters.type) &&
      (!filters.status || video.status === filters.status)
    );
  });

  return (
    <div className="container">
      <h2 className="title">Video Explorer</h2>

      <div className="controls">
        <div className="left-controls">

          <input
            type="text"
            placeholder="Search videos..."
            className="input-field"
            onChange={(e) => setSearch(e.target.value)}
          />

          <select
            className={`select-field ${filters.language ? "active-field" : ""}`}
            onChange={(e) =>
              setFilters({ ...filters, language: e.target.value })
            }
          >
            <option value="">Language</option>
            <option value="Hindi">Hindi</option>
            <option value="English">English</option>
          </select>

          <select
            className={`select-field ${filters.type ? "active-field" : ""}`}
            onChange={(e) =>
              setFilters({ ...filters, type: e.target.value })
            }
          >
            <option value="">Video Type</option>
            <option value="Reel">Reel</option>
            <option value="Short">Short</option>
            <option value="Summary">Summary</option>
          </select>

          <select
            className={`select-field ${filters.status ? "active-field" : ""}`}
            onChange={(e) =>
              setFilters({ ...filters, status: e.target.value })
            }
          >
            <option value="">Status</option>
            <option value="Published">Published</option>
            <option value="Not Published">Not Published</option>
          </select>

        </div>

        <div className="grid-icons">
          <BsGrid1X2
            className={gridSize === 2 ? "icon active" : "icon"}
            onClick={() => setGridSize(2)}
          />
          <BsGrid
            className={gridSize === 3 ? "icon active" : "icon"}
            onClick={() => setGridSize(3)}
          />
          <BsGrid3X3Gap
            className={gridSize === 4 ? "icon active" : "icon"}
            onClick={() => setGridSize(4)}
          />
        </div>
      </div>

      <div className={`grid grid-${gridSize}`}>
        {filteredVideos.map((video) => (
          <div
            key={video.id}
            className="card"
            onClick={() => setSelectedVideo(video)}
          >
            <img src={video.thumbnail} alt="thumb" />
            <div className="card-content">
              <h3>{video.title}</h3>
              <p className="channel">{video.channel} Channel</p>
              <p className="meta">
                {video.duration} • {video.type} • {video.language}
              </p>
              <span
                className={
                  video.status === "Published"
                    ? "badge green"
                    : "badge red"
                }
              >
                {video.status}
              </span>
            </div>
          </div>
        ))}
      </div>

      {selectedVideo && (
        <div className="modal" onClick={() => setSelectedVideo(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <img src={selectedVideo.thumbnail} alt="preview" />
            <h2>{selectedVideo.title}</h2>
            <p>{selectedVideo.channel}</p>
            <button onClick={() => setSelectedVideo(null)}>Close</button>
          </div>
        </div>
      )}
    </div>
  );
}
