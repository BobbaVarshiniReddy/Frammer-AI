import React, { useEffect, useRef, useState } from "react";
import "./VideoExplorer.css";
import { BsGrid3X3Gap, BsGrid1X2, BsGrid } from "react-icons/bs";

export default function VideoExplorer() {
  const PAGE_SIZE = 12;

  const [gridSize, setGridSize] = useState(3);
  const [selectedVideo, setSelectedVideo] = useState(null);

  const [metaTypes, setMetaTypes] = useState([]);
  const [metaUploaders, setMetaUploaders] = useState([]);

  const [filters, setFilters] = useState({
    name: "",
    type: "",
    uploaded_by: "",
    video_id: "",
  });

  const [videos, setVideos] = useState([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [initialLoading, setInitialLoading] = useState(false);
  const [sortLabel] = useState("A-Z (Headline)");

  const sentinelRef = useRef(null);

  const buildUrl = (nextOffset) => {
    const params = new URLSearchParams();
    if (filters.name) params.append("name", filters.name);
    if (filters.type) params.append("type", filters.type);
    if (filters.uploaded_by) params.append("uploaded_by", filters.uploaded_by);
    if (filters.video_id) params.append("video_id", filters.video_id);
    params.append("offset", String(nextOffset));
    params.append("limit", String(PAGE_SIZE));
    return `https://frammer-ai-1-spo5.onrender.com/videos?${params.toString()}`;
  };

  const fetchMeta = async () => {
    try {
      const res = await fetch("https://frammer-ai-1-spo5.onrender.com/videos/meta");
      const data = await res.json();
      setMetaTypes(data.types || []);
      setMetaUploaders(data.uploaded_by || []);
    } catch (e) {
      setMetaTypes([]);
      setMetaUploaders([]);
    }
  };

  const loadPage = async ({ nextOffset, append }) => {
    setError(null);
    if (!append) {
      setInitialLoading(true);
    }
    setLoading(true);

    try {
      const res = await fetch(buildUrl(nextOffset));
      const resData = await res.json();

      if (resData.error) {
        setError(resData.error);
        setVideos([]);
        setHasMore(false);
        return;
      }

      const nextVideos = resData.data || [];
      setVideos((prev) => (append ? [...prev, ...nextVideos] : nextVideos));
      setOffset(resData.next_offset ?? nextOffset + nextVideos.length);
      setHasMore(Boolean(resData.has_more));
    } catch (e) {
      setError(e.message || "Failed to load videos");
      setHasMore(false);
    } finally {
      setLoading(false);
      setInitialLoading(false);
    }
  };

  const loadFirstPage = () => {
    setVideos([]);
    setOffset(0);
    setHasMore(true);
    loadPage({ nextOffset: 0, append: false });
  };

  const loadMore = () => {
    if (loading || !hasMore) return;
    // Prevent duplicate initial fetch while the first page is not loaded yet.
    if (offset === 0 && videos.length === 0) return;
    loadPage({ nextOffset: offset, append: true });
  };

  useEffect(() => {
    fetchMeta();
  }, []);

  useEffect(() => {
    const t = setTimeout(() => {
      loadFirstPage();
    }, 300);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters.name, filters.type, filters.uploaded_by, filters.video_id]);

  useEffect(() => {
    if (!sentinelRef.current) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const first = entries[0];
        if (first && first.isIntersecting) {
          loadMore();
        }
      },
      { root: null, rootMargin: "250px", threshold: 0 }
    );

    observer.observe(sentinelRef.current);
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sentinelRef, offset, hasMore, loading, filters]);

  return (
    <div className="container">
      <h2 className="title">Video Explorer</h2>

      <div className="controls">
        <div className="left-controls">
          <select
            className={`select-field ${filters.type ? "active-field" : ""}`}
            value={filters.type}
            onChange={(e) => setFilters((prev) => ({ ...prev, type: e.target.value }))}
          >
            <option value="">Type</option>
            {metaTypes.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>

          <select
            className={`select-field ${filters.uploaded_by ? "active-field" : ""}`}
            value={filters.uploaded_by}
            onChange={(e) => setFilters((prev) => ({ ...prev, uploaded_by: e.target.value }))}
          >
            <option value="">Uploaded By</option>
            {metaUploaders.map((u) => (
              <option key={u} value={u}>
                {u}
              </option>
            ))}
          </select>

          <input
            type="text"
            placeholder="Video ID"
            className="input-field"
            value={filters.video_id}
            onChange={(e) => setFilters((prev) => ({ ...prev, video_id: e.target.value }))}
          />
        </div>

        <div style={{ color: "#ffffff7a", alignSelf: "center" }}>{sortLabel}</div>

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

      {initialLoading && <p>Loading videos...</p>}
      {error && <p style={{ color: "red" }}>{error}</p>}

      <div className={`grid grid-${gridSize}`}>
        {videos.map((video, i) => (
          <div
            key={`${video.video_id ?? "missing"}-${i}`}
            className="card"
            onClick={() => setSelectedVideo(video)}
          >
            <img src={video.thumbnail} alt="thumb" />
            <div className="card-content">
              <h3>{video.title}</h3>
              <p className="channel">
                {video.uploaded_by ? `${video.uploaded_by}` : "Uploaded Channel"}
              </p>
              <p className="meta">
                {video.type ? video.type : "Unknown"} • {video.video_id ?? "N/A"}
              </p>
              <span className={video.published === "Yes" ? "badge green" : "badge red"}>
                {video.published === "Yes" ? "Published" : "Not Published"}
              </span>
            </div>
          </div>
        ))}
      </div>

      <div ref={sentinelRef} style={{ height: 1 }} />

      {loading && !initialLoading && <p style={{ color: "#ffffff7a" }}>Loading more...</p>}
      {!hasMore && videos.length > 0 && <p style={{ color: "#ffffff7a" }}>No more videos</p>}

      {selectedVideo && (
        <div className="modal" onClick={() => setSelectedVideo(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <img src={selectedVideo.thumbnail} alt="preview" />
            <h2>{selectedVideo.title}</h2>
            <p>{selectedVideo.uploaded_by}</p>
            {selectedVideo.published_url ? (
              <a
                href={selectedVideo.published_url}
                target="_blank"
                rel="noreferrer"
                style={{ display: "inline-block", marginTop: 10, color: "#ff4649" }}
              >
                Open Published URL
              </a>
            ) : null}
            <button onClick={() => setSelectedVideo(null)}>Close</button>
          </div>
        </div>
      )}
    </div>
  );
}
