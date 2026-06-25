"""
QQ2Opus — 单声道优化、具备自然切歌与语义解析的 NAS 音频推流服务端
======================================================================
为 ESP32-S3 单喇叭智能音箱提供低带宽、高容错的 Opus 音频流。
与同网段 go-music-api 联动，支持歌单/单曲模糊搜索、插播、自动切歌。
"""

import asyncio
import json
import os
import re
import shutil
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

# FFmpeg availability check (used for graceful degradation)
FFMPEG_AVAILABLE = shutil.which("ffmpeg") is not None

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

# Load .env file if present
_load_env = Path(__file__).resolve().parent.parent / ".env"
if _load_env.exists():
    load_dotenv(_load_env)

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

app = FastAPI(title="QQ2Opus", version="2.0.0")

DB_PATH      = os.environ.get("DB_PATH", "/cache/music_server.db")
CACHE_DIR    = os.environ.get("CACHE_DIR", "/cache")
API_BASE     = os.environ.get("GO_MUSIC_API_BASE", "http://192.168.1.50:8080")
CHUNK_SIZE   = int(os.environ.get("CHUNK_SIZE", "4096"))
PUID         = os.environ.get("PUID", "")
PGID         = os.environ.get("PGID", "")

# 本地歌单名 → {id, source} 硬映射
PLAYLIST_NAME_MAP: dict[str, dict[str, str]] = {}
_map_env = os.environ.get("PLAYLIST_NAME_MAP", "")
if _map_env:
    try:
        PLAYLIST_NAME_MAP = json.loads(_map_env)
    except json.JSONDecodeError:
        pass

# go-music-api 源平台猜测顺序（当只提供 ID 不含 source 时按此顺序尝试）
SOURCE_GUESS_ORDER = ["qq", "netease", "kuwo", "kugou", "migu"]

os.makedirs(CACHE_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=3000")
    return conn


def init_db() -> None:
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS music_cache (
            song_id     TEXT PRIMARY KEY,
            title       TEXT DEFAULT '',
            artist      TEXT DEFAULT '',
            source      TEXT DEFAULT '',
            file_path   TEXT DEFAULT '',
            status      TEXT DEFAULT 'pending',
            file_size   INTEGER DEFAULT 0,
            last_played_at TIMESTAMP,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS speaker_state (
            speaker_id      TEXT PRIMARY KEY,
            current_song_id TEXT DEFAULT '',
            playlist_json   TEXT DEFAULT '[]',
            current_index   INTEGER DEFAULT 0,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Add source column to existing tables (migration)
    try:
        conn.execute("ALTER TABLE music_cache ADD COLUMN source TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # column already exists

    conn.execute("""
        INSERT OR IGNORE INTO speaker_state
        VALUES ('default', '', '[]', 0, datetime('now'))
    """)
    conn.commit()
    conn.close()


init_db()


def _read_state() -> tuple[list[str], int]:
    conn = _get_db()
    row = conn.execute(
        "SELECT playlist_json, current_index FROM speaker_state WHERE speaker_id='default'"
    ).fetchone()
    conn.close()
    if not row:
        return [], 0
    return json.loads(row["playlist_json"] or "[]"), row["current_index"]


def _write_state(playlist: list[str], index: int,
                 current_song_id: str = "") -> None:
    conn = _get_db()
    conn.execute(
        """UPDATE speaker_state
           SET playlist_json=?, current_index=?, current_song_id=?,
               updated_at=datetime('now')
           WHERE speaker_id='default'""",
        (json.dumps(playlist), index, current_song_id),
    )
    conn.commit()
    conn.close()


def _upsert_cache(song_id: str, **fields) -> None:
    if not fields:
        return
    conn = _get_db()
    existing = conn.execute(
        "SELECT song_id FROM music_cache WHERE song_id=?", (song_id,)
    ).fetchone()
    if existing:
        set_clause = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [song_id]
        conn.execute(f"UPDATE music_cache SET {set_clause} WHERE song_id=?", vals)
    else:
        keys = ["song_id"] + list(fields.keys())
        placeholders = ", ".join("?" for _ in keys)
        vals = [song_id] + list(fields.values())
        conn.execute(
            f"INSERT INTO music_cache ({', '.join(keys)}) VALUES ({placeholders})",
            vals,
        )
    conn.commit()
    conn.close()


def _get_cache(song_id: str) -> Optional[sqlite3.Row]:
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM music_cache WHERE song_id=?", (song_id,)
    ).fetchone()
    conn.close()
    return row


# ═══════════════════════════════════════════════════════════════════
#  go-music-api Client (async)
# ═══════════════════════════════════════════════════════════════════

async def _api_get(endpoint: str, params: dict = None,
                   timeout: float = 20.0) -> dict:
    """Thin wrapper: GET go-music-api, return parsed JSON."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(f"{API_BASE}{endpoint}", params=params)
        resp.raise_for_status()
        return resp.json()


def _ok(data: dict) -> bool:
    """Check if go-music-api response code is 200."""
    return data.get("code") == 200


def _song_from_result(item: dict) -> dict:
    """Normalize a song item from API responses into a standard dict."""
    return {
        "id": str(item.get("id", "")),
        "name": item.get("name") or "",
        "artist": item.get("artist") or "",
        "source": item.get("source") or "",
        "album": item.get("album") or "",
        "cover": item.get("cover") or "",
        "duration": item.get("duration") or 0,
    }


# ---- Search ----

async def search_song(keyword: str) -> Optional[dict]:
    """Search single song; returns normalized dict or None."""
    try:
        data = await _api_get("/api/v1/music/search",
                              {"q": keyword, "type": "song"})
    except Exception:
        return None
    if not _ok(data):
        return None
    songs = data.get("data", {}).get("songs") or []
    if songs:
        return _song_from_result(songs[0])
    return None


async def search_playlist(keyword: str) -> Optional[dict]:
    """Search playlist by name; returns {id, name, source} or None."""
    try:
        data = await _api_get("/api/v1/music/search",
                              {"q": keyword, "type": "playlist"})
    except Exception:
        return None
    if not _ok(data):
        return None
    playlists = data.get("data", {}).get("playlists") or []
    if playlists:
        p = playlists[0]
        return {
            "id": str(p.get("id", "")),
            "name": p.get("name") or "",
            "source": p.get("source") or "",
        }
    return None


# ---- Playlist detail ----

async def get_playlist_tracks(playlist_id: str, source: str) -> list[dict]:
    """Return list of normalized song dicts from a playlist."""
    try:
        data = await _api_get("/api/v1/playlist/detail",
                              {"id": playlist_id, "source": source})
    except Exception:
        return []
    if not _ok(data):
        return []
    tracks = data.get("data") or []
    if not isinstance(tracks, list):
        return []
    return [_song_from_result(t) for t in tracks]


# ---- Song URL ----

async def fetch_song_url(song_id: str, source: str) -> Optional[str]:
    """Return the playable stream URL for a song."""
    try:
        data = await _api_get("/api/v1/music/url",
                              {"id": song_id, "source": source},
                              timeout=15.0)
    except Exception:
        return None
    if not _ok(data):
        return None
    return (data.get("data") or {}).get("url") or ""


async def fetch_song_url_any_source(song_id: str) -> Optional[tuple[str, str]]:
    """Try to get song URL from cached source, or guess from common sources."""
    # 1. Try cached source
    cache = _get_cache(song_id)
    if cache and cache["source"]:
        url = await fetch_song_url(song_id, cache["source"])
        if url:
            return url, cache["source"]

    # 2. Guess source
    for src in SOURCE_GUESS_ORDER:
        url = await fetch_song_url(song_id, src)
        if url:
            _upsert_cache(song_id, source=src)
            return url, src

    return None


# ---- Resolve helpers ----

async def resolve_song(raw: str) -> Optional[dict]:
    """
    Given a song_id or song_name, return {id, name, artist, source}.
    - If looks like a pure ID → return minimal dict (source to be resolved later)
    - If looks like a name → search API
    """
    raw = raw.strip()
    if not raw:
        return None

    # Pure ID pattern (alphanumeric, no spaces/CJK)
    if re.match(r'^[\w\-]+$', raw) and len(raw) < 40 and not re.search(r'[\u4e00-\u9fff]', raw):
        return {"id": raw, "name": raw, "artist": "", "source": ""}

    # Fuzzy name search
    result = await search_song(raw)
    if not result:
        return None
    # Cache metadata
    _upsert_cache(result["id"], title=result["name"], artist=result["artist"],
                  source=result["source"])
    return result


async def resolve_playlist(raw: str) -> Optional[dict]:
    """
    Given a playlist_name or playlist_id, return {id, source}.
    """
    raw = raw.strip()
    if not raw:
        return None

    # 1. Local hard mapping
    if raw in PLAYLIST_NAME_MAP:
        entry = PLAYLIST_NAME_MAP[raw]
        if isinstance(entry, dict) and entry.get("id"):
            return {"id": entry["id"], "source": entry.get("source", "qq")}
        if isinstance(entry, str) and entry:
            return {"id": entry, "source": "qq"}

    # 2. Pure ID
    if re.match(r'^[\w\-]+$', raw) and len(raw) < 40 and not re.search(r'[\u4e00-\u9fff]', raw):
        # Try guessing source
        for src in SOURCE_GUESS_ORDER:
            tracks = await get_playlist_tracks(raw, src)
            if tracks:
                return {"id": raw, "source": src}
        return {"id": raw, "source": "qq"}  # fallback

    # 3. Remote fuzzy search
    result = await search_playlist(raw)
    if result:
        return {"id": result["id"], "source": result["source"]}
    return None


# ═══════════════════════════════════════════════════════════════════
#  Recommend Playlists
# ═══════════════════════════════════════════════════════════════════

RECOMMEND_PLAYLISTS: list[dict] = []
_rec_env = os.environ.get("RECOMMEND_PLAYLISTS", "")
if _rec_env:
    try:
        RECOMMEND_PLAYLISTS = json.loads(_rec_env)
    except json.JSONDecodeError:
        pass


async def fetch_recommend_playlists(source: str = "", limit: int = 20) -> list[dict]:
    """
    Fetch recommended playlists from go-music-api.
    - source (optional): filter by platform (qq/netesae/kuwo/kugou/migu).
    - limit: max results.
    Returns list of {id, name, source, cover, track_count, play_count, creator}.
    """
    try:
        data = await _api_get("/api/v1/playlist/recommend")
    except Exception:
        return _fallback_recommend(source, limit)

    if not _ok(data):
        return _fallback_recommend(source, limit)

    playlists = data.get("data") or []
    if not isinstance(playlists, list):
        return _fallback_recommend(source, limit)

    result = []
    for p in playlists:
        if source and p.get("source", "") != source:
            continue
        result.append({
            "id": str(p.get("id", "")),
            "name": p.get("name") or "",
            "source": p.get("source") or "",
            "cover": p.get("cover") or "",
            "track_count": p.get("track_count") or 0,
            "play_count": p.get("play_count") or 0,
            "creator": p.get("creator") or "",
            "description": p.get("description") or "",
        })
        if len(result) >= limit:
            break

    if not result:
        return _fallback_recommend(source, limit)
    return result


def _fallback_recommend(source: str, limit: int) -> list[dict]:
    """Local fallback: env-configured or built-in curated list."""
    candidates = RECOMMEND_PLAYLISTS if RECOMMEND_PLAYLISTS else [
        {"id": "7828723309", "name": "华语热门精选", "source": "qq", "cover": "", "track_count": 100, "play_count": 0, "creator": "QQ音乐"},
        {"id": "8678155324", "name": "抖音热门歌曲", "source": "qq", "cover": "", "track_count": 200, "play_count": 0, "creator": "QQ音乐"},
        {"id": "3100839429", "name": "经典老歌500首", "source": "qq", "cover": "", "track_count": 500, "play_count": 0, "creator": "QQ音乐"},
        {"id": "128556", "name": "华语新歌速递", "source": "netease", "cover": "", "track_count": 50, "play_count": 0, "creator": "网易云音乐"},
        {"id": "3778678", "name": "纯音乐|安静治愈", "source": "netease", "cover": "", "track_count": 100, "play_count": 0, "creator": "网易云音乐"},
    ]
    if source:
        candidates = [c for c in candidates if c.get("source") == source]
    return candidates[:limit]


# ═══════════════════════════════════════════════════════════════════
#  Background Download & Transcode
# ═══════════════════════════════════════════════════════════════════

async def download_and_transcode(song_id: str) -> None:
    """
    Asynchronous background task:
    1. Mark status = 'downloading'
    2. Resolve song URL (with source guessing)
    3. Transcode to mono Opus via FFmpeg
    4. Mark status = 'completed' or 'failed'
    """
    tmp_path = os.path.join(CACHE_DIR, f"{song_id}.tmp")
    final_path = os.path.join(CACHE_DIR, f"{song_id}.opus")

    _upsert_cache(song_id, status="downloading")

    if not FFMPEG_AVAILABLE:
        _upsert_cache(song_id, status="failed")
        return

    try:
        # --- Resolve URL (with source) ---
        url_source = await fetch_song_url_any_source(song_id)
        if not url_source:
            _upsert_cache(song_id, status="failed")
            return
        url, source = url_source
        _upsert_cache(song_id, source=source)

        # --- FFmpeg transcode ---
        cmd = [
            "ffmpeg", "-threads", "2",
            "-i", url,
            "-c:a", "libopus",
            "-b:a", "80k",
            "-vbr", "on",
            "-ar", "48000",
            "-ac", "1",
            "-f", "ogg",
            "-y", tmp_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()

        if proc.returncode == 0 and os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            os.rename(tmp_path, final_path)
            file_size = os.path.getsize(final_path)

            if PUID and PGID:
                try:
                    os.chown(final_path, int(PUID), int(PGID))
                except (PermissionError, OSError):
                    pass

            _upsert_cache(song_id, status="completed",
                          file_path=final_path, file_size=file_size)
        else:
            _upsert_cache(song_id, status="failed")

    except Exception:
        _upsert_cache(song_id, status="failed")
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════
#  Streaming Generators
# ═══════════════════════════════════════════════════════════════════

async def _stream_cached_file(file_path: str):
    """Async generator: read cached .opus file in chunks."""
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


async def _stream_ffmpeg_realtime(url: str):
    """
    Async generator: launch FFmpeg subprocess, pipe stdout → yield chunks.
    Guarantees FFmpeg cleanup on disconnect / completion.
    """
    cmd = [
        "ffmpeg", "-threads", "2",
        "-i", url,
        "-c:a", "libopus",
        "-b:a", "80k",
        "-vbr", "on",
        "-ar", "48000",
        "-ac", "1",
        "-f", "ogg",
        "pipe:1",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        while True:
            chunk = await proc.stdout.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk
    finally:
        if proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except (TimeoutError, ProcessLookupError):
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════════
#  API Endpoints
# ═══════════════════════════════════════════════════════════════════

# ─── POST /api/set_playlist ────────────────────────────────────────

@app.post("/api/set_playlist")
async def set_playlist(data: dict):
    """
    Set / overwrite the speaker's playlist.

    Accepts one of:
      - {"song_ids": ["123", "456"]}
      - {"playlist_id": "789"}          (optionally + "source": "netease")
      - {"playlist_name": "我喜欢的音乐"}
      - {"song_name": "周杰伦 晴天"}
    """
    song_ids: list[str] = []

    # --- Case 1: Direct song ID list ---
    if "song_ids" in data and data["song_ids"]:
        song_ids = [str(s) for s in data["song_ids"]]
        # For bare IDs without metadata, pre-fetch will resolve source later

    # --- Case 2: Single song name → wrap as 1-song playlist ---
    elif "song_name" in data and data["song_name"]:
        result = await resolve_song(data["song_name"])
        if not result:
            raise HTTPException(status_code=404,
                                detail=f"Song not found: {data['song_name']}")
        song_ids = [result["id"]]
        # Metadata already cached by resolve_song

    # --- Case 3/4: Playlist by ID or name ---
    else:
        raw = data.get("playlist_id") or data.get("playlist_name") or ""
        user_source = data.get("source", "")  # optional explicit source
        if not raw:
            raise HTTPException(
                status_code=400,
                detail="Provide one of: song_ids, playlist_id, playlist_name, song_name",
            )

        if "playlist_id" in data and user_source:
            # User provided both ID and source → direct
            tracks = await get_playlist_tracks(raw, user_source)
        elif "playlist_id" in data:
            # ID only → try guessing source
            tracks = []
            for src in SOURCE_GUESS_ORDER:
                tracks = await get_playlist_tracks(raw, src)
                if tracks:
                    break
        else:
            # playlist_name → resolve first
            resolved = await resolve_playlist(raw)
            if not resolved:
                raise HTTPException(status_code=404,
                                    detail=f"Playlist not found: {raw}")
            tracks = await get_playlist_tracks(resolved["id"], resolved["source"])

        if not tracks:
            raise HTTPException(status_code=404,
                                detail=f"Playlist empty or unavailable: {raw}")

        # Cache metadata for all tracks
        for t in tracks:
            _upsert_cache(t["id"], title=t["name"], artist=t["artist"],
                          source=t["source"])

        song_ids = [t["id"] for t in tracks]

    # --- Persist ---
    _write_state(song_ids, 0)

    # --- Trigger background preload for first 2 songs ---
    for sid in song_ids[:2]:
        asyncio.create_task(download_and_transcode(sid))

    return {
        "message": "Playlist updated",
        "count": len(song_ids),
    }


# ─── POST /api/insert_song ─────────────────────────────────────────

@app.post("/api/insert_song")
async def insert_song(data: dict):
    """
    Insert a song right after the current playing position.
    Accepts: {"song_id": "xxx"} or {"song_name": "陈奕迅 十年"}
    """
    raw = data.get("song_id") or data.get("song_name") or ""
    if not raw:
        raise HTTPException(status_code=400,
                            detail="Provide song_id or song_name")

    result = await resolve_song(raw)
    if not result:
        raise HTTPException(status_code=404, detail=f"Song not found: {raw}")

    playlist, idx = _read_state()
    playlist.insert(idx + 1, result["id"])
    _write_state(playlist, idx)

    asyncio.create_task(download_and_transcode(result["id"]))

    return {"message": "Song inserted", "song_id": result["id"],
            "position": idx + 1}


# ─── GET /api/recommend_playlists ──────────────────────────────────

@app.get("/api/recommend_playlists")
async def recommend_playlists(
    source: str = Query("", description="平台过滤: qq/netesae/kuwo/kugou/migu，留空则全部"),
    limit: int = Query(20, ge=1, le=50, description="返回数量上限"),
):
    """
    Get recommended playlists for the speaker to display.
    Call go-music-api's native recommendation endpoint; falls back to
    a curated local list if the API is unreachable.

    The speaker can use the returned id + source with POST /api/set_playlist
    to set a playlist for playback.
    """
    playlists = await fetch_recommend_playlists(source=source, limit=limit)
    return {"playlists": playlists}


# ─── POST /api/control ─────────────────────────────────────────────

@app.post("/api/control")
async def control(action: str = Query(..., description="next or prev")):
    if action not in ("next", "prev"):
        raise HTTPException(status_code=400,
                            detail="Action must be 'next' or 'prev'")

    playlist, idx = _read_state()
    if not playlist:
        raise HTTPException(status_code=400, detail="Playlist is empty")

    if action == "next":
        idx = min(idx + 1, len(playlist))
    else:
        idx = max(idx - 1, 0)

    _write_state(playlist, idx)

    return {"message": f"Playback {action}", "current_index": idx,
            "total": len(playlist)}


# ─── GET /api/current_status ───────────────────────────────────────

@app.get("/api/current_status")
async def current_status():
    """
    Polled by the speaker for on-screen display.
    Returns current song title & artist.
    """
    playlist, idx = _read_state()

    if idx >= len(playlist) or not playlist:
        return {"status": "idle", "title": "", "artist": "", "song_id": ""}

    song_id = playlist[idx]

    # 1. Try local cache (fast path)
    cache = _get_cache(song_id)
    if cache and cache["title"]:
        return {"status": "playing", "title": cache["title"],
                "artist": cache["artist"] or "", "song_id": song_id}

    # 2. Fallback: show song_id as title
    return {"status": "playing", "title": song_id, "artist": "", "song_id": song_id}


# ─── GET /api/next_stream.opus  (CORE streaming endpoint) ──────────

@app.get("/api/next_stream.opus")
async def next_stream():
    """
    Streaming endpoint for the speaker's audio pipeline.

    Logic:
      1. If playlist exhausted → HTTP 404 "PLAYLIST_END"
      2. Lock current song_id, then auto-increment DB index
      3. Pre-fetch next song in background
      4. Stream cached .opus file (fast path) or real-time FFmpeg proxy

    When the generator finishes, FastAPI closes the TCP connection →
    speaker sees DONE → requests the next song automatically.
    """
    playlist, idx = _read_state()

    if idx >= len(playlist) or not playlist:
        raise HTTPException(status_code=404, detail="PLAYLIST_END")

    song_id = playlist[idx]

    # --- Auto-step ---
    new_idx = idx + 1
    _write_state(playlist, new_idx, current_song_id=song_id)

    # --- Pre-fetch next song ---
    if new_idx < len(playlist):
        asyncio.create_task(download_and_transcode(playlist[new_idx]))

    # --- Check cache ---
    cache = _get_cache(song_id)
    cache_path = cache["file_path"] if cache else ""

    if cache_path and os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        # ✅ Fast path: pre-transcoded Opus file
        _upsert_cache(song_id, last_played_at=datetime.now().isoformat())

        return StreamingResponse(
            _stream_cached_file(cache_path),
            media_type="audio/ogg",
            headers={"X-Song-Id": song_id, "X-Stream-Mode": "cache"},
        )
    else:
        # ⚡ Live path: real-time FFmpeg proxy
        if not FFMPEG_AVAILABLE:
            raise HTTPException(
                status_code=503,
                detail="FFmpeg not installed — cannot transcode live streams. "
                       "Install ffmpeg or pre-cache the song first.",
            )

        url_source = await fetch_song_url_any_source(song_id)
        if not url_source:
            raise HTTPException(status_code=404,
                                detail="Song URL unavailable")
        url, source = url_source
        _upsert_cache(song_id, source=source)

        return StreamingResponse(
            _stream_ffmpeg_realtime(url),
            media_type="audio/ogg",
            headers={"X-Song-Id": song_id, "X-Stream-Mode": "live"},
        )


# ─── GET /api/play/{song_id}.opus  (browser-friendly, no side-effects) ──

@app.get("/api/play/{song_id}.opus")
async def play_song(song_id: str):
    """
    Stream a specific song by ID, without touching the playlist state.
    Ideal for browser playback testing — just open the URL and listen.
    """
    if not re.match(r'^[\w\-]+$', song_id):
        raise HTTPException(status_code=400, detail="Invalid song_id")

    # 1. Try cached file
    cache = _get_cache(song_id)
    cache_path = cache["file_path"] if cache else ""
    if cache_path and os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        _upsert_cache(song_id, last_played_at=datetime.now().isoformat())
        return StreamingResponse(
            _stream_cached_file(cache_path),
            media_type="audio/ogg",
            headers={"X-Song-Id": song_id, "X-Stream-Mode": "cache"},
        )

    # 2. Fallback: real-time FFmpeg proxy
    if not FFMPEG_AVAILABLE:
        raise HTTPException(status_code=503,
                            detail="FFmpeg not installed")

    url_source = await fetch_song_url_any_source(song_id)
    if not url_source:
        raise HTTPException(status_code=404, detail="Song URL unavailable")
    url, source = url_source
    _upsert_cache(song_id, source=source)

    return StreamingResponse(
        _stream_ffmpeg_realtime(url),
        media_type="audio/ogg",
        headers={"X-Song-Id": song_id, "X-Stream-Mode": "live"},
    )


# ═══════════════════════════════════════════════════════════════════
#  Health check
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}
