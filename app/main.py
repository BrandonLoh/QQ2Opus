from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
import sqlite3
import json
import os
import subprocess
from datetime import datetime

app = FastAPI()

DB_PATH = "/cache/music_server.db"
CACHE_DIR = "/cache"
CHUNK_SIZE = 4096

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS music_cache (
        song_id TEXT PRIMARY KEY,
        title TEXT,
        artist TEXT,
        file_path TEXT,
        status TEXT,
        file_size INTEGER DEFAULT 0,
        last_played_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS speaker_state (
        speaker_id TEXT PRIMARY KEY,
        current_song_id TEXT,
        playlist_json TEXT,
        current_index INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    INSERT OR IGNORE INTO speaker_state VALUES ('default', '', '[]', 0, datetime('now'))
    """)
    conn.commit()
    conn.close()

init_db()

# Helper function to execute FFmpeg commands
def ffmpeg_transcode(remote_url, output_path):
    command = [
        'ffmpeg', '-i', remote_url,
        '-c:a', 'libopus',
        '-b:a', '80k',
        '-vbr', 'on',
        '-ar', '48000',
        '-ac', '1',
        '-f', 'ogg',
        output_path
    ]
    subprocess.run(command, check=True)

# Background downloader
def background_downloader(song_id, remote_url):
    tmp_path = os.path.join(CACHE_DIR, f"{song_id}.tmp")
    final_path = os.path.join(CACHE_DIR, f"{song_id}.opus")
    try:
        ffmpeg_transcode(remote_url, tmp_path)
        os.rename(tmp_path, final_path)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE music_cache SET status = 'completed', file_path = ?, file_size = ?
        WHERE song_id = ?
        """, (final_path, os.path.getsize(final_path), song_id))
        conn.commit()
        conn.close()
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE music_cache SET status = 'failed' WHERE song_id = ?", (song_id,))
        conn.commit()
        conn.close()

# API: Set playlist
@app.post("/api/set_playlist")
async def set_playlist(data: dict, background_tasks: BackgroundTasks):
    song_ids = data.get("song_ids", [])
    if not song_ids:
        raise HTTPException(status_code=400, detail="No song IDs provided")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    UPDATE speaker_state SET playlist_json = ?, current_index = 0, updated_at = datetime('now')
    WHERE speaker_id = 'default'
    """, (json.dumps(song_ids),))
    conn.commit()
    conn.close()
    # Trigger background download for the first two songs
    for song_id in song_ids[:2]:
        background_tasks.add_task(background_downloader, song_id, f"https://example.com/{song_id}.mp3")
    return {"message": "Playlist updated"}

# API: Insert song
@app.post("/api/insert_song")
async def insert_song(data: dict):
    song_id = data.get("song_id")
    if not song_id:
        raise HTTPException(status_code=400, detail="No song ID provided")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT playlist_json, current_index FROM speaker_state WHERE speaker_id = 'default'")
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="Speaker state not found")
    playlist = json.loads(row[0])
    current_index = row[1]
    playlist.insert(current_index + 1, song_id)
    cursor.execute("""
    UPDATE speaker_state SET playlist_json = ?, updated_at = datetime('now')
    WHERE speaker_id = 'default'
    """, (json.dumps(playlist),))
    conn.commit()
    conn.close()
    return {"message": "Song inserted"}

# API: Control playback
@app.post("/api/control")
async def control(action: str):
    if action not in ["next", "prev"]:
        raise HTTPException(status_code=400, detail="Invalid action")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT playlist_json, current_index FROM speaker_state WHERE speaker_id = 'default'")
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="Speaker state not found")
    playlist = json.loads(row[0])
    current_index = row[1]
    if action == "next":
        current_index = min(current_index + 1, len(playlist) - 1)
    elif action == "prev":
        current_index = max(current_index - 1, 0)
    cursor.execute("""
    UPDATE speaker_state SET current_index = ?, updated_at = datetime('now')
    WHERE speaker_id = 'default'
    """, (current_index,))
    conn.commit()
    conn.close()
    return {"message": "Playback updated", "current_index": current_index}

# API: Stream next song
@app.get("/api/next_stream.opus")
async def next_stream():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT playlist_json, current_index FROM speaker_state WHERE speaker_id = 'default'")
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="Speaker state not found")
    playlist = json.loads(row[0])
    current_index = row[1]
    if current_index >= len(playlist):
        raise HTTPException(status_code=404, detail="PLAYLIST_END")
    song_id = playlist[current_index]
    cursor.execute("SELECT file_path, status FROM music_cache WHERE song_id = ?", (song_id,))
    song_row = cursor.fetchone()
    conn.close()
    if not song_row or song_row[1] != "completed" or not os.path.exists(song_row[0]):
        raise HTTPException(status_code=404, detail="Song not available")
    def file_stream():
        with open(song_row[0], "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                yield chunk
    return StreamingResponse(file_stream(), media_type="audio/ogg")