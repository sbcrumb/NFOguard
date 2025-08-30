#!/usr/bin/env python3
# webhook_server.py - FastAPI webhook server for media date cache

import os
import json
import asyncio
import glob
import re
from pathlib import Path
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Set
import sqlite3
import uvicorn
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# Import our existing logic
from media_date_cache import (
   db_connect, apply_series, parse_imdb_from_path, 
   _log, _iso_now, SonarrClient
)

# Pydantic models for webhook payloads
class SonarrWebhook(BaseModel):
   eventType: str
   series: Optional[Dict[str, Any]] = None
   episodes: Optional[list] = []
   episodeFile: Optional[Dict[str, Any]] = None
   isUpgrade: Optional[bool] = False
   movie: Optional[Dict[str, Any]] = None
   remoteMovie: Optional[Dict[str, Any]] = None
   deletedFiles: Optional[list] = []
   
   class Config:
       extra = "allow"  # Allow extra fields not defined in the model

class RadarrWebhook(BaseModel):
   eventType: str
   movie: Optional[Dict[str, Any]] = None
   movieFile: Optional[Dict[str, Any]] = None
   isUpgrade: Optional[bool] = False
   deletedFiles: Optional[list] = []
   remoteMovie: Optional[Dict[str, Any]] = None
   renamedMovieFiles: Optional[List[Dict[str, Any]]] = []  # present on Rename
   
   class Config:
       extra = "allow"

class HealthResponse(BaseModel):
   status: str
   version: str
   uptime: str
   database_status: str

app = FastAPI(
   title="Media Date Cache Webhook Server",
   description="Webhook server for managing TV show and movie import dates and NFO files",
   version="1.0.0"
)

# Global state
start_time = datetime.now(timezone.utc)
db_path = Path(os.environ.get("DB_PATH", "/app/data/media_dates.db"))

class WebhookConfig:
   def __init__(self):
       # TV paths
       tv_paths_str = os.environ.get("TV_PATHS", os.environ.get("MEDIA_PATH", "/media/tv,/media/tv6"))
       self.tv_paths = [Path(p.strip()) for p in tv_paths_str.split(",")]
       
       # Movie paths
       movie_paths_str = os.environ.get("MOVIE_PATHS", "/media/movies,/media/movies6")
       self.movie_paths = [Path(p.strip()) for p in movie_paths_str.split(",")]
       
       self.manage_nfo = os.environ.get("MANAGE_NFO", "true").lower() == "true"
       self.fix_dir_mtimes = os.environ.get("FIX_DIR_MTIMES", "true").lower() == "true"
       self.lock_metadata = os.environ.get("LOCK_METADATA", "true").lower() == "true"
       self.debug = os.environ.get("DEBUG", "false").lower() == "true"
       # Batching configuration
       self.batch_delay = float(os.environ.get("BATCH_DELAY", "5.0"))  # seconds
       self.max_concurrent_series = int(os.environ.get("MAX_CONCURRENT_SERIES", "3"))

config = WebhookConfig()

# Movie database schema additions
MOVIE_SCHEMA = """
CREATE TABLE IF NOT EXISTS movies (
  imdb_id TEXT PRIMARY KEY,
  movie_path TEXT NOT NULL,
  last_applied TEXT
);

CREATE TABLE IF NOT EXISTS movie_dates (
  imdb_id TEXT PRIMARY KEY,
  released TEXT,
  dateadded TEXT,
  source TEXT,
  has_video_file INTEGER DEFAULT 0
);
"""

def ensure_movie_tables(conn: sqlite3.Connection):
    """Ensure movie tables exist in the database"""
    try:
        result = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='movies'").fetchone()
        if not result:
            _log("INFO", "Creating movie tables in database")
            conn.executescript(MOVIE_SCHEMA)
            conn.commit()
    except sqlite3.Error as e:
        _log("ERROR", f"Failed to create movie tables: {e}")

# Movie processing functions
def parse_imdb_from_movie_path(p: Path):
    """Extract IMDb ID from movie directory name"""
    # Look for patterns like: Movie Name (2023) [imdb-tt1234567]
    m = re.search(r'\[imdb-(tt\d+)\]', p.name, re.IGNORECASE)
    return m.group(1).lower() if m else None

def is_movie_dir(p: Path) -> bool:
    """Check if path is a movie directory with IMDb ID"""
    return p.is_dir() and parse_imdb_from_movie_path(p) is not None

def write_movie_nfo(movie_dir: Path, imdb_id: str, released: str, dateadded: str, source: str, lock_metadata: bool = True):
    """Write movie NFO file"""
    try:
        movie_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _log("ERROR", f"Failed creating movie directory {movie_dir}: {e}")
        return
        
    nfo_file = movie_dir / f"{movie_dir.name}.nfo"
    xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        "<movie>",
        f"  <id>{imdb_id}</id>",
        f"  <uniqueid type=\"imdb\" default=\"true\">{imdb_id}</uniqueid>",
    ]
    
    if released:
        xml.append(f"  <premiered>{released.split('T')[0]}</premiered>")
        xml.append(f"  <year>{released.split('-')[0]}</year>")
    
    if dateadded:
        xml.append(f"  <dateadded>{dateadded}</dateadded>")
    
    if lock_metadata:
        xml.append("  <lockdata>true</lockdata>")
    
    if source:
        xml.append(f"  <!-- source: {source} -->")
        xml.append(f"  <!-- managed by media-date-cache -->")
    
    xml.append("</movie>\n")
    content = "\n".join(xml)
    
    try:
        nfo_file.write_text(content, encoding="utf-8")
        _log("DEBUG", f"Created movie NFO: {nfo_file}")
    except Exception as e:
        _log("ERROR", f"Failed writing {nfo_file}: {e}")

def set_movie_file_mtime(movie_dir: Path, dateadded_iso: str):
    """Set movie file modification time to match dateadded"""
    if not dateadded_iso:
        return
        
    try:
        ts = datetime.fromisoformat(dateadded_iso.replace("Z", "+00:00")).timestamp()
        
        # Find video files
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        for f in movie_dir.iterdir():
            if f.is_file() and f.suffix.lower() in video_exts:
                try:
                    os.utime(f, (ts, ts), follow_symlinks=False)
                    _log("DEBUG", f"Updated movie file mtime: {f} -> {dateadded_iso}")
                except Exception as e:
                    _log("WARNING", f"Failed to update mtime on {f}: {e}")
                    
        # Set directory mtime too
        try:
            os.utime(movie_dir, (ts, ts), follow_symlinks=False)
            _log("DEBUG", f"Updated movie dir mtime: {movie_dir} -> {dateadded_iso}")
        except Exception as e:
            _log("WARNING", f"Failed to update mtime on {movie_dir}: {e}")
            
    except Exception as e:
        _log("WARNING", f"Bad timestamp for movie mtimes: {dateadded_iso}: {e}")

def apply_movie(movie_dir: Path, conn: sqlite3.Connection):
    """Apply date caching to a movie directory"""
    imdb_id = parse_imdb_from_movie_path(movie_dir)
    if not imdb_id:
        _log("ERROR", f"Movie path does not contain an IMDb id [imdb-tt...]: {movie_dir}")
        return

    ensure_movie_tables(conn)
    
    # Store/refresh movie row
    conn.execute(
        "INSERT INTO movies(imdb_id, movie_path, last_applied) VALUES(?,?,?) "
        "ON CONFLICT(imdb_id) DO UPDATE SET movie_path=excluded.movie_path, last_applied=excluded.last_applied",
        (imdb_id, str(movie_dir), _iso_now()),
    )
    
    # Check if movie has video files
    video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
    has_video_files = any(f.is_file() and f.suffix.lower() in video_exts for f in movie_dir.iterdir())
    
    if not has_video_files:
        _log("WARNING", f"No video files found in movie directory: {movie_dir}")
        conn.execute(
            "INSERT INTO movie_dates(imdb_id, released, dateadded, source, has_video_file) VALUES(?,?,?,?,?) "
            "ON CONFLICT(imdb_id) DO UPDATE SET has_video_file=excluded.has_video_file",
            (imdb_id, None, None, None, 0),
        )
        conn.commit()
        return

    # For movies, we'll use simpler logic - primarily file mtime as dateadded
    # Get the newest video file mtime as our dateadded
    newest_mtime = None
    for f in movie_dir.iterdir():
        if f.is_file() and f.suffix.lower() in video_exts:
            try:
                mtime = f.stat().st_mtime
                if newest_mtime is None or mtime > newest_mtime:
                    newest_mtime = mtime
            except Exception:
                continue
    
    dateadded_iso = None
    if newest_mtime:
        try:
            dateadded_iso = datetime.fromtimestamp(newest_mtime, tz=timezone.utc).isoformat(timespec="seconds")
        except Exception:
            pass
    
    released_iso = None
    source = "file:mtime"
    
    # Store in database
    conn.execute(
        "INSERT INTO movie_dates(imdb_id, released, dateadded, source, has_video_file) VALUES(?,?,?,?,?) "
        "ON CONFLICT(imdb_id) DO UPDATE SET released=excluded.released, dateadded=excluded.dateadded, source=excluded.source, has_video_file=excluded.has_video_file",
        (imdb_id, released_iso, dateadded_iso, source, 1),
    )
    
    # Create NFO if requested
    if config.manage_nfo:
        write_movie_nfo(movie_dir, imdb_id, released_iso, dateadded_iso, source, config.lock_metadata)
    
    # Update file times if requested
    if config.fix_dir_mtimes and dateadded_iso:
        set_movie_file_mtime(movie_dir, dateadded_iso)
    
    conn.commit()
    _log("INFO", f"Processed movie: {movie_dir.name}")

# Thread-safe batching mechanism (updated for both TV and movies)
class WebhookBatcher:
    def __init__(self):
        self.pending_items: Dict[str, Dict] = {}  # key -> latest webhook data
        self.timers: Dict[str, threading.Timer] = {}  # key -> timer
        self.processing: Set[str] = set()  # Currently processing items
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=config.max_concurrent_series, thread_name_prefix="media-processor")
        
    def add_webhook(self, key: str, webhook_data: Dict, media_type: str):
        """Add webhook to batch, resetting timer if needed"""
        with self.lock:
            # Cancel existing timer if present
            if key in self.timers:
                self.timers[key].cancel()
                _log("DEBUG", f"Cancelled existing timer for {key}")
            
            # Store/update webhook data
            webhook_data['media_type'] = media_type  # Add media type to data
            self.pending_items[key] = webhook_data
            _log("INFO", f"Batched {media_type} webhook for {key} (delay: {config.batch_delay}s)")
            
            # Create new timer
            timer = threading.Timer(config.batch_delay, self._process_item, args=[key])
            self.timers[key] = timer
            timer.start()
    
    def _process_item(self, key: str):
        """Process an item after batch delay"""
        with self.lock:
            if key in self.processing or key not in self.pending_items:
                return
            self.processing.add(key)
            webhook_data = self.pending_items.pop(key)
            self.timers.pop(key, None)
        
        try:
            media_type = webhook_data.get('media_type', 'unknown')
            _log("INFO", f"Processing batched {media_type}: {key}")
            self.executor.submit(self._process_item_sync, key, webhook_data)
        except Exception as e:
            _log("ERROR", f"Error submitting {media_type} processing for {key}: {e}")
            with self.lock:
                self.processing.discard(key)
    
    def _process_item_sync(self, key: str, webhook_data: Dict):
        """Synchronous item processing with dedicated DB connection"""
        try:
            media_type = webhook_data.get('media_type', 'unknown')
            if media_type == 'tv':
                self._process_tv_sync(key, webhook_data)
            elif media_type == 'movie':
                self._process_movie_sync(key, webhook_data)
            else:
                _log("ERROR", f"Unknown media type: {media_type}")
        finally:
            with self.lock:
                self.processing.discard(key)
    
    def _process_tv_sync(self, imdb_id: str, webhook_data: Dict):
        """Process TV series"""
        try:
            series_info = webhook_data.get('series', {})
            series_title = series_info.get("title", "Unknown")
            series_path_str = webhook_data.get('series_path')
            
            if not series_path_str:
                _log("ERROR", f"No series path found for {imdb_id}")
                return
                
            series_path = Path(series_path_str)
            if not series_path.exists():
                _log("ERROR", f"Series path does not exist: {series_path}")
                return
            
            conn = None
            try:
                conn = db_connect(db_path)
                conn.execute("PRAGMA busy_timeout = 30000")
                conn.execute("PRAGMA journal_mode = WAL")
                
                class MockArgs:
                    def __init__(self):
                        self.manage_nfo = config.manage_nfo
                        self.fix_dir_mtimes = config.fix_dir_mtimes
                        self.lock_metadata = config.lock_metadata
                        self.use_cache = True
                        self.force_refresh = False
                        self.skip_if_current = False
                        self.debug = config.debug
                        self.update_video_mtimes = True
                
                _log("INFO", f"Processing TV series: {series_title} at {series_path}")
                apply_series(series_path, MockArgs(), conn)
                _log("INFO", f"Successfully processed batched TV series: {series_title}")
                
            except Exception as e:
                _log("ERROR", f"Error processing TV series {series_title}: {e}")
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
        except Exception as e:
            _log("ERROR", f"Error in TV processing: {e}")
    
    def _process_movie_sync(self, imdb_id: str, webhook_data: Dict):
        """Process movie"""
        try:
            movie_info = webhook_data.get('movie', {})
            movie_title = movie_info.get("title", "Unknown")
            movie_path_str = webhook_data.get('movie_path')
            
            if not movie_path_str:
                _log("ERROR", f"No movie path found for {imdb_id}")
                return
                
            movie_path = Path(movie_path_str)
            if not movie_path.exists():
                _log("ERROR", f"Movie path does not exist: {movie_path}")
                return
            
            conn = None
            try:
                conn = db_connect(db_path)
                conn.execute("PRAGMA busy_timeout = 30000")
                conn.execute("PRAGMA journal_mode = WAL")
                
                _log("INFO", f"Processing movie: {movie_title} at {movie_path}")
                apply_movie(movie_path, conn)
                _log("INFO", f"Successfully processed batched movie: {movie_title}")
                
            except Exception as e:
                _log("ERROR", f"Error processing movie {movie_title}: {e}")
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
        except Exception as e:
            _log("ERROR", f"Error in movie processing: {e}")
    
    def get_status(self) -> Dict:
        """Get current batching status"""
        with self.lock:
            return {
                "pending_items": list(self.pending_items.keys()),
                "processing_items": list(self.processing),
                "pending_count": len(self.pending_items),
                "processing_count": len(self.processing),
                "executor_active_threads": getattr(self.executor, '_threads', 0)
            }

# Global batcher instance
batcher = WebhookBatcher()

def find_series_path_from_sonarr_path(sonarr_path: str) -> Optional[Path]:
   """Convert Sonarr's full path to container path"""
   if not sonarr_path:
       return None
       
   if sonarr_path.startswith("/mnt/unionfs/Media/TV/"):
       container_path = sonarr_path.replace("/mnt/unionfs/Media/TV/", "/media/")
       path = Path(container_path)
       if path.exists():
           _log("INFO", f"Found series path via Sonarr path: {path}")
           return path
   
   return None

def find_movie_path_from_radarr_path(radarr_path: str) -> Optional[Path]:
   """Convert Radarr's full path (folderPath or path) to container path"""
   if not radarr_path:
       return None
   if radarr_path.startswith("/mnt/unionfs/Media/Movies/"):
       container_path = radarr_path.replace("/mnt/unionfs/Media/Movies/", "/media/")
       path = Path(container_path)
       if path.exists():
           _log("INFO", f"Found movie path via Radarr path: {path}")
           return path
   return None

def find_series_path(series_title: str, imdb_id: str, sonarr_path: str = None) -> Optional[Path]:
   """Find the series directory based on title, IMDb ID, and optional Sonarr path"""
   
   if sonarr_path:
       series_path = find_series_path_from_sonarr_path(sonarr_path)
       if series_path:
           return series_path
   
   if imdb_id and not imdb_id.startswith("tt"):
       imdb_id = f"tt{imdb_id}"
   
   _log("DEBUG", f"Searching for series: {series_title} with IMDb ID: {imdb_id}")
   
   for media_path in config.tv_paths:
       if not media_path.exists():
           _log("WARNING", f"TV path does not exist: {media_path}")
           continue
           
       if imdb_id:
           pattern = str(media_path / f"*[imdb-{imdb_id}]*")
           matches = glob.glob(pattern)
           if matches:
               found_path = Path(matches[0])
               _log("INFO", f"Found series directory by IMDb ID: {found_path}")
               return found_path
       
       if series_title:
           series_title_clean = series_title.lower().replace(" ", "").replace("-", "").replace(":", "")
           for item in media_path.iterdir():
               if item.is_dir() and "[imdb-" in item.name.lower():
                   item_clean = item.name.lower().replace(" ", "").replace("-", "").replace(":", "")
                   if series_title_clean in item_clean:
                       _log("INFO", f"Found series directory by title: {item}")
                       return item
   
   _log("WARNING", f"Could not find directory for series: {series_title} ({imdb_id})")
   return None

def find_movie_path(movie_title: str, imdb_id: str, radarr_path: str = None) -> Optional[Path]:
   """Find the movie directory based on title, IMDb ID, and optional Radarr path"""
   
   if radarr_path:
       movie_path = find_movie_path_from_radarr_path(radarr_path)
       if movie_path:
           return movie_path
   
   if imdb_id and not imdb_id.startswith("tt"):
       imdb_id = f"tt{imdb_id}"
   
   _log("DEBUG", f"Searching for movie: {movie_title} with IMDb ID: {imdb_id}")
   
   for media_path in config.movie_paths:
       if not media_path.exists():
           _log("WARNING", f"Movie path does not exist: {media_path}")
           continue
           
       if imdb_id:
           pattern = str(media_path / f"*[imdb-{imdb_id}]*")
           matches = glob.glob(pattern)
           if matches:
               found_path = Path(matches[0])
               _log("INFO", f"Found movie directory by IMDb ID: {found_path}")
               return found_path
       
       if movie_title:
           movie_title_clean = movie_title.lower().replace(" ", "").replace("-", "").replace(":", "")
           for item in media_path.iterdir():
               if item.is_dir() and "[imdb-" in item.name.lower():
                   item_clean = item.name.lower().replace(" ", "").replace("-", "").replace(":", "")
                   if movie_title_clean in item_clean:
                       _log("INFO", f"Found movie directory by title: {item}")
                       return item
   
   _log("WARNING", f"Could not find directory for movie: {movie_title} ({imdb_id})")
   return None

async def process_sonarr_import(webhook_data: SonarrWebhook):
   """Process a Sonarr import webhook by adding to batch"""
   try:
       if not webhook_data.series:
           _log("WARNING", f"No series data in {webhook_data.eventType} webhook")
           return
           
       series_info = webhook_data.series
       series_title = series_info.get("title", "Unknown")
       imdb_id = series_info.get("imdbId", "")
       sonarr_path = series_info.get("path", "")
       
       if imdb_id:
           imdb_id = imdb_id.replace("tt", "").strip()
           if imdb_id:
               imdb_id = f"tt{imdb_id}"
       
       if not imdb_id:
           _log("ERROR", f"No IMDb ID found for series: {series_title}")
           return
       
       _log("INFO", f"Received webhook for series: {series_title} (IMDb: {imdb_id})")
       
       series_path = find_series_path(series_title, imdb_id, sonarr_path)
       if not series_path:
           _log("ERROR", f"Could not find series directory for: {series_title} ({imdb_id})")
           return
       
       webhook_dict = {
           'series': series_info,
           'series_path': str(series_path),
           'event_type': webhook_data.eventType
       }
       
       batcher.add_webhook(imdb_id, webhook_dict, 'tv')
           
   except Exception as e:
       _log("ERROR", f"Error processing Sonarr webhook: {e}")
       raise

async def process_radarr_import(webhook_data: RadarrWebhook):
   """Process a Radarr import webhook by adding to batch"""
   try:
       if not webhook_data.movie:
           _log("WARNING", f"No movie data in {webhook_data.eventType} webhook")
           return
           
       movie_info = webhook_data.movie
       movie_title = movie_info.get("title", "Unknown")

       # Prefer folderPath for Rename payloads; fall back to path
       radarr_path = movie_info.get("folderPath") or movie_info.get("path", "")

       # imdbId may be missing on Rename; fall back to remoteMovie or parse from folder
       imdb_id = (movie_info.get("imdbId") or (webhook_data.remoteMovie or {}).get("imdbId") or "").strip()
       if imdb_id:
           imdb_id = f"tt{imdb_id.replace('tt','')}"
       
       movie_path = find_movie_path(movie_title, imdb_id, radarr_path)
       if not imdb_id and movie_path:
           # Try to parse from the resolved folder name
           parsed = parse_imdb_from_movie_path(movie_path)
           if parsed:
               imdb_id = parsed

       if not imdb_id:
           _log("WARNING", f"No IMDb ID available for movie '{movie_title}'. Proceeding by path only.")

       _log("INFO", f"Received webhook for movie: {movie_title} (IMDb: {imdb_id or 'unknown'})")
       if getattr(webhook_data, "renamedMovieFiles", None):
           _log("INFO", f"Renamed {len(webhook_data.renamedMovieFiles)} file(s) for {movie_title}")

       if not movie_path:
           _log("ERROR", f"Could not find movie directory for: {movie_title} ({imdb_id or 'unknown'})")
           return
       
       webhook_dict = {
           'movie': movie_info,
           'movie_path': str(movie_path),
           'event_type': webhook_data.eventType
       }
       
       batcher.add_webhook(imdb_id or str(movie_path), webhook_dict, 'movie')
           
   except Exception as e:
       _log("ERROR", f"Error processing Radarr webhook: {e}")
       raise

@app.post("/webhook/sonarr")
async def sonarr_webhook(request: Request, background_tasks: BackgroundTasks):
   """Handle Sonarr webhooks"""
   try:
       webhook_data = await request.json()
       _log("DEBUG", f"Raw Sonarr webhook payload: {webhook_data}")
       
       webhook = SonarrWebhook(**webhook_data)
       _log("INFO", f"Received Sonarr webhook: {webhook.eventType}")
       
       if webhook.eventType not in ["Download", "Upgrade", "Rename"]:
           return {"status": "ignored", "reason": f"Event type {webhook.eventType} not processed"}
       
       await process_sonarr_import(webhook)
       return {"status": "accepted", "message": "Sonarr webhook queued for batched processing"}
       
   except Exception as e:
       _log("ERROR", f"Failed to parse Sonarr webhook: {e}")
       raise HTTPException(status_code=422, detail=f"Invalid Sonarr webhook format: {e}")

@app.post("/webhook/radarr")
async def radarr_webhook(request: Request, background_tasks: BackgroundTasks):
   """Handle Radarr webhooks"""
   try:
       webhook_data = await request.json()
       _log("DEBUG", f"Raw Radarr webhook payload: {webhook_data}")
       
       webhook = RadarrWebhook(**webhook_data)
       _log("INFO", f"Received Radarr webhook: {webhook.eventType}")
       
       # Allow common movie-file events; Rename payload has folderPath/renamedMovieFiles
       if webhook.eventType not in ["Download", "Upgrade", "Rename", "MovieFileDelete", "MovieDelete"]:
           return {"status": "ignored", "reason": f"Event type {webhook.eventType} not processed"}
       
       await process_radarr_import(webhook)
       return {"status": "accepted", "message": "Radarr webhook queued for batched processing"}
       
   except Exception as e:
       _log("ERROR", f"Failed to parse Radarr webhook: {e}")
       raise HTTPException(status_code=422, detail=f"Invalid Radarr webhook format: {e}")

@app.get("/health")
async def health_check() -> HealthResponse:
   """Health check endpoint"""
   uptime = datetime.now(timezone.utc) - start_time
   
   # Check database connectivity
   try:
       conn = db_connect(db_path)
       conn.execute("PRAGMA busy_timeout = 5000")
       conn.execute("SELECT 1").fetchone()
       conn.close()
       db_status = "healthy"
   except Exception as e:
       db_status = f"error: {e}"
   
   return HealthResponse(
       status="healthy" if db_status == "healthy" else "degraded",
       version="1.0.0",
       uptime=str(uptime),
       database_status=db_status
   )

@app.get("/stats")
async def get_stats():
   """Get database statistics"""
   try:
       conn = db_connect(db_path)
       conn.execute("PRAGMA busy_timeout = 5000")
       
       # Ensure movie tables exist
       ensure_movie_tables(conn)
       
       # Get TV series count
       tv_series_count = conn.execute("SELECT COUNT(*) FROM series").fetchone()[0]
       
       # Get TV episode count
       tv_episode_count = conn.execute("SELECT COUNT(*) FROM episode_dates").fetchone()[0]
       
       # Get TV episodes with video files
       tv_with_files = conn.execute("SELECT COUNT(*) FROM episode_dates WHERE has_video_file = 1").fetchone()[0]
       
       # Get movie count
       movie_count = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
       
       # Get movies with video files
       movies_with_files = conn.execute("SELECT COUNT(*) FROM movie_dates WHERE has_video_file = 1").fetchone()[0]
       
       # Get TV source breakdown
       tv_sources = conn.execute("""
           SELECT source, COUNT(*) as count 
           FROM episode_dates 
           WHERE has_video_file = 1
           GROUP BY source 
           ORDER BY count DESC
       """).fetchall()
       
       # Get movie source breakdown
       movie_sources = conn.execute("""
           SELECT source, COUNT(*) as count 
           FROM movie_dates 
           WHERE has_video_file = 1
           GROUP BY source 
           ORDER BY count DESC
       """).fetchall()
       
       conn.close()
       
       return {
           "tv_series_count": tv_series_count,
           "tv_episode_count": tv_episode_count,
           "tv_episodes_with_files": tv_with_files,
           "movie_count": movie_count,
           "movies_with_files": movies_with_files,
           "tv_sources": dict(tv_sources),
           "movie_sources": dict(movie_sources)
       }
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@app.get("/batch/status")
async def get_batch_status():
   """Get current batching status"""
   return batcher.get_status()

def iterate_movie_paths():
   """Find all movie directories in configured paths"""
   movies = []
   for movie_path in config.movie_paths:
       if not movie_path.exists():
           continue
       for item in movie_path.iterdir():
           if is_movie_dir(item):
               movies.append(item)
   return sorted(movies)

def run_manual_scan_sync(scan_paths: List[Path], scan_type: str = "both"):
   """Synchronous function for manual scan - runs in executor"""
   from media_date_cache import iterate_target_path
   
   class MockArgs:
       def __init__(self):
           self.manage_nfo = config.manage_nfo
           self.fix_dir_mtimes = config.fix_dir_mtimes
           self.lock_metadata = config.lock_metadata
           self.use_cache = True
           self.force_refresh = False
           self.skip_if_current = True  # Skip current files in manual scan
           self.debug = config.debug
           self.update_video_mtimes = True
   
   conn = None
   try:
       conn = db_connect(db_path)
       conn.execute("PRAGMA busy_timeout = 30000")
       conn.execute("PRAGMA journal_mode = WAL")
       ensure_movie_tables(conn)
       
       for scan_path in scan_paths:
           _log("INFO", f"Starting manual scan of: {scan_path}")
           
           if scan_type in ["both", "tv"]:
               # Scan TV series
               tv_targets = iterate_target_path(scan_path)
               for series_dir in tv_targets:
                   try:
                       apply_series(series_dir, MockArgs(), conn)
                   except Exception as e:
                       _log("ERROR", f"Failed processing TV series {series_dir}: {e}")
           
           if scan_type in ["both", "movies"]:
               # Scan movies
               if scan_path in config.movie_paths:
                   for item in scan_path.iterdir():
                       if is_movie_dir(item):
                           try:
                               apply_movie(item, conn)
                           except Exception as e:
                               _log("ERROR", f"Failed processing movie {item}: {e}")
           
           _log("INFO", f"Completed manual scan of: {scan_path}")
   finally:
       if conn:
           try:
               conn.close()
           except:
               pass

@app.post("/manual/scan")
async def manual_scan(background_tasks: BackgroundTasks, path: Optional[str] = None, scan_type: str = "both"):
   """Trigger a manual library scan"""
   if path:
       scan_paths = [Path(path)]
   else:
       # Scan both TV and movie paths
       scan_paths = list(config.tv_paths) + list(config.movie_paths)
   
   # Validate scan_type
   if scan_type not in ["both", "tv", "movies"]:
       raise HTTPException(status_code=400, detail="scan_type must be 'both', 'tv', or 'movies'")
   
   # Run in executor
   async def run_scan():
       await asyncio.get_event_loop().run_in_executor(None, run_manual_scan_sync, scan_paths, scan_type)
   
   background_tasks.add_task(run_scan)
   
   return {"status": "started", "message": f"Manual {scan_type} scan started for {len(scan_paths)} paths"}

@app.post("/manual/scan/tv")
async def manual_scan_tv(background_tasks: BackgroundTasks, path: Optional[str] = None):
   """Trigger a manual TV library scan"""
   return await manual_scan(background_tasks, path, "tv")

@app.post("/manual/scan/movies")
async def manual_scan_movies(background_tasks: BackgroundTasks, path: Optional[str] = None):
   """Trigger a manual movie library scan"""
   return await manual_scan(background_tasks, path, "movies")

@app.get("/test/path/{media_title}")
async def test_path_matching(media_title: str, imdb_id: Optional[str] = None, 
                           media_type: str = "tv", app_path: Optional[str] = None):
   """Test endpoint to debug path matching"""
   if media_type == "tv":
       result = find_series_path(media_title, imdb_id or "", app_path or "")
       paths = config.tv_paths
   elif media_type == "movie":
       result = find_movie_path(media_title, imdb_id or "", app_path or "")
       paths = config.movie_paths
   else:
       raise HTTPException(status_code=400, detail="media_type must be 'tv' or 'movie'")
   
   return {
       "media_title": media_title,
       "media_type": media_type,
       "imdb_id": imdb_id,
       "app_path": app_path,
       "search_paths": [str(p) for p in paths],
       "found_path": str(result) if result else None,
       "exists": result.exists() if result else False
   }

if __name__ == "__main__":
   # Enable debug logging if requested
   if config.debug:
       import logging
       logging.basicConfig(level=logging.DEBUG)
       os.environ["DEBUG"] = "true"
   
   _log("INFO", "Starting Media Date Cache Webhook Server with TV and Movie support")
   _log("INFO", f"TV paths: {[str(p) for p in config.tv_paths]}")
   _log("INFO", f"Movie paths: {[str(p) for p in config.movie_paths]}")
   _log("INFO", f"Database path: {db_path}")
   _log("INFO", f"Manage NFO: {config.manage_nfo}")
   _log("INFO", f"Fix dir mtimes: {config.fix_dir_mtimes}")
   _log("INFO", f"Lock metadata: {config.lock_metadata}")
   _log("INFO", f"Batch delay: {config.batch_delay}s")
   _log("INFO", f"Max concurrent items: {config.max_concurrent_series}")
   
   uvicorn.run(
       "webhook_server:app",
       host="0.0.0.0", 
       port=int(os.environ.get("PORT", "8080")),
       reload=False
   )

