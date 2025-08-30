#!/usr/bin/env python3
# app.py - NFOGuard Webhook Server (TV + Movies)
#
# Features:
# - TV via existing media_date_cache.apply_series (Sonarr webhooks + manual scan)
# - Movies with robust <dateadded> logic and NFO upsert:
#   * Priority configurable: import_then_digital | digital_then_import
#   * Poll policy: always | if_missing | never
#   * Digital date candidates: Radarr, TMDB, Jellyseerr (optional), OMDb (optional) -> pick earliest
#   * Digital sanity check (re-release guard): compare to cinema/import/min and fallback
#   * Update policy: backfill_only | overwrite (+ preserve flags in backfill mode)
#   * Pretty XML, ensures <dateadded>, <lockdata>, <premiered>, <year>, footer comments, uniqueid imdb
#   * Optional mtimes update to match chosen <dateadded>
#
# Endpoints:
# - POST /webhook/sonarr
# - POST /webhook/radarr
# - POST /manual/scan            (scan_type=both|tv|movies, optional path=...)
# - POST /manual/scan/tv
# - POST /manual/scan/movies
# - GET  /health
# - GET  /stats
# - GET  /batch/status
#
# Env (highlights):
#   MANAGER_BRAND (default NFOGuard)
#   MOVIE_PRIORITY=import_then_digital|digital_then_import
#   MOVIE_POLL_MODE=always|if_missing|never
#   MOVIE_DATE_UPDATE_MODE=backfill_only|overwrite
#   MOVIE_PRESERVE_RADARR_IMPORT=true|false
#   MOVIE_PRESERVE_EXISTING=true|false
#   MOVIE_DIGITAL_SANITY_CHECK=true|false
#   MOVIE_DIGITAL_SANITY_REF=cinema|import|min
#   MOVIE_DIGITAL_MAX_LAG_DAYS=365
#   MOVIE_DIGITAL_INVALID_FALLBACK=import|cinema
#   TMDB_API_KEY / TMDB_PRIMARY_COUNTRY
#   OMDB_API_KEY
#   JELLYSEERR_URL / JELLYSEERR_API_KEY
#   RADARR_URL / RADARR_API_KEY / RADARR_TIMEOUT / RADARR_RETRIES
#   SONARR_URL / SONARR_API_KEY  (TV pathing only; series handled in media_date_cache)
#   TV_PATHS, MOVIE_PATHS
#   MANAGE_NFO, FIX_DIR_MTIMES, LOCK_METADATA, DEBUG
#   BATCH_DELAY, MAX_CONCURRENT_SERIES
#
# Run: uvicorn app:app --host 0.0.0.0 --port 8080

import os
import json
import asyncio
import glob
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Set, Tuple
import sqlite3
import uvicorn
import threading
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET
from urllib.parse import urlencode, urljoin, quote
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import URLError, HTTPError
import time

# Import TV helpers from your existing module
from media_date_cache import (
    db_connect, apply_series, parse_imdb_from_path,
    _log, _iso_now, SonarrClient
)

# ---------------------------
# Models
# ---------------------------

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
        extra = "allow"

class RadarrWebhook(BaseModel):
    eventType: str
    movie: Optional[Dict[str, Any]] = None
    movieFile: Optional[Dict[str, Any]] = None
    isUpgrade: Optional[bool] = False
    deletedFiles: Optional[list] = []
    remoteMovie: Optional[Dict[str, Any]] = None
    renamedMovieFiles: Optional[List[Dict[str, Any]]] = None  # on Rename

    class Config:
        extra = "allow"

class HealthResponse(BaseModel):
    status: str
    version: str
    uptime: str
    database_status: str

# ---------------------------
# App & Config
# ---------------------------

app = FastAPI(
    title="NFOGuard Webhook Server",
    description="Locks true import dates; manages NFOs and mtimes for TV & Movies",
    version="1.7.0",
)

start_time = datetime.now(timezone.utc)
db_path = Path(os.environ.get("DB_PATH", "/app/data/media_dates.db"))

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _bool_env(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return _norm(v) in ("1", "true", "yes", "y", "on")

class WebhookConfig:
    def __init__(self):
        tv_paths_str = os.environ.get("TV_PATHS", os.environ.get("MEDIA_PATH", "/media/tv,/media/tv6"))
        self.tv_paths = [Path(p.strip()) for p in tv_paths_str.split(",") if p.strip()]

        movie_paths_str = os.environ.get("MOVIE_PATHS", "/media/movies,/media/movies6")
        self.movie_paths = [Path(p.strip()) for p in movie_paths_str.split(",") if p.strip()]

        self.manage_nfo = _bool_env("MANAGE_NFO", True)
        self.fix_dir_mtimes = _bool_env("FIX_DIR_MTIMES", True)
        self.lock_metadata = _bool_env("LOCK_METADATA", True)
        self.debug = _bool_env("DEBUG", False)

        self.batch_delay = float(os.environ.get("BATCH_DELAY", "5.0"))
        self.max_concurrent_series = int(os.environ.get("MAX_CONCURRENT_SERIES", "3"))

        self.manager_brand = os.environ.get("MANAGER_BRAND", "NFOGuard")

        # Radarr
        self.radarr_url = (os.environ.get("RADARR_URL", "") or "").rstrip("/")
        self.radarr_api_key = os.environ.get("RADARR_API_KEY", "") or ""
        self.radarr_timeout = int(os.environ.get("RADARR_TIMEOUT", "45"))
        self.radarr_retries = int(os.environ.get("RADARR_RETRIES", "3"))

        # Optional externals
        self.tmdb_api_key = os.environ.get("TMDB_API_KEY") or ""
        self.tmdb_country = (os.environ.get("TMDB_PRIMARY_COUNTRY") or "US").upper()
        self.omdb_api_key = os.environ.get("OMDB_API_KEY") or ""
        self.jelly_url = (os.environ.get("JELLYSEERR_URL") or "").rstrip("/")
        self.jelly_api = os.environ.get("JELLYSEERR_API_KEY") or ""

        # Legacy (logged only)
        raw_strategy = _norm(os.environ.get("MOVIE_DATE_STRATEGY", "radarr_import"))
        aliases = {
            "import": "radarr_import",
            "radarr": "radarr_import",
            "digital_release": "digital",
            "physical_release": "physical",
            "in_cinemas": "cinema",
            "theatrical": "cinema",
            "mtime": "file_mtime",
        }
        self.movie_date_strategy = aliases.get(raw_strategy, raw_strategy)

        # Priority
        self.movie_priority = _norm(os.environ.get("MOVIE_PRIORITY", "import_then_digital"))
        if self.movie_priority not in ("import_then_digital", "digital_then_import"):
            self.movie_priority = "import_then_digital"

        # Poll policy
        self.movie_poll_mode = _norm(os.environ.get("MOVIE_POLL_MODE", "always"))  # always|if_missing|never
        if self.movie_poll_mode not in ("always", "if_missing", "never"):
            self.movie_poll_mode = "always"

        # Update policy
        self.movie_date_update_mode = _norm(os.environ.get("MOVIE_DATE_UPDATE_MODE", "backfill_only"))
        self.movie_preserve_radarr_import = _bool_env("MOVIE_PRESERVE_RADARR_IMPORT", True)
        self.movie_preserve_existing = _bool_env("MOVIE_PRESERVE_EXISTING", True)

        # Digital sanity check
        self.movie_digital_sanity_check = _bool_env("MOVIE_DIGITAL_SANITY_CHECK", False)
        self.movie_digital_max_lag_days = int(os.environ.get("MOVIE_DIGITAL_MAX_LAG_DAYS", "365"))
        self.movie_digital_sanity_ref = _norm(os.environ.get("MOVIE_DIGITAL_SANITY_REF", "cinema"))  # cinema|import|min
        if self.movie_digital_sanity_ref not in ("cinema", "import", "min"):
            self.movie_digital_sanity_ref = "cinema"
        self.movie_digital_invalid_fallback = _norm(os.environ.get("MOVIE_DIGITAL_INVALID_FALLBACK", "import"))  # import|cinema
        if self.movie_digital_invalid_fallback not in ("import", "cinema"):
            self.movie_digital_invalid_fallback = "import"

config = WebhookConfig()

# ---------------------------
# DB schema (movies)
# ---------------------------

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
    try:
        result = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='movies'").fetchone()
        if not result:
            _log("INFO", "Creating movie tables in database")
            conn.executescript(MOVIE_SCHEMA)
            conn.commit()
    except sqlite3.Error as e:
        _log("ERROR", f"Failed to create movie tables: {e}")

# ---------------------------
# HTTP helper
# ---------------------------

def _get_json(url: str, timeout: int = 20, headers: Dict[str, str] = None) -> Optional[Any]:
    try:
        req = UrlRequest(url, headers=headers or {"Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        _log("WARNING", f"GET {url} failed: {e}")
        return None

# ---------------------------
# Radarr client
# ---------------------------

class RadarrClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = 45, retries: int = 3):
        self.base_url = base_url.rstrip("/") + "/"
        self.api_key = api_key
        self.timeout = timeout
        self.retries = max(0, retries)

    def _get(self, path: str, params: Dict[str, Any] = None) -> Optional[Any]:
        if not self.api_key:
            return None
        attempt = 0
        last_err = None
        while attempt <= self.retries:
            try:
                params = params or {}
                params["apikey"] = self.api_key
                url = urljoin(self.base_url, path.lstrip("?").lstrip("/"))
                if params:
                    url = url + ("&" if "?" in url else "?") + urlencode(params)
                req = UrlRequest(url, headers={"Accept": "application/json"})
                with urlopen(req, timeout=self.timeout) as resp:
                    data = resp.read().decode("utf-8")
                    return json.loads(data)
            except (URLError, HTTPError, json.JSONDecodeError) as e:
                last_err = e
                time.sleep(min(2 ** attempt, 5))
                attempt += 1
        _log("WARNING", f"Radarr GET {path} failed after retries: {last_err}")
        return None

    def movie_by_imdb(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        imdb_id = imdb_id if imdb_id.startswith("tt") else f"tt{imdb_id}"
        res = self._get("/api/v3/movie", {"imdbId": imdb_id})
        if isinstance(res, list) and res:
            return res[0]
        res = self._get("/api/v3/movie/lookup/imdb", {"imdbId": imdb_id})
        if isinstance(res, dict) and res.get("id"):
            return res
        return None

    def movie_files(self, movie_id: int) -> List[Dict[str, Any]]:
        res = self._get("/api/v3/moviefile", {"movieId": movie_id})
        return res if isinstance(res, list) else []

    def earliest_dateadded(self, movie_id: int) -> Optional[str]:
        files = self.movie_files(movie_id)
        best = None
        for f in files:
            da = f.get("dateAdded")
            if not da:
                continue
            try:
                dt = datetime.fromisoformat(da.replace("Z", "+00:00"))
                if best is None or dt < best:
                    best = dt
            except Exception:
                continue
        if best:
            return best.astimezone(timezone.utc).isoformat(timespec="seconds")
        return None

    def earliest_import_event(self, movie_id: int) -> Optional[str]:
        data = self._get("/api/v3/history/movie", {
            "movieId": movie_id, "page": 1, "pageSize": 1000,
            "sortKey": "date", "sortDirection": "ascending"
        })
        if not data:
            return None
        items = data if isinstance(data, list) else data.get("records") or []
        first_grab = None
        for it in items:
            ev = (it.get("eventType") or it.get("type") or "").lower()
            date_iso = None
            if it.get("date"):
                try:
                    date_iso = datetime.fromisoformat(it["date"].replace("Z", "+00:00")).astimezone(timezone.utc).isoformat(timespec="seconds")
                except Exception:
                    date_iso = None
            if ev in ("downloadfolderimported", "moviefileimported"):
                if date_iso:
                    return date_iso
            if ev == "grabbed" and date_iso and first_grab is None:
                first_grab = date_iso
        if first_grab:
            return first_grab
        return None

# ---------------------------
# TMDB / OMDb / Jellyseerr helpers
# ---------------------------

def _tmdb_id_from_imdb(imdb_id: str) -> Optional[int]:
    if not config.tmdb_api_key:
        return None
    try:
        url_find = f"https://api.themoviedb.org/3/find/{quote(imdb_id)}?{urlencode({'api_key': config.tmdb_api_key, 'external_source':'imdb_id'})}"
        j = _get_json(url_find, timeout=20)
        res = (j or {}).get("movie_results") or []
        if res:
            return res[0].get("id")
        return None
    except Exception:
        return None

def tmdb_digital_date_from_imdb(imdb_id: str) -> Optional[str]:
    if not config.tmdb_api_key:
        return None
    try:
        tmdb_id = _tmdb_id_from_imdb(imdb_id)
        if not tmdb_id:
            return None
        url_rel = f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates?{urlencode({'api_key': config.tmdb_api_key})}"
        k = _get_json(url_rel, timeout=20)
        if not k:
            return None
        for entry in (k.get("results") or []):
            if (entry.get("iso_3166_1") or "").upper() != config.tmdb_country:
                continue
            for rd in (entry.get("release_dates") or []):
                if rd.get("type") == 4 and rd.get("release_date"):  # 4 = Digital
                    try:
                        dt = datetime.fromisoformat(rd["release_date"].replace("Z", "+00:00"))
                        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
                    except Exception:
                        continue
        return None
    except Exception as e:
        _log("WARNING", f"TMDB lookup failed for {imdb_id}: {e}")
        return None

def jellyseerr_digital_date_from_tmdb(tmdb_id: int) -> Optional[str]:
    base = config.jelly_url
    api = config.jelly_api
    if not (base and api and tmdb_id):
        return None
    try:
        url = base.rstrip("/") + f"/api/v1/movie/{tmdb_id}"
        j = _get_json(url, timeout=20, headers={"X-Api-Key": api, "Accept": "application/json"})
        if not j:
            return None
        candidates = []
        for k in ("digitalReleaseDate", "physicalReleaseDate", "vodReleaseDate"):
            v = j.get(k)
            if v:
                iso = _parse_date_to_iso(v)
                if iso:
                    candidates.append(iso)
        for arr_key in ("releaseDates", "releases", "dates"):
            for rd in (j.get(arr_key) or []):
                name = (rd.get("type") or rd.get("label") or "").lower()
                v = rd.get("date") or rd.get("releaseDate")
                if v and ("digital" in name or "vod" in name or "stream" in name):
                    iso = _parse_date_to_iso(v)
                    if iso:
                        candidates.append(iso)
        if not candidates:
            return None
        return sorted(
            candidates,
            key=lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
        )[0]
    except Exception as e:
        _log("WARNING", f"Jellyseerr lookup failed for tmdb:{tmdb_id}: {e}")
        return None

def omdb_dvd_date(imdb_id: str) -> Optional[str]:
    if not config.omdb_api_key:
        return None
    try:
        url = f"http://www.omdbapi.com/?{urlencode({'i': imdb_id, 'apikey': config.omdb_api_key})}"
        j = _get_json(url, timeout=15)
        if not j or _norm(j.get("Response")) != "true":
            return None
        dvd = j.get("DVD") or j.get("Released")
        if not dvd:
            return None
        for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(dvd, fmt).replace(tzinfo=timezone.utc)
                return dt.isoformat(timespec="seconds")
            except Exception:
                continue
        return None
    except Exception as e:
        _log("WARNING", f"OMDb lookup failed for {imdb_id}: {e}")
        return None

# ---------------------------
# XML helpers (pretty + upsert)
# ---------------------------

def _ensure_child_text(parent: ET.Element, tag: str, text: str, overwrite: bool = True) -> ET.Element:
    child = parent.find(tag)
    if child is None:
        child = ET.SubElement(parent, tag)
        child.text = text
    else:
        if overwrite or (child.text is None or str(child.text).strip() == ""):
            child.text = text
    return child

def _ensure_uniqueid_imdb(root: ET.Element, imdb_id: str):
    imdb_elem = None
    for uid in root.findall("uniqueid"):
        if (uid.attrib.get("type") or "").lower() == "imdb":
            imdb_elem = uid
            break
    if imdb_elem is None:
        imdb_elem = ET.SubElement(root, "uniqueid", {"type": "imdb", "default": "true"})
    else:
        if "default" not in imdb_elem.attrib:
            imdb_elem.set("default", "true")
    imdb_elem.text = imdb_id

def _indent(elem: ET.Element, level: int = 0, space: str = "  "):
    i = "\n" + level * space
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + space
        for idx, e in enumerate(list(elem)):
            _indent(e, level + 1, space)
            if not e.tail or not e.tail.strip():
                e.tail = i + (space if idx < len(elem) - 1 else "")
        if not elem.tail or not elem.tail.strip():
            elem.tail = "\n" + (level - 1) * space if level else "\n"
    else:
        if not elem.text or not elem.text.strip():
            elem.text = ""
        if not elem.tail or not elem.tail.strip():
            elem.tail = "\n" + (level - 1) * space if level else "\n"

def _strip_existing_footer_comments(root: ET.Element):
    for node in list(root):
        if isinstance(node.tag, str):
            continue
        txt = (node.text or "").strip().lower()
        if "source:" in txt or "managed by" in txt:
            root.remove(node)

def upsert_movie_nfo(
    movie_dir: Path,
    imdb_id: str,
    released: Optional[str],
    dateadded: Optional[str],
    source_detail: str,
    lock_metadata: bool = True,
    media_label: str = "Movie",
    brand: str = "NFOGuard",
):
    canonical_path = movie_dir / "movie.nfo"
    alt_path = movie_dir / f"{movie_dir.name}.nfo"

    root = None
    for p in (canonical_path, alt_path):
        if p.exists():
            try:
                t = ET.parse(str(p))
                r = t.getroot()
                if r is not None and r.tag.lower() == "movie":
                    root = r
                    break
            except Exception:
                pass
    if root is None:
        root = ET.Element("movie")

    if imdb_id and not imdb_id.lower().startswith("tt"):
        imdb_id = f"tt{imdb_id}"

    if imdb_id:
        _ensure_uniqueid_imdb(root, imdb_id)

    if released:
        ymd = released.split("T")[0]
        _ensure_child_text(root, "premiered", ymd, overwrite=False)
        _ensure_child_text(root, "year", ymd.split("-")[0], overwrite=False)

    if dateadded:
        _ensure_child_text(root, "dateadded", dateadded, overwrite=True)

    if lock_metadata:
        _ensure_child_text(root, "lockdata", "true", overwrite=True)

    _strip_existing_footer_comments(root)
    root.append(ET.Comment(f" source: {media_label}; {source_detail} "))
    root.append(ET.Comment(f" managed by {brand} "))

    _indent(root)

    for target in [canonical_path] + ([alt_path] if alt_path.exists() else []):
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            ET.ElementTree(root).write(str(target), encoding="utf-8", xml_declaration=True)
            _log("DEBUG", f"Upserted NFO: {target}")
        except Exception as e:
            _log("ERROR", f"Failed writing {target}: {e}")

# ---------------------------
# Date selection
# ---------------------------

def _parse_date_to_iso(d: str) -> Optional[str]:
    if not d:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
            dt = datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
            return dt.isoformat(timespec="seconds")
        dt = datetime.fromisoformat(d.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return None

def _radarr_movie_by_imdb(imdb_id: str) -> Optional[Dict[str, Any]]:
    if not (config.radarr_url and config.radarr_api_key):
        return None
    rc = RadarrClient(config.radarr_url, config.radarr_api_key, config.radarr_timeout, config.radarr_retries)
    return rc.movie_by_imdb(imdb_id)

def _radarr_earliest_import_history_iso(movie_id: int) -> Optional[str]:
    rc = RadarrClient(config.radarr_url, config.radarr_api_key, config.radarr_timeout, config.radarr_retries)
    return rc.earliest_import_event(movie_id)

def _radarr_earliest_file_dateadded_iso(movie_id: int) -> Optional[str]:
    rc = RadarrClient(config.radarr_url, config.radarr_api_key, config.radarr_timeout, config.radarr_retries)
    return rc.earliest_dateadded(movie_id)

def _strategy_movie_date_from_movie_obj(movie_obj: Dict[str, Any], which: str) -> Optional[str]:
    key_map = {
        "digital": ["digitalRelease", "digitalReleaseDate"],
        "physical": ["physicalRelease", "physicalReleaseDate"],
        "cinema": ["inCinemas", "inCinema", "theatricalRelease"],
    }
    for k in key_map.get(which, []):
        val = movie_obj.get(k)
        iso = _parse_date_to_iso(val) if val else None
        if iso:
            return iso
    return None

def _collect_digital_candidates(imdb_id: str, movie_obj: Optional[Dict[str, Any]]) -> List[Tuple[str, str]]:
    cands: List[Tuple[str, str]] = []
    if movie_obj:
        iso = _strategy_movie_date_from_movie_obj(movie_obj, "digital")
        if iso:
            cands.append((iso, "radarr:digitalRelease"))
    iso = tmdb_digital_date_from_imdb(imdb_id)
    if iso:
        cands.append((iso, "tmdb:digital"))
    tmdb_id = _tmdb_id_from_imdb(imdb_id)
    if tmdb_id:
        iso = jellyseerr_digital_date_from_tmdb(tmdb_id)
        if iso:
            cands.append((iso, "jellyseerr:digital"))
    iso = omdb_dvd_date(imdb_id)
    if iso:
        cands.append((iso, "omdb:dvd"))
    return cands

def _pick_earliest(cands: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    if not cands:
        return None
    try:
        return sorted(
            cands,
            key=lambda t: datetime.fromisoformat(t[0].replace("Z", "+00:00"))
        )[0]
    except Exception:
        return cands[0]

def _decide_movie_date(imdb_id: str, movie_dir: Path, poll_external: bool) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Decide <dateadded> and supporting 'released' (premiered/year) for NFO.

    Returns: (dateadded_iso, source_detail, released_iso)
    """
    released_iso: Optional[str] = None
    movie_obj: Optional[Dict[str, Any]] = None
    movie_id: Optional[int] = None

    def from_file_mtime() -> Tuple[Optional[str], str]:
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        newest = None
        for f in movie_dir.iterdir():
            if f.is_file() and f.suffix.lower() in video_exts:
                try:
                    mt = f.stat().st_mtime
                    if newest is None or mt > newest:
                        newest = mt
                except Exception:
                    continue
        if newest:
            try:
                iso = datetime.fromtimestamp(newest, tz=timezone.utc).isoformat(timespec="seconds")
                return iso, "file:mtime"
            except Exception:
                pass
        return None, "file:mtime"

    def ensure_movie():
        nonlocal movie_obj, movie_id, released_iso
        if movie_obj is None and poll_external and config.radarr_url and config.radarr_api_key:
            movie_obj = _radarr_movie_by_imdb(imdb_id)
            if movie_obj:
                movie_id = movie_obj.get("id")
                released_iso = _parse_date_to_iso(movie_obj.get("inCinemas") or movie_obj.get("theatricalRelease"))

    def from_import_history() -> Tuple[Optional[str], str]:
        ensure_movie()
        if poll_external and movie_id:
            iso = _radarr_earliest_import_history_iso(movie_id)
            if iso:
                return iso, "radarr:history.import"
        if poll_external and movie_id:
            iso2 = _radarr_earliest_file_dateadded_iso(movie_id)
            if iso2:
                return iso2, "radarr:moviefile.dateAdded"
        return None, "radarr:history"

    def from_radarr_field(which: str) -> Tuple[Optional[str], str]:
        ensure_movie()
        label_map = {"digital": "radarr:digitalRelease", "physical": "radarr:physicalRelease", "cinema": "radarr:inCinemas"}
        if poll_external and movie_obj:
            iso = _strategy_movie_date_from_movie_obj(movie_obj, which)
            return iso, label_map.get(which, "radarr:field")
        return None, label_map.get(which, "radarr:field")

    def from_external_digital_bundle() -> Tuple[Optional[str], str]:
        ensure_movie()
        cands = _collect_digital_candidates(imdb_id, movie_obj)
        picked = _pick_earliest(cands)
        return picked if picked else (None, "digital:none")

    def maybe_sanitize_digital(digi_iso: Optional[str]) -> Tuple[Optional[str], str]:
        if not digi_iso or not config.movie_digital_sanity_check:
            return digi_iso, "radarr:digitalRelease"
        ref_dates: List[datetime] = []
        if config.movie_digital_sanity_ref in ("cinema", "min"):
            cine_iso, _ = from_radarr_field("cinema")
            if cine_iso:
                try:
                    ref_dates.append(datetime.fromisoformat(cine_iso.replace("Z", "+00:00")))
                except Exception:
                    pass
        if config.movie_digital_sanity_ref in ("import", "min"):
            imp_iso, _ = from_import_history()
            if imp_iso:
                try:
                    ref_dates.append(datetime.fromisoformat(imp_iso.replace("Z", "+00:00")))
                except Exception:
                    pass
        if not ref_dates:
            return digi_iso, "radarr:digitalRelease"
        try:
            d_digi = datetime.fromisoformat(digi_iso.replace("Z", "+00:00"))
            ref_dt = min(ref_dates)
            lag = (d_digi - ref_dt).days
            if lag > config.movie_digital_max_lag_days:
                if config.movie_digital_invalid_fallback == "import":
                    imp_iso2, imp_src2 = from_import_history()
                    if imp_iso2:
                        return imp_iso2, "radarr:history.import"
                cine_iso2, _ = from_radarr_field("cinema")
                if cine_iso2:
                    return cine_iso2, "radarr:inCinemas"
        except Exception:
            pass
        return digi_iso, "radarr:digitalRelease"

    # Priority flows
    if config.movie_priority == "import_then_digital":
        imp_iso, imp_src = from_import_history()
        if imp_iso:
            return imp_iso, imp_src, released_iso
        digi_iso, digi_src = from_external_digital_bundle()
        digi_iso, digi_src = maybe_sanitize_digital(digi_iso)
        if digi_iso:
            return digi_iso, digi_src, released_iso
        cine_iso, cine_src = from_radarr_field("cinema")
        if cine_iso:
            return cine_iso, cine_src, released_iso or cine_iso
        mtime_iso, mtime_src = from_file_mtime()
        return mtime_iso, mtime_src, released_iso

    # digital_then_import
    digi_iso, digi_src = from_external_digital_bundle()
    digi_iso, digi_src = maybe_sanitize_digital(digi_iso)
    if digi_iso:
        return digi_iso, digi_src, released_iso
    imp_iso, imp_src = from_import_history()
    if imp_iso:
        return imp_iso, imp_src, released_iso
    cine_iso, cine_src = from_radarr_field("cinema")
    if cine_iso:
        return cine_iso, cine_src, released_iso or cine_iso
    mtime_iso, mtime_src = from_file_mtime()
    return mtime_iso, mtime_src, released_iso

# ---------------------------
# Movie apply
# ---------------------------

def parse_imdb_from_movie_path(p: Path) -> Optional[str]:
    m = re.search(r'\[imdb-(tt\d+)\]', p.name, re.IGNORECASE)
    return m.group(1).lower() if m else None

def is_movie_dir(p: Path) -> bool:
    return p.is_dir() and parse_imdb_from_movie_path(p) is not None

def set_movie_file_mtime(movie_dir: Path, dateadded_iso: str):
    if not dateadded_iso:
        return
    try:
        ts = datetime.fromisoformat(dateadded_iso.replace("Z", "+00:00")).timestamp()
        video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")
        for f in movie_dir.iterdir():
            if f.is_file() and f.suffix.lower() in video_exts:
                try:
                    os.utime(f, (ts, ts), follow_symlinks=False)
                    _log("DEBUG", f"Updated movie file mtime: {f} -> {dateadded_iso}")
                except Exception as e:
                    _log("WARNING", f"Failed to update mtime on {f}: {e}")
        try:
            os.utime(movie_dir, (ts, ts), follow_symlinks=False)
            _log("DEBUG", f"Updated movie dir mtime: {movie_dir} -> {dateadded_iso}")
        except Exception as e:
            _log("WARNING", f"Failed to update mtime on {movie_dir}: {e}")
    except Exception as e:
        _log("WARNING", f"Bad timestamp for movie mtimes: {dateadded_iso}: {e}")

def apply_movie(movie_dir: Path, conn: sqlite3.Connection):
    imdb_id = parse_imdb_from_movie_path(movie_dir)
    if not imdb_id:
        _log("ERROR", f"Movie path does not contain an IMDb id [imdb-tt...]: {movie_dir}")
        return

    ensure_movie_tables(conn)

    conn.execute(
        "INSERT INTO movies(imdb_id, movie_path, last_applied) VALUES(?,?,?) "
        "ON CONFLICT(imdb_id) DO UPDATE SET movie_path=excluded.movie_path, last_applied=excluded.last_applied",
        (imdb_id, str(movie_dir), _iso_now()),
    )

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

    row = conn.execute(
        "SELECT released, dateadded, source FROM movie_dates WHERE imdb_id=?", (imdb_id,)
    ).fetchone()
    existing_released = row[0] if row else None
    existing_dateadded = row[1] if row else None
    existing_source = row[2] if row else None

    # poll policy
    if config.movie_poll_mode == "never":
        poll_external = False
    elif config.movie_poll_mode == "if_missing":
        poll_external = not existing_dateadded or (existing_source or "") == "file:mtime"
    else:
        poll_external = True

    preserve = False
    if config.movie_date_update_mode == "backfill_only":
        if existing_dateadded:
            if (config.movie_preserve_radarr_import and (existing_source or "").startswith("radarr:moviefile")) \
               or (config.movie_preserve_existing and (existing_source or "") != "file:mtime"):
                preserve = True

    if preserve:
        dateadded_iso = existing_dateadded
        src_detail = existing_source or "unknown"
        if not existing_released and poll_external and (config.radarr_url and config.radarr_api_key):
            movie_obj = _radarr_movie_by_imdb(imdb_id)
            if movie_obj:
                existing_released = _parse_date_to_iso(movie_obj.get("inCinemas") or movie_obj.get("theatricalRelease"))
        chosen_released = existing_released
        _log("INFO", f"Preserving existing movie date for {movie_dir.name} ({dateadded_iso}; {src_detail})")
    else:
        dateadded_iso, src_detail, decided_released = _decide_movie_date(imdb_id, movie_dir, poll_external=poll_external)
        chosen_released = existing_released or decided_released
        conn.execute(
            "INSERT INTO movie_dates(imdb_id, released, dateadded, source, has_video_file) VALUES(?,?,?,?,?) "
            "ON CONFLICT(imdb_id) DO UPDATE SET released=excluded.released, dateadded=excluded.dateadded, "
            "source=excluded.source, has_video_file=excluded.has_video_file",
            (imdb_id, chosen_released, dateadded_iso, src_detail, 1),
        )

    if config.manage_nfo:
        upsert_movie_nfo(
            movie_dir,
            imdb_id,
            chosen_released,
            dateadded_iso,
            source_detail=src_detail or "unknown",
            lock_metadata=config.lock_metadata,
            media_label="Movie",
            brand=config.manager_brand,
        )

    if config.fix_dir_mtimes and dateadded_iso:
        set_movie_file_mtime(movie_dir, dateadded_iso)

    conn.commit()
    _log("INFO", f"Processed movie: {movie_dir.name} (source={src_detail or 'unknown'}; preserved={preserve})")

# ---------------------------
# Batching
# ---------------------------

class WebhookBatcher:
    def __init__(self):
        self.pending_items: Dict[str, Dict] = {}
        self.timers: Dict[str, threading.Timer] = {}
        self.processing: Set[str] = set()
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=config.max_concurrent_series, thread_name_prefix="media-processor")

    def add_webhook(self, key: str, webhook_data: Dict, media_type: str):
        with self.lock:
            if key in self.timers:
                self.timers[key].cancel()
                _log("DEBUG", f"Cancelled existing timer for {key}")

            webhook_data['media_type'] = media_type
            self.pending_items[key] = webhook_data
            _log("INFO", f"Batched {media_type} webhook for {key} (delay: {config.batch_delay}s)")

            timer = threading.Timer(config.batch_delay, self._process_item, args=[key])
            self.timers[key] = timer
            timer.start()

    def _process_item(self, key: str):
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
        with self.lock:
            return {
                "pending_items": list(self.pending_items.keys()),
                "processing_items": list(self.processing),
                "pending_count": len(self.pending_items),
                "processing_count": len(self.processing),
                "executor_active_threads": getattr(self.executor, '_threads', 0)
            }

batcher = WebhookBatcher()

# ---------------------------
# Path helpers
# ---------------------------

def find_series_path_from_sonarr_path(sonarr_path: str) -> Optional[Path]:
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

# ---------------------------
# Webhook processors
# ---------------------------

async def process_sonarr_import(webhook_data: SonarrWebhook):
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
    try:
        if not webhook_data.movie:
            _log("WARNING", f"No movie data in {webhook_data.eventType} webhook")
            return

        movie_info = webhook_data.movie
        movie_title = movie_info.get("title", "Unknown")
        radarr_path = movie_info.get("folderPath") or movie_info.get("path", "")

        imdb_id = (movie_info.get("imdbId") or (webhook_data.remoteMovie or {}).get("imdbId") or "").strip()
        if imdb_id:
            imdb_id = f"tt{imdb_id.replace('tt','')}"

        movie_path = find_movie_path(movie_title, imdb_id, radarr_path)
        if not imdb_id and movie_path:
            parsed = parse_imdb_from_movie_path(movie_path)
            if parsed:
                imdb_id = parsed

        if not imdb_id:
            _log("WARNING", f"No IMDb ID available for movie '{movie_title}'. Proceeding by path only.")

        _log("INFO", f"Received webhook for movie: {movie_title} (IMDb: {imdb_id or 'unknown'})")
        if webhook_data.renamedMovieFiles:
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

# ---------------------------
# Payload reader (json or form)
# ---------------------------

async def _read_payload(request: Request) -> dict:
    ctype = (request.headers.get("content-type") or "").lower()
    try:
        if "application/json" in ctype:
            return await request.json()
        form = await request.form()
        if "payload" in form:
            return json.loads(form["payload"])
        return dict(form)
    except Exception as e:
        _log("ERROR", f"Failed to read webhook payload: {e}")
        return {}

# ---------------------------
# Routes
# ---------------------------

@app.post("/webhook/sonarr")
async def sonarr_webhook(request: Request, background_tasks: BackgroundTasks):
    _log("INFO", "Hit /webhook/sonarr")
    try:
        payload = await _read_payload(request)
        if not payload:
            raise HTTPException(status_code=422, detail="Empty or unreadable Sonarr payload")

        _log("DEBUG", f"Raw Sonarr webhook payload: {payload}")
        webhook = SonarrWebhook(**payload)
        _log("INFO", f"Received Sonarr webhook: {webhook.eventType}")

        if webhook.eventType not in ["Download", "Upgrade", "Rename"]:
            return {"status": "ignored", "reason": f"Event type {webhook.eventType} not processed"}

        await process_sonarr_import(webhook)
        return {"status": "accepted", "message": "Sonarr webhook queued for batched processing"}

    except HTTPException:
        raise
    except Exception as e:
        _log("ERROR", f"Failed to parse Sonarr webhook: {e}")
        raise HTTPException(status_code=422, detail=f"Invalid Sonarr webhook format: {e}")

@app.post("/webhook/radarr")
async def radarr_webhook(request: Request, background_tasks: BackgroundTasks):
    _log("INFO", "Hit /webhook/radarr")
    try:
        payload = await _read_payload(request)
        if not payload:
            raise HTTPException(status_code=422, detail="Empty or unreadable Radarr payload")

        _log("DEBUG", f"Raw Radarr webhook payload: {payload}")
        webhook = RadarrWebhook(**payload)
        _log("INFO", f"Received Radarr webhook: {webhook.eventType}")

        if webhook.eventType not in ["Download", "Upgrade", "Rename", "MovieFileDelete", "MovieDelete", "Test"]:
            return {"status": "ignored", "reason": f"Event type {webhook.eventType} not processed"}

        if webhook.eventType != "Test":
            await process_radarr_import(webhook)
            return {"status": "accepted", "message": "Radarr webhook queued for batched processing"}
        else:
            return {"status": "ok", "message": "Radarr test received"}

    except HTTPException:
        raise
    except Exception as e:
        _log("ERROR", f"Failed to parse Radarr webhook: {e}")
        raise HTTPException(status_code=422, detail=f"Invalid Radarr webhook format: {e}")

@app.get("/health")
async def health_check() -> HealthResponse:
    uptime = datetime.now(timezone.utc) - start_time
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
        version="1.7.0",
        uptime=str(uptime),
        database_status=db_status
    )

@app.get("/stats")
async def get_stats():
    try:
        conn = db_connect(db_path)
        conn.execute("PRAGMA busy_timeout = 5000")
        ensure_movie_tables(conn)

        tv_series_count = conn.execute("SELECT COUNT(*) FROM series").fetchone()[0]
        tv_episode_count = conn.execute("SELECT COUNT(*) FROM episode_dates").fetchone()[0]
        tv_with_files = conn.execute("SELECT COUNT(*) FROM episode_dates WHERE has_video_file = 1").fetchone()[0]

        movie_count = conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
        movies_with_files = conn.execute("SELECT COUNT(*) FROM movie_dates WHERE has_video_file = 1").fetchone()[0]

        tv_sources = conn.execute("""
            SELECT source, COUNT(*) as count
            FROM episode_dates
            WHERE has_video_file = 1
            GROUP BY source
            ORDER BY count DESC
        """).fetchall()

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
    return batcher.get_status()

# ---------------------------
# Manual scans
# ---------------------------

def run_manual_scan_sync(scan_paths: List[Path], scan_type: str = "both"):
    from media_date_cache import iterate_target_path

    class MockArgs:
        def __init__(self):
            self.manage_nfo = config.manage_nfo
            self.fix_dir_mtimes = config.fix_dir_mtimes
            self.lock_metadata = config.lock_metadata
            self.use_cache = True
            self.force_refresh = False
            self.skip_if_current = True
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
                # Only run TV scan on paths that are in tv_paths
                if scan_path in config.tv_paths:
                    tv_targets = iterate_target_path(scan_path)
                    for series_dir in tv_targets:
                        try:
                            apply_series(series_dir, MockArgs(), conn)
                        except Exception as e:
                            _log("ERROR", f"Failed processing TV series {series_dir}: {e}")

            if scan_type in ["both", "movies"]:
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
    if scan_type not in ["both", "tv", "movies"]:
        raise HTTPException(status_code=400, detail="scan_type must be 'both', 'tv', or 'movies'")

    if path:
        scan_paths = [Path(path)]
    else:
        if scan_type == "movies":
            scan_paths = list(config.movie_paths)
        elif scan_type == "tv":
            scan_paths = list(config.tv_paths)
        else:
            scan_paths = list(config.tv_paths) + list(config.movie_paths)

    async def run_scan():
        await asyncio.get_event_loop().run_in_executor(None, run_manual_scan_sync, scan_paths, scan_type)

    background_tasks.add_task(run_scan)
    return {"status": "started", "message": f"Manual {scan_type} scan started for {len(scan_paths)} paths"}

@app.post("/manual/scan/tv")
async def manual_scan_tv(background_tasks: BackgroundTasks, path: Optional[str] = None):
    return await manual_scan(background_tasks, path, "tv")

@app.post("/manual/scan/movies")
async def manual_scan_movies(background_tasks: BackgroundTasks, path: Optional[str] = None):
    return await manual_scan(background_tasks, path, "movies")

# ---------------------------
# Main
# ---------------------------

if __name__ == "__main__":
    if config.debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)
        os.environ["DEBUG"] = "true"

    _log("INFO", "Starting NFOGuard Webhook Server")
    _log("INFO", f"TV paths: {[str(p) for p in config.tv_paths]}")
    _log("INFO", f"Movie paths: {[str(p) for p in config.movie_paths]}")
    _log("INFO", f"Database path: {db_path}")
    _log("INFO", f"Manage NFO: {config.manage_nfo}")
    _log("INFO", f"Fix dir mtimes: {config.fix_dir_mtimes}")
    _log("INFO", f"Lock metadata: {config.lock_metadata}")
    _log("INFO", f"Batch delay: {config.batch_delay}s")
    _log("INFO", f"Max concurrent items: {config.max_concurrent_series}")
    _log("INFO", f"MOVIE_PRIORITY={config.movie_priority}")
    _log("INFO", f"MOVIE_POLL_MODE={config.movie_poll_mode}")
    _log("INFO", f"MOVIE_DATE_STRATEGY={config.movie_date_strategy}")
    _log("INFO", f"MOVIE_DATE_UPDATE_MODE={config.movie_date_update_mode} "
                 f"(preserve_import={config.movie_preserve_radarr_import}, "
                 f"preserve_existing={config.movie_preserve_existing})")
    _log("INFO", f"MOVIE_DIGITAL_SANITY_CHECK={config.movie_digital_sanity_check} "
                 f"(ref={config.movie_digital_sanity_ref}, max_lag_days={config.movie_digital_max_lag_days}, "
                 f"invalid_fallback={config.movie_digital_invalid_fallback})")
    if config.radarr_url and config.radarr_api_key:
        _log("INFO", f"Radarr enabled @ {config.radarr_url} (timeout={config.radarr_timeout}s, retries={config.radarr_retries})")
    if config.tmdb_api_key:
        _log("INFO", f"TMDB enabled (country={config.tmdb_country})")
    if config.omdb_api_key:
        _log("INFO", "OMDb enabled")
    if config.jelly_url and config.jelly_api:
        _log("INFO", f"Jellyseerr enabled @ {config.jelly_url}")

uvicorn.run(
    app,  # pass the object, not "app:app"
    host="0.0.0.0",
    port=int(os.environ.get("PORT", "8080")),
    reload=False
)

