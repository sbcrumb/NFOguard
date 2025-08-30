# NFOGuard


*** Alpha in progress things are volatile use at your own risk ***

**NFOGuard** is a lightweight webhook service that locks in the *true import date* of your movies and TV episodes, ensuring that upgrades, renames, or reimports never bubble old media up as “new” in Emby, Jellyfin, or Plex.  

It integrates with **Sonarr** and **Radarr**, listens for import/rename/upgrade events, and automatically manages `.nfo` files and filesystem timestamps to keep your library chronology consistent.

---

## ✨ Features

- **Import Date Locking**  
  Captures the original date a movie or episode was imported and preserves it across upgrades.

- **NFO Management**  
  Creates and updates `.nfo` files for movies and TV episodes with consistent `<dateadded>` fields.  
  Optionally adds `<lockdata>` to prevent metadata drift from scrapers.

- **Filesystem Time Fixing**  
  Updates file and directory modification times (`mtime`) to match the preserved import date.

- **Batching Support**  
  Groups multiple webhook events (e.g. whole-season downloads) to avoid thrashing.

- **Database Backing**  
  Uses SQLite to remember previously-seen imports, so dates persist across restarts.

- **Manual Scans**  
  Exposes endpoints to rescan and reconcile TV or movie libraries on demand.

---

## 🏗 How It Works

1. **Webhooks**  
   - Add NFOGuard as a webhook in Sonarr and Radarr.  
   - Supported event types: `Download`, `Upgrade`, `Rename`.

2. **Processing**  
   - Maps Sonarr/Radarr paths to container paths.  
   - Looks up IMDb IDs from folder names or payloads.  
   - Writes `.nfo` files and updates file/dir mtimes.  
   - Records everything in `media_dates.db`.

3. **Playback Apps**  
   - Emby/Jellyfin/Plex see consistent `dateadded` values.  
   - Upgrades don’t bubble to the top of “Recently Added.”

---

## 🚀 Quick Start (Docker)

```bash
docker run -d \
  --name=NFOGuard \
  -p 8080:8080 \
  -v /mnt/unionfs/Media:/media:rw \
  -v /opt/NFOGuard/data:/app/data \
  -e TV_PATHS="/media/TV/tv,/media/TV/tv6" \
  -e MOVIE_PATHS="/media/Movies/movies,/media/Movies/movies6" \
  ghcr.io/your-org/NFOGuard:latest
⚙️ Environment Variables
Variable	Default	Description
TV_PATHS	/media/tv,/media/tv6	Comma-separated list of TV root paths
MOVIE_PATHS	/media/movies,/media/movies6	Comma-separated list of Movie root paths
DB_PATH	/app/data/media_dates.db	Path to SQLite database
MANAGE_NFO	true	Write/update NFO files
FIX_DIR_MTIMES	true	Sync directory/file mtimes to import date
LOCK_METADATA	true	Add <lockdata> in NFOs
BATCH_DELAY	5.0	Delay before processing batched events
DEBUG	false	Enable verbose logging

🔌 API Endpoints
POST /webhook/sonarr – Sonarr events

POST /webhook/radarr – Radarr events

POST /manual/scan – Manual scan (TV + Movies)

POST /manual/scan/tv – Manual scan TV only

POST /manual/scan/movies – Manual scan Movies only

GET /health – Health check

GET /stats – Database stats

GET /batch/status – Current batch queue

📖 Example Workflow
You add The Blacklist in Sonarr.

Sonarr downloads S01E01 → NFOGuard logs the import date in DB and .nfo.

Months later, a better-quality upgrade replaces the file.

NFOGuard updates the .nfo and mtime, but keeps the original import date.

Emby/Jellyfin/Plex see the file as unchanged in chronology.

Result: no more “old shows” showing up in “Recently Added.”

📜 License
MIT License – do what you want, just don’t blame us if your timestamps go timey-wimey.