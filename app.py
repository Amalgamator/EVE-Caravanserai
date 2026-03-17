"""
EVE Caravanserai — multi-hub market comparison tool.

Compare any two markets: NPC trade hubs, freeport player structures,
or any accessible player structure — giving traders visibility on
import/export margins and price gaps across New Eden.

Usage:  python app.py
Opens:  http://127.0.0.1:8182
"""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
import json
import zipfile
import logging
import os
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Optional

import requests
from flask import Flask, Response, jsonify, redirect, render_template, request

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("caravanserai")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# App & constants
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

DB_UNIVERSE   = "universeData.db"    # SDE map + type data — rebuild on SDE update
DB_MARKET     = "caravanserai.db"   # snapshots, structure names, freeports
DB_USER       = "userConfig.db"     # characters, client ID, kv config
ESI_BASE          = "https://esi.evetech.net/latest"
SDE_BASE          = "https://www.fuzzwork.co.uk/dump/latest"
FUZZWORKS_CSV_URL = "https://market.fuzzwork.co.uk/aggregatecsv.csv.gz"

APPAREL_CATEGORY_ID = 30  # invCategories: Apparel

# Upwell structures that can fit a Market Services Module, with display names
# Type IDs verified against EVE Ref (everef.net)
MARKET_STRUCTURE_TYPES = {
    35832: "Astrahus",
    35833: "Fortizar",
    35834: "Keepstar",
    35826: "Azbel",
    35827: "Sotiyo",
    35836: "Tatara",
}

# ---------------------------------------------------------------------------
# NPC trade hubs
# ---------------------------------------------------------------------------
# Each hub is defined by its canonical station and the region whose Fuzzworks
# CSV data covers it. Hek and Odebeinn share Metropolis (10000042).

NPC_HUBS = {
    60003760: {
        "name":      "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
        "short":     "Jita",
        "system":    "Jita",
        "region_id": 10000002,  # The Forge
    },
    60008494: {
        "name":      "Amarr VIII (Oris) - Emperor Family Academy",
        "short":     "Amarr",
        "system":    "Amarr",
        "region_id": 10000043,  # Domain
    },
    60011866: {
        "name":      "Dodixie IX - Moon 20 - Federation Navy Assembly Plant",
        "short":     "Dodixie",
        "system":    "Dodixie",
        "region_id": 10000032,  # Sinq Laison
    },
    60005686: {
        "name":      "Hek VIII - Moon 12 - Boundless Creation Factory",
        "short":     "Hek",
        "system":    "Hek",
        "region_id": 10000042,  # Metropolis
    },
    60004588: {
        "name":      "Rens VI - Moon 8 - Brutor Tribe Treasury",
        "short":     "Rens",
        "system":    "Rens",
        "region_id": 10000030,  # Heimatar
    },
    60012631: {
        "name":      "Odebeinn V - Moon 5 - Inherent Implants Biotech Production",
        "short":     "Odebeinn",
        "system":    "Odebeinn",
        "region_id": 10000042,  # Metropolis
    },
}

A4E_MARKET_HUBS_URL = "https://www.adam4eve.eu/market_hubs.php"
# NPC station IDs to exclude from freeport scraping
NPC_STATION_IDS = set(NPC_HUBS.keys())

# ---------------------------------------------------------------------------
# EVE SSO  (PKCE — public client, no secret required)
# ---------------------------------------------------------------------------
# Register your app at: https://developers.eveonline.com/
# Application type:     Authentication Only (PKCE)
# Callback URL:         http://127.0.0.1:8182/callback
#
# Required scopes:
#   esi-universe.read_structures.v1     resolve structure names/details
#   esi-search.search_structures.v1     search structures by system name
#   esi-markets.structure_markets.v1    fetch structure market orders
#   esi-ui.open_window.v1               open in-game windows (market details, info)
#   esi-ui.write_waypoint.v1            set autopilot destination/waypoints
#   publicData                          public character info
#   esi-corporations.read_structures.v1 corp-visible structures
#   esi-structures.read_character.v1    character-visible structures

SSO_AUTH_URL  = "https://login.eveonline.com/v2/oauth/authorize"
SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"
SSO_VERIFY    = "https://login.eveonline.com/oauth/verify"
SSO_REDIRECT  = "http://127.0.0.1:8182/callback"
SSO_SCOPES    = " ".join([
    "publicData",
    "esi-universe.read_structures.v1",
    "esi-search.search_structures.v1",
    "esi-markets.structure_markets.v1",
    "esi-ui.open_window.v1",
    "esi-ui.write_waypoint.v1",               # set autopilot destination
    "esi-markets.read_character_orders.v1",   # character buy/sell order quantities
    "esi-markets.read_corporation_orders.v1", # corporation buy/sell order quantities
])

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def get_db()      -> sqlite3.Connection: return _open_db(DB_MARKET)
def get_universe() -> sqlite3.Connection: return _open_db(DB_UNIVERSE)
def get_user()     -> sqlite3.Connection: return _open_db(DB_USER)


# ---------------------------------------------------------------------------
# Database migrations
# ---------------------------------------------------------------------------
# Each entry is (version: int, db: str, sql: str).
# Migrations are applied in version order on startup — never modified, only
# appended. Users keep their data; only schema changes are applied.
# db is one of: 'market', 'universe', 'user'
#
# How to add a migration:
#   1. Append a new tuple with the next version number.
#   2. Write forward-only SQL (ADD COLUMN, CREATE TABLE, CREATE INDEX, etc.).
#   3. Bump CURRENT_VERSION in the auto-updater section when you tag a release.
#
# SQLite limitations: ALTER TABLE only supports ADD COLUMN.
# To rename/drop columns, use the CREATE+INSERT+DROP pattern.
# ---------------------------------------------------------------------------

MIGRATIONS: list[tuple[int, str, str]] = [
    # ── v1: initial schema ────────────────────────────────────────────────
    (1, "market", """
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date     TEXT    NOT NULL,
            location_id       INTEGER NOT NULL,
            type_id           INTEGER NOT NULL,
            sell_units        REAL,
            buy_units         REAL,
            lowest_sell_10pct REAL,
            highest_buy_10pct REAL,
            split_price       REAL,
            UNIQUE(snapshot_date, location_id, type_id)
        );
        CREATE INDEX IF NOT EXISTS idx_ms_loc_date
            ON market_snapshots(location_id, snapshot_date);
        CREATE TABLE IF NOT EXISTS structure_names (
            structure_id    INTEGER PRIMARY KEY,
            name            TEXT,
            solar_system_id INTEGER,
            type_id         INTEGER,
            is_freeport     INTEGER DEFAULT 0,
            fetched_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS freeports (
            structure_id  INTEGER PRIMARY KEY,
            name          TEXT,
            system        TEXT,
            region        TEXT,
            sell_orders   INTEGER DEFAULT 0,
            last_seen     TEXT
        );
    """),
    (1, "universe", """
        CREATE TABLE IF NOT EXISTS type_names (
            type_id INTEGER PRIMARY KEY,
            name    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS type_groups (
            type_id       INTEGER PRIMARY KEY,
            group_id      INTEGER NOT NULL,
            group_name    TEXT,
            category_id   INTEGER NOT NULL,
            category_name TEXT,
            meta_level    INTEGER
        );
        CREATE TABLE IF NOT EXISTS map_regions (
            region_id INTEGER PRIMARY KEY,
            name      TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS map_constellations (
            constellation_id INTEGER PRIMARY KEY,
            name             TEXT NOT NULL,
            region_id        INTEGER
        );
        CREATE TABLE IF NOT EXISTS map_systems (
            system_id        INTEGER PRIMARY KEY,
            name             TEXT NOT NULL,
            constellation_id INTEGER,
            region_id        INTEGER,
            security         REAL
        );
        CREATE INDEX IF NOT EXISTS idx_map_systems_name ON map_systems(name);
        CREATE TABLE IF NOT EXISTS sde_meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """),
    (1, "user", """
        CREATE TABLE IF NOT EXISTS characters (
            character_id   INTEGER PRIMARY KEY,
            character_name TEXT    NOT NULL,
            access_token   TEXT,
            refresh_token  TEXT,
            expires_at     REAL,
            is_primary     INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS kv (
            namespace TEXT NOT NULL,
            key       TEXT NOT NULL,
            value     TEXT,
            PRIMARY KEY (namespace, key)
        );
    """),
    # ── Add new migrations below this line ────────────────────────────────
    # Example:
    # (2, "market", "ALTER TABLE freeports ADD COLUMN corporation_id INTEGER;"),
]


def _get_conn(db: str):
    return {"market": get_db, "universe": get_universe, "user": get_user}[db]()


def init_db() -> None:
    """Apply all pending migrations to all three databases."""
    _ensure_schema_version_tables()
    _apply_migrations()


def _ensure_schema_version_tables() -> None:
    """Create the schema_version table in each DB if it doesn't exist."""
    for getter in (get_db, get_universe, get_user):
        conn = getter()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        """)
        conn.commit()
        conn.close()


def _apply_migrations() -> None:
    """Apply any migrations not yet recorded in schema_version."""
    # Group by db so we open each connection only once per run
    from itertools import groupby
    by_db = {}
    for version, db, sql in MIGRATIONS:
        by_db.setdefault(db, []).append((version, sql))

    for db, steps in by_db.items():
        conn = _get_conn(db)
        applied = {
            r[0] for r in
            conn.execute("SELECT version FROM schema_version").fetchall()
        }
        pending = [(v, s) for v, s in steps if v not in applied]
        if not pending:
            conn.close()
            continue
        for version, sql in sorted(pending):
            try:
                conn.executescript(sql)
                conn.execute("INSERT OR IGNORE INTO schema_version VALUES (?)", (version,))
                conn.commit()
                log.info("[DB] Applied migration v%d to %s db.", version, db)
            except Exception as exc:
                log.error("[DB] Migration v%d failed on %s db: %s", version, db, exc)
                raise
        conn.close()

# ---------------------------------------------------------------------------
# Key-value store
# ---------------------------------------------------------------------------

def kv_get(namespace: str, key: str):
    conn = get_user()
    row  = conn.execute(
        "SELECT value FROM kv WHERE namespace=? AND key=?", (namespace, key)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    try:
        return json.loads(row["value"])
    except Exception:
        return row["value"]


def kv_set(namespace: str, key: str, value) -> None:
    conn = get_user()
    conn.execute(
        "INSERT OR REPLACE INTO kv (namespace, key, value) VALUES (?,?,?)",
        (namespace, key, json.dumps(value))
    )
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# ESI helpers
# ---------------------------------------------------------------------------

def esi_get(path: str, params: Optional[dict] = None, retries: int = 3):
    url = f"{ESI_BASE}{path}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json(), int(r.headers.get("X-Pages", 1))
            if r.status_code == 204:
                return [], 1
            log.warning("ESI %s -> HTTP %s (attempt %s)", path, r.status_code, attempt + 1)
        except Exception as exc:
            log.warning("ESI %s -> %s (attempt %s/%s)", path, exc, attempt + 1, retries)
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    return None, 1


def esi_get_authed(path: str, token: str, params: Optional[dict] = None):
    url     = f"{ESI_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code == 200:
            return r.json(), int(r.headers.get("X-Pages", 1))
        log.warning("ESI AUTH %s -> HTTP %s", path, r.status_code)
    except Exception as exc:
        log.warning("ESI AUTH %s -> %s", path, exc)
    return None, 1


def esi_get_authed_all_pages(path: str, token: str, params: Optional[dict] = None) -> list:
    params       = params or {}
    first, pages = esi_get_authed(path, token, {**params, "page": 1})
    results      = list(first) if first else []
    for page in range(2, pages + 1):
        data, _ = esi_get_authed(path, token, {**params, "page": page})
        if data:
            results.extend(data)
        time.sleep(0.05)
    return results

# ---------------------------------------------------------------------------
# Type name resolution
# ---------------------------------------------------------------------------

def resolve_type_names(type_ids: list) -> None:
    """Ensure all type_ids have names in the universe cache.
    The SDE build populates type_names for all known types; this is a
    safety net for newly released items not yet in the local SDE build.
    """
    if not type_ids:
        return
    conn = get_universe()
    placeholders = ",".join("?" * len(type_ids))
    cached = {
        r["type_id"]
        for r in conn.execute(
            f"SELECT type_id FROM type_names WHERE type_id IN ({placeholders})",
            tuple(type_ids)
        ).fetchall()
    }
    missing = [tid for tid in type_ids if tid not in cached]
    if missing:
        log.info("Type names: %d to fetch from ESI (%d already cached).",
                 len(missing), len(cached))
    for i in range(0, len(missing), 1000):
        batch = missing[i:i + 1000]
        try:
            r = requests.post(f"{ESI_BASE}/universe/names/", json=batch, timeout=20)
            if r.status_code == 200:
                conn.executemany(
                    "INSERT OR IGNORE INTO type_names VALUES (?,?)",
                    [(item["id"], item["name"]) for item in r.json()]
                )
            else:
                log.warning("/universe/names/ returned HTTP %s", r.status_code)
        except Exception as exc:
            log.warning("/universe/names/ failed: %s", exc)
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Apparel / SKIN filters
# ---------------------------------------------------------------------------

# Exclude items whose group name is in this set (applied when "Exclude apparel & SKINs" is on)
_EXCLUDED_GROUPS = {"30-Day SKIN", "Permanent SKIN"}


_apparel_ids: set         = set()
_apparel_ids_loaded: bool = False


def get_apparel_ids() -> set:
    global _apparel_ids, _apparel_ids_loaded
    if _apparel_ids_loaded:
        return _apparel_ids
    conn = get_universe()
    rows = conn.execute(
        "SELECT type_id FROM type_groups WHERE category_id=?",
        (APPAREL_CATEGORY_ID,)
    ).fetchall()
    conn.close()
    _apparel_ids        = {r["type_id"] for r in rows}
    _apparel_ids_loaded = True
    log.info("[Filter] %d apparel type IDs loaded.", len(_apparel_ids))
    return _apparel_ids


def filter_unwanted(type_ids: set, lookup: dict) -> int:
    """Remove Apparel and SKIN-group items in-place. Returns removed count."""
    if not type_ids:
        return 0
    apparel   = get_apparel_ids()
    to_remove = set(type_ids & apparel)
    remaining = type_ids - to_remove
    if remaining:
        conn = get_universe()
        placeholders = ",".join("?" * len(remaining))
        rows = conn.execute(
            f"SELECT type_id, group_name FROM type_groups WHERE type_id IN ({placeholders})",
            tuple(remaining)
        ).fetchall()
        conn.close()
        for row in rows:
            if row["group_name"] in _EXCLUDED_GROUPS:
                to_remove.add(row["type_id"])
    type_ids -= to_remove
    for tid in to_remove:
        lookup.pop(tid, None)
    return len(to_remove)

# ---------------------------------------------------------------------------
# Market order aggregation
# ---------------------------------------------------------------------------

def aggregate_orders(orders: list) -> dict:
    sells: dict = defaultdict(list)
    buys:  dict = defaultdict(list)
    for o in orders:
        (buys if o["is_buy_order"] else sells)[o["type_id"]].append(
            (o["price"], o["volume_remain"])
        )
    result = {}
    for tid in set(sells) | set(buys):
        sv   = sorted(sells.get(tid, []), key=lambda x: x[0])
        bv   = sorted(buys.get(tid,  []), key=lambda x: -x[0])
        ls10 = _weighted_price_of_pct(sv, 0.10)
        hb10 = _weighted_price_of_pct(bv, 0.10)
        result[tid] = {
            "sell_units":        sum(v for _, v in sv),
            "buy_units":         sum(v for _, v in bv),
            "lowest_sell_10pct": ls10,
            "highest_buy_10pct": hb10,
            "split_price":       (ls10 + hb10) / 2 if (ls10 and hb10) else None,
        }
    return result


def _weighted_price_of_pct(orders: list, pct: float) -> Optional[float]:
    if not orders:
        return None
    total        = sum(v for _, v in orders)
    target       = total * pct
    accumulated  = 0.0
    weighted_sum = 0.0
    for price, vol in orders:
        take          = min(vol, target - accumulated)
        weighted_sum += price * take
        accumulated  += take
        if accumulated >= target:
            break
    return weighted_sum / accumulated if accumulated else None

# ---------------------------------------------------------------------------
# NPC hub snapshots  (Fuzzworks aggregatecsv.csv.gz)
# ---------------------------------------------------------------------------
# The CSV encodes rows as:  region_id|type_id|is_buy_order
# We download it once and parse snapshots for all requested region IDs.
# Fuzzworks' `fivepercent` = vol-weighted avg of the cheapest/priciest 5%.

_fuzzworks_lock  = threading.Lock()
_fuzzworks_cache: Optional[bytes] = None   # raw gzip bytes, refreshed daily
_fuzzworks_date:  Optional[str]   = None   # date string of cached download


def _get_fuzzworks_raw() -> bytes:
    """Return today's Fuzzworks CSV, downloading only once per day."""
    global _fuzzworks_cache, _fuzzworks_date
    today = date.today().isoformat()
    with _fuzzworks_lock:
        if _fuzzworks_date == today and _fuzzworks_cache:
            return _fuzzworks_cache
        log.info("[Fuzzworks] Downloading market CSV...")
        t0  = time.time()
        r   = requests.get(FUZZWORKS_CSV_URL, timeout=120, stream=True)
        r.raise_for_status()
        _fuzzworks_cache = r.content
        _fuzzworks_date  = today
        log.info("[Fuzzworks] Downloaded %d KB in %.1fs.",
                 len(_fuzzworks_cache) // 1024, time.time() - t0)
        return _fuzzworks_cache


def fetch_npc_hub_snapshot(station_id: int, exclude_skins: bool = True) -> dict:
    """
    Fetch and store a market snapshot for an NPC trade hub.
    Uses Fuzzworks regional data keyed by the hub's region_id.
    """
    hub = NPC_HUBS.get(station_id)
    if not hub:
        return {"status": "error", "message": f"Unknown NPC hub {station_id}"}

    today = date.today().isoformat()
    conn  = get_db()
    cnt   = conn.execute(
        "SELECT COUNT(*) as c FROM market_snapshots "
        "WHERE snapshot_date=? AND location_id=?", (today, station_id)
    ).fetchone()["c"]
    conn.close()
    if cnt:
        return {"status": "already_exists", "date": today}

    raw        = _get_fuzzworks_raw()
    region_str = str(hub["region_id"])
    sells: dict = {}
    buys:  dict = {}

    def safe_float(v):
        try:
            return float(v) if v else None
        except (TypeError, ValueError):
            return None

    with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            parts = row.get("what", "").split("|")
            if len(parts) != 3:
                continue
            reg, tid_str, is_buy = parts
            if reg != region_str:
                continue
            try:
                tid = int(tid_str)
            except ValueError:
                continue
            entry = {
                "volume":      safe_float(row.get("volume")),
                "fivepercent": safe_float(row.get("fivepercent")),
            }
            (buys if is_buy == "true" else sells)[tid] = entry

    all_ids = set(sells) | set(buys)
    log.info("[%s] Parsed %d sell-side and %d buy-side entries.",
             hub["short"], len(sells), len(buys))

    resolve_type_names(list(all_ids))

    if exclude_skins:
        removed = filter_unwanted(all_ids, {**sells, **buys})
        for tid in list(sells):
            if tid not in all_ids:
                del sells[tid]
        for tid in list(buys):
            if tid not in all_ids:
                del buys[tid]
        log.info("[%s] Filtered %d Apparel/SKIN types. Storing %d.",
                 hub["short"], removed, len(all_ids))

    conn = get_db()
    c    = conn.cursor()
    for tid in all_ids:
        sell = sells.get(tid, {})
        buy  = buys.get(tid,  {})
        ls10 = sell.get("fivepercent")
        hb10 = buy.get("fivepercent")
        c.execute(
            """INSERT OR IGNORE INTO market_snapshots
               (snapshot_date, location_id, type_id, sell_units, buy_units,
                lowest_sell_10pct, highest_buy_10pct, split_price)
               VALUES (?,?,?,?,?,?,?,?)""",
            (today, station_id, tid,
             sell.get("volume") or 0, buy.get("volume") or 0,
             ls10, hb10,
             (ls10 + hb10) / 2 if (ls10 and hb10) else None)
        )
    conn.commit()
    conn.close()
    return {"status": "created", "date": today, "types": len(all_ids)}

# ---------------------------------------------------------------------------
# Structure market snapshots  (ESI authenticated)
# ---------------------------------------------------------------------------

def fetch_structure_snapshot(structure_id: int, exclude_skins: bool = True) -> dict:
    today = date.today().isoformat()
    conn  = get_db()
    cnt   = conn.execute(
        "SELECT COUNT(*) as c FROM market_snapshots "
        "WHERE snapshot_date=? AND location_id=?", (today, structure_id)
    ).fetchone()["c"]
    conn.close()
    if cnt:
        return {"status": "already_exists", "date": today}

    token = sso_get_token()
    if not token:
        return {"status": "error", "message": "Not authenticated."}

    log.info("[Structure %s] Fetching market orders...", structure_id)
    t0     = time.time()
    orders = esi_get_authed_all_pages(f"/markets/structures/{structure_id}/", token)
    if not orders:
        return {"status": "empty"}

    log.info("[Structure %s] %d orders in %.1fs, aggregating...",
             structure_id, len(orders), time.time() - t0)
    agg = aggregate_orders(orders)
    resolve_type_names(list(agg.keys()))

    if exclude_skins:
        tid_set = set(agg.keys())
        removed = filter_unwanted(tid_set, agg)
        if removed:
            log.info("[Structure %s] Filtered %d Apparel/SKIN types.", structure_id, removed)

    conn = get_db()
    c    = conn.cursor()
    for tid, d in agg.items():
        c.execute(
            """INSERT OR IGNORE INTO market_snapshots
               (snapshot_date, location_id, type_id, sell_units, buy_units,
                lowest_sell_10pct, highest_buy_10pct, split_price)
               VALUES (?,?,?,?,?,?,?,?)""",
            (today, structure_id, tid,
             d["sell_units"], d["buy_units"],
             d["lowest_sell_10pct"], d["highest_buy_10pct"], d["split_price"])
        )
    conn.commit()
    conn.close()
    return {"status": "created", "date": today, "types": len(agg)}

# ---------------------------------------------------------------------------
# Comparison query
# ---------------------------------------------------------------------------

def get_comparison(src_id: int, dst_id: int, snapshot_date: Optional[str] = None) -> list:
    """
    Compare two market locations by their location_id on a given date.
    src = Source (left panel), dst = Destination (right panel).
    Import % = dst_sell / src_buy  (buy at source, sell at destination)
    Export % = src_sell / dst_buy  (buy at destination, sell at source)
    """
    snapshot_date = snapshot_date or date.today().isoformat()
    conn  = get_db()
    conn.execute(f"ATTACH DATABASE ? AS uni", (DB_UNIVERSE,))
    rows  = conn.execute("""
        SELECT
            COALESCE(s.type_id, d.type_id)  AS type_id,
            tn.name                          AS type_name,
            tg.group_name                    AS group_name,
            tg.category_name                 AS category_name,
            tg.meta_level                    AS meta_level,
            s.sell_units                     AS src_supply,
            s.buy_units                      AS src_demand,
            s.split_price                    AS src_split,
            s.lowest_sell_10pct              AS src_sell,
            s.highest_buy_10pct              AS src_buy,
            d.sell_units                     AS dst_supply,
            d.buy_units                      AS dst_demand,
            d.split_price                    AS dst_split,
            d.lowest_sell_10pct              AS dst_sell,
            d.highest_buy_10pct              AS dst_buy
        FROM market_snapshots s
        FULL OUTER JOIN market_snapshots d
               ON d.type_id      = s.type_id
              AND d.location_id  = :dst_id
              AND d.snapshot_date = :snap_date
        LEFT JOIN uni.type_names tn
               ON tn.type_id = COALESCE(s.type_id, d.type_id)
        LEFT JOIN uni.type_groups tg
               ON tg.type_id = COALESCE(s.type_id, d.type_id)
        WHERE s.location_id   = :src_id
          AND s.snapshot_date = :snap_date
    """, {"src_id": src_id, "dst_id": dst_id, "snap_date": snapshot_date}).fetchall()

    result = []
    for row in rows:
        src_supply = row["src_supply"] or 0
        src_demand = row["src_demand"] or 0
        dst_supply = row["dst_supply"] or 0
        dst_demand = row["dst_demand"] or 0
        src_sell   = row["src_sell"]
        src_buy    = row["src_buy"]
        dst_sell   = row["dst_sell"]
        dst_buy    = row["dst_buy"]
        result.append({
            "type_id":       row["type_id"],
            "type_name":     row["type_name"] or f"Type {row['type_id']}",
            "group_name":    row["group_name"] or "",
            "category_name": row["category_name"] or "",
            "meta_level":    row["meta_level"] if row["meta_level"] is not None else 0,
            "src_supply": src_supply,
            "src_demand": src_demand,
            "src_split":  row["src_split"],
            "src_sell":   src_sell,
            "src_buy":    src_buy,
            "dst_supply": dst_supply,
            "dst_demand": dst_demand,
            "dst_split":  row["dst_split"],
            "dst_sell":   dst_sell,
            "dst_buy":    dst_buy,
            # Bid/ask spread: (sell - buy) / sell * 100
            "src_spread": round((src_sell - src_buy) / src_sell * 100, 2) if (src_sell and src_buy and src_sell > 0) else None,
            "dst_spread": round((dst_sell - dst_buy) / dst_sell * 100, 2) if (dst_sell and dst_buy and dst_sell > 0) else None,
            # Import: buy at source, sell at destination  →  dst_sell / src_buy
            "import_margin": round(dst_sell / src_buy * 100, 2) if (dst_sell and src_buy) else None,
            # Export: buy at destination, sell at source  →  src_sell / dst_buy
            "export_margin": round(src_sell / dst_buy * 100, 2) if (src_sell and dst_buy) else None,
        })
    conn.execute("DETACH DATABASE uni")
    conn.close()
    return result

# ---------------------------------------------------------------------------
# SDE universe cache
# ---------------------------------------------------------------------------

def _scrape_freeports() -> None:
    """
    Scrape adam4eve.eu/market_hubs.php for the top player-owned market structures.
    Only runs if the freeports table is empty or last scrape was >23h ago.
    Stores results in the freeports table.
    """
    import re as _re

    conn = get_db()
    last = kv_get("freeports", "last_scrape")
    count = conn.execute("SELECT COUNT(*) as c FROM freeports").fetchone()["c"]
    conn.close()

    if last and count:
        try:
            age = time.time() - datetime.fromisoformat(last).timestamp()
            if age < 82800:  # 23 hours
                log.debug("[Freeports] Scrape skipped — last run %.0fh ago.", age / 3600)
                return
        except Exception:
            pass

    log.info("[Freeports] Scraping A4E market hubs...")
    try:
        r = requests.get(A4E_MARKET_HUBS_URL, timeout=20,
                         headers={"User-Agent": "EVE-Caravanserai/1.0"})
        r.raise_for_status()
    except Exception as exc:
        log.warning("[Freeports] Scrape failed: %s", exc)
        return

    # Extract rows: structure_history.php?id=XXXXXXX ... Type=Fortizar|Astrahus|etc
    # Each row has: system, sec, name (with link containing id), type
    html = r.text

    # Match all structure IDs from links — player structures have large IDs (>1e12)
    # Pattern: stationID=XXXXXXX in market_orders.php links
    found = []
    # Parse table rows — find structure_history links with large IDs (player structures)
    id_pattern    = _re.compile(r'structure_history\.php\?id=(\d+)')
    name_pattern  = _re.compile(r'structure_history\.php\?id=\d+"[^>]*>([^<]+)<')
    system_pattern = _re.compile(r'location\.php\?id=\d+"[^>]*>([^<]+)</a>\s*</td>\s*<td[^>]*>\s*[\d,]+\s*</td>\s*<td[^>]*>\s*<a[^>]+structure_history')

    # Walk through the HTML finding player structure rows
    # Simpler: find all (id, name) pairs then filter by ID size
    rows = _re.findall(
        r'structure_history\.php\?id=(\d+)"[^>]*title="Show structure history">([^<]+)</a>',
        html
    )

    # Also capture the system from the preceding <td> cells in the row
    # Full row pattern: grab region, system, sec, name+id, type, sell_orders
    row_pattern = _re.compile(
        r'location\.php\?id=\d+"[^>]*>([^<]+)</a>\s*</td>\s*'   # region
        r'<td[^>]*>\s*<a[^>]*>([^<]+)</a>\s*</td>\s*'           # system
        r'<td[^>]*>([\d,]+)\s*</td>\s*'                          # sec
        r'<td[^>]*>\s*<a[^>]*structure_history[^>]*id=(\d+)[^>]*>([^<]+)</a>\s*</td>\s*'  # id+name
        r'<td[^>]*>([^<]+)</td>\s*'                              # type
        r'<td[^>]*>.*?</td>\s*'                                  # buy orders
        r'<td[^>]*>.*?</td>\s*'                                  # buy volume
        r'<td[^>]*>\s*<a[^>]*>([\d.,]+)</a>',                   # sell orders
        _re.DOTALL
    )
    matches = row_pattern.findall(html)
    log.info("[Freeports] A4E regex found %d rows.", len(matches))

    # Fall back to simpler ID+name extraction if regex misses rows
    if len(matches) < 3:
        log.info("[Freeports] Falling back to simple ID extraction.")
        matches_simple = _re.findall(
            r'structure_history\.php\?id=(\d+)"[^>]*>([^<]+)</a>',
            html
        )
        conn = get_db()
        now  = datetime.now(timezone.utc).isoformat()
        inserted = 0
        for sid_str, name in matches_simple:
            sid = int(sid_str)
            if sid in NPC_STATION_IDS or sid < 1_000_000_000_000:
                continue
            conn.execute(
                """INSERT INTO freeports (structure_id, name, system, region, sell_orders, last_seen)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(structure_id) DO UPDATE SET
                       name=excluded.name, last_seen=excluded.last_seen""",
                (sid, name.strip(), "", "", 0, now)
            )
            inserted += 1
        conn.commit(); conn.close()
        kv_set("freeports", "last_scrape", now)
        log.info("[Freeports] Stored %d structures (simple mode).", inserted)
        return

    conn = get_db()
    now  = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for region, system, sec, sid_str, name, struct_type, sell_str in matches:
        sid = int(sid_str)
        if sid in NPC_STATION_IDS or sid < 1_000_000_000_000:
            continue
        sell_orders = int(sell_str.replace(".", "").replace(",", "")) if sell_str.strip() else 0
        conn.execute(
            """INSERT INTO freeports (structure_id, name, system, region, sell_orders, last_seen)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(structure_id) DO UPDATE SET
                   name=excluded.name, system=excluded.system,
                   region=excluded.region, sell_orders=excluded.sell_orders,
                   last_seen=excluded.last_seen""",
            (sid, name.strip(), system.strip(), region.strip(), sell_orders, now)
        )
        inserted += 1

    conn.commit(); conn.close()
    kv_set("freeports", "last_scrape", now)
    log.info("[Freeports] Stored/updated %d player market structures.", inserted)


def _ensure_universe_cache() -> None:
    """Build or rebuild the universe cache from the CCP SDE zip.

    Version check strategy:
      1. Fetch schema-changelog.yaml (small, always public).
         It lists every build that changed schemas, with which files/fields.
      2. Get the latest buildNumber from _sde.jsonl (tiny).
      3. If our stored build == latest → skip entirely.
      4. If builds exist between stored and latest, check whether any of them
         touched the files we actually use. If none did → update stored build
         number without re-downloading the zip.
      5. Only download the full ~70MB zip when a relevant file changed or the
         cache is empty.

    Files we care about:
      mapRegions, mapConstellations, mapSolarSystems,
      categories, groups, typeDogma, types
    """
    RELEVANT_FILES = {
        "mapRegions", "mapConstellations", "mapSolarSystems",
        "categories", "groups", "typeDogma", "types",
    }

    conn  = get_universe()
    count = conn.execute("SELECT COUNT(*) as c FROM map_systems").fetchone()["c"]
    stored_build = None
    try:
        row = conn.execute("SELECT value FROM sde_meta WHERE key='build'").fetchone()
        stored_build = int(row["value"]) if row else None
    except Exception:
        pass
    conn.close()

    # Fetch schema-changelog.yaml once. It is the only publicly accessible
    # version source (individual JSONL files return 403; only the zip and
    # this changelog are available without authentication).
    #
    # The first afterBuildNumber is the latest build. We also parse every
    # entry to build a map of (build → changed files) for the skip logic.
    import re as _re

    latest_build    = None
    changelog_builds = []   # [(afterBuildNumber, {file_names}), ...]
    try:
        r = requests.get(
            "https://developers.eveonline.com/static-data/tranquility/schema-changelog.yaml",
            timeout=15
        )
        if r.status_code == 200:
            current_num   = None
            current_files = set()
            for line in r.text.splitlines():
                # New entry — "- afterBuildNumber: 3241024"
                m = _re.match(r"^- afterBuildNumber:\s*(\d+)", line)
                if m:
                    if current_num is not None:
                        changelog_builds.append((current_num, current_files))
                    current_num   = int(m.group(1))
                    current_files = set()
                    if latest_build is None:
                        latest_build = current_num   # first entry = latest
                    continue
                # File/field name line — "      mapSolarSystems: added."
                m = _re.match(r"^\s{4,}([a-zA-Z][a-zA-Z0-9_]+):\s", line)
                if m and current_num is not None:
                    current_files.add(m.group(1))
            if current_num is not None:
                changelog_builds.append((current_num, current_files))
        else:
            log.debug("[SDE] schema-changelog.yaml returned HTTP %s", r.status_code)
    except Exception as exc:
        log.debug("[SDE] Could not fetch schema-changelog.yaml: %s", exc)

    if not latest_build:
        if count:
            log.debug("[SDE] Could not check SDE version — keeping existing cache.")
            return
        # No cache and no version info — attempt build anyway
    elif count and stored_build and stored_build == latest_build:
        log.debug("[SDE] Universe cache is current (build %s).", latest_build)
        return

    # ── Decide whether to rebuild based on which files changed ───────────────
    relevant_changed = False

    if count and stored_build and latest_build:
        newer = [(b, f) for b, f in changelog_builds if b > stored_build]
        if not newer:
            # Build number advanced but no schema changelog entry — data-only
            # patch (e.g. new items). Rebuild to pick up the new type names.
            relevant_changed = True
        else:
            for _b, files in newer:
                if files & RELEVANT_FILES:
                    relevant_changed = True
                    break
            if not relevant_changed:
                # All changes are in files we don't use. Advance stored build
                # and skip the 70 MB download.
                c = get_universe()
                c.execute("INSERT OR REPLACE INTO sde_meta VALUES ('build', ?)",
                          (str(latest_build),))
                c.commit(); c.close()
                log.info("[SDE] Build %s → %s: no relevant changes — skipping zip.",
                         stored_build, latest_build)
                return
    else:
        relevant_changed = True  # cache empty or no stored build

    if stored_build and stored_build != latest_build:
        log.info("[SDE] SDE updated (%s → %s). Rebuilding.", stored_build, latest_build)
    elif not count:
        log.info("[SDE] Universe cache empty — building (build %s).", latest_build)

    global _apparel_ids_loaded
    _apparel_ids_loaded = False
    log.info("[SDE] Downloading CCP SDE zip (build %s)...", latest_build)

    CCP_ZIP_URL = "https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip"

    def bulk_insert(table, rows, cols):
        conn = get_universe()
        conn.executemany(
            f"INSERT OR IGNORE INTO {table} VALUES ({','.join(['?']*cols)})", rows
        )
        conn.commit()
        conn.close()

    try:
        # Download the full SDE zip once, extract needed files in memory
        r = requests.get(CCP_ZIP_URL, timeout=300)
        r.raise_for_status()
        log.info("[SDE] Downloaded %d MB. Extracting...", len(r.content) // 1_000_000)

        zf = zipfile.ZipFile(io.BytesIO(r.content))

        def read_jsonl(filename: str) -> list:
            """Read a JSONL file from the in-memory zip."""
            try:
                data = zf.read(filename).decode("utf-8")
            except KeyError:
                # Some zips have a subfolder prefix
                matches = [n for n in zf.namelist() if n.endswith("/" + filename) or n == filename]
                if not matches:
                    raise FileNotFoundError(f"{filename} not found in zip")
                data = zf.read(matches[0]).decode("utf-8")
            return [json.loads(line) for line in data.splitlines() if line.strip()]

        # ── Map data: all from CCP SDE ────────────────────────────────────────

        # mapRegions.jsonl: _key=regionID, name.en
        rows = []
        for obj in read_jsonl("mapRegions.jsonl"):
            try:
                rows.append((obj["_key"], obj["name"]["en"]))
            except (KeyError, TypeError):
                pass
        bulk_insert("map_regions", rows, 2)
        log.info("[SDE] %d regions.", len(rows))

        # mapConstellations.jsonl: _key=constellationID, name.en, regionID
        rows = []
        for obj in read_jsonl("mapConstellations.jsonl"):
            try:
                rows.append((obj["_key"], obj["name"]["en"], obj["regionID"]))
            except (KeyError, TypeError):
                pass
        bulk_insert("map_constellations", rows, 3)
        log.info("[SDE] %d constellations.", len(rows))

        # mapSolarSystems.jsonl: _key=solarSystemID, name.en, constellationID,
        #                        regionID, securityStatus
        rows = []
        for obj in read_jsonl("mapSolarSystems.jsonl"):
            try:
                rows.append((
                    obj["_key"],
                    obj["name"]["en"],
                    obj["constellationID"],
                    obj["regionID"],
                    float(obj.get("securityStatus") or 0),
                ))
            except (KeyError, TypeError):
                pass
        bulk_insert("map_systems", rows, 5)
        log.info("[SDE] %d solar systems.", len(rows))

        # ── Type data ─────────────────────────────────────────────────────────

        # categories.jsonl: _key=categoryID, name.en
        cat_names = {}
        for obj in read_jsonl("categories.jsonl"):
            try:
                cat_names[obj["_key"]] = obj["name"]["en"]
            except (KeyError, TypeError):
                pass
        log.info("[SDE] %d categories.", len(cat_names))

        # groups.jsonl: _key=groupID, name.en, categoryID
        group_info = {}
        for obj in read_jsonl("groups.jsonl"):
            try:
                group_info[obj["_key"]] = {
                    "category_id": obj["categoryID"],
                    "group_name":  obj["name"]["en"],
                }
            except (KeyError, TypeError):
                pass
        log.info("[SDE] %d groups.", len(group_info))

        # typeDogma.jsonl: _key=typeID, dogmaAttributes=[{attributeID, value}]
        # attributeID 633 = metaLevel (0=T1, 1-4=Meta, 5=T2/T3, 6=Storyline,
        #                              7-9=Faction, 10-14=Deadspace, 15=Officer)
        META_LEVEL_ATTR = 633
        meta_levels = {}
        for obj in read_jsonl("typeDogma.jsonl"):
            try:
                for attr in obj.get("dogmaAttributes", []):
                    if attr["attributeID"] == META_LEVEL_ATTR:
                        meta_levels[obj["_key"]] = int(attr["value"])
                        break
            except (KeyError, TypeError, ValueError):
                pass
        log.info("[SDE] %d meta level values.", len(meta_levels))

        # types.jsonl: _key=typeID, name.en, groupID
        # Build type_names and type_groups in one pass
        type_name_rows = []
        type_group_rows = []
        for obj in read_jsonl("types.jsonl"):
            try:
                tid  = obj["_key"]
                gid  = obj["groupID"]
                name = obj["name"]["en"]
                gi   = group_info.get(gid)
                if name:
                    type_name_rows.append((tid, name))
                if gi is None:
                    continue
                type_group_rows.append((
                    tid, gid,
                    gi["group_name"],
                    gi["category_id"],
                    cat_names.get(gi["category_id"], ""),
                    meta_levels.get(tid, 0),
                ))
            except (KeyError, TypeError):
                pass

        conn = get_universe()
        conn.executemany("INSERT OR IGNORE INTO type_names VALUES (?,?)", type_name_rows)
        conn.commit(); conn.close()
        bulk_insert("type_groups", type_group_rows, 6)
        log.info("[SDE] %d type names, %d type→group→category mappings. Universe cache ready.",
                 len(type_name_rows), len(type_group_rows))

        # Record the build number we just downloaded
        if latest_build:
            conn = get_universe()
            conn.execute("INSERT OR REPLACE INTO sde_meta VALUES ('build', ?)", (latest_build,))
            conn.commit(); conn.close()
            log.info("[SDE] Recorded build %s.", latest_build)

    except Exception as exc:
        log.error("[SDE] Cache build failed: %s", exc, exc_info=True)


def search_systems(query: str) -> list:
    q = query.strip()
    if not q:
        return []
    conn = get_universe()
    rows = conn.execute("""
        SELECT s.system_id, s.name, s.security,
               r.name AS region_name,
               c.name AS constellation_name
        FROM map_systems s
        LEFT JOIN map_regions       r ON r.region_id        = s.region_id
        LEFT JOIN map_constellations c ON c.constellation_id = s.constellation_id
        WHERE s.name LIKE ? ESCAPE '\\'
        ORDER BY s.name
        LIMIT 20
    """, (f"{q}%",)).fetchall()
    conn.close()
    return [{
        "id":            r["system_id"],
        "name":          r["name"],
        "security":      round(r["security"], 1) if r["security"] is not None else None,
        "region":        r["region_name"] or "",
        "constellation": r["constellation_name"] or "",
    } for r in rows]

# ---------------------------------------------------------------------------
# SSO (PKCE)
# ---------------------------------------------------------------------------

def get_all_characters() -> list:
    """Return all authenticated characters from DB."""
    conn = get_user()
    rows = conn.execute(
        "SELECT character_id, character_name, is_primary FROM characters ORDER BY is_primary DESC, character_name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_character_token(character_id: Optional[int] = None) -> Optional[str]:
    """Return a valid access token for the given character (or primary if None)."""
    conn = get_user()
    if character_id:
        row = conn.execute(
            "SELECT * FROM characters WHERE character_id=?", (character_id,)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM characters WHERE is_primary=1 LIMIT 1"
        ).fetchone()
        if not row:
            row = conn.execute("SELECT * FROM characters LIMIT 1").fetchone()
    conn.close()
    if not row:
        return None
    tokens = dict(row)
    if time.time() > (tokens.get("expires_at") or 0) - 60:
        return _refresh_character_token(tokens)
    return tokens.get("access_token")


def sso_get_token() -> Optional[str]:
    """Return primary character token (backwards compat)."""
    # Check legacy kv store first for smooth migration
    tokens = kv_get("auth", "tokens")
    if tokens and time.time() < tokens.get("expires_at", 0) - 60:
        return tokens.get("access_token")
    return get_character_token()


def _refresh_character_token(tokens: dict) -> Optional[str]:
    """Refresh a character token and persist it."""
    client_id = kv_get("config", "esi_client_id")
    if not client_id:
        return None
    try:
        r = requests.post(SSO_TOKEN_URL, data={
            "grant_type":    "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id":     client_id,
        }, timeout=15)
        if r.status_code == 200:
            new_tok = r.json()
            expires_at = time.time() + new_tok.get("expires_in", 1199)
            conn = get_user()
            conn.execute(
                "UPDATE characters SET access_token=?, refresh_token=?, expires_at=? WHERE character_id=?",
                (new_tok["access_token"], new_tok.get("refresh_token", tokens["refresh_token"]),
                 expires_at, tokens["character_id"])
            )
            conn.commit(); conn.close()
            return new_tok["access_token"]
        log.warning("[SSO] Refresh failed for char %s: HTTP %s", tokens.get("character_id"), r.status_code)
    except Exception as exc:
        log.warning("[SSO] Refresh error: %s", exc)
    return None
    try:
        r = requests.post(SSO_TOKEN_URL, data={
            "grant_type":    "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id":     client_id,
        }, timeout=15)
        if r.status_code == 200:
            new_tokens               = r.json()
            new_tokens["expires_at"] = time.time() + new_tokens.get("expires_in", 1199)
            kv_set("auth", "tokens", new_tokens)
            log.info("[SSO] Token refreshed.")
            return new_tokens["access_token"]
        log.warning("[SSO] Refresh failed: HTTP %s", r.status_code)
    except Exception as exc:
        log.warning("[SSO] Refresh error: %s", exc)
    return None


def _complete_auth(code: str) -> None:
    client_id = kv_get("config", "esi_client_id")
    verifier  = kv_get("auth",   "pkce_verifier")
    if not client_id or not code:
        log.error("[SSO] Missing client_id or code.")
        return
    try:
        r = requests.post(SSO_TOKEN_URL, data={
            "grant_type":    "authorization_code",
            "code":          code,
            "client_id":     client_id,
            "redirect_uri":  SSO_REDIRECT,
            "code_verifier": verifier,
        }, timeout=15)
        if r.status_code != 200:
            log.error("[SSO] Token exchange failed: %s %s", r.status_code, r.text)
            return
        tokens               = r.json()
        tokens["expires_at"] = time.time() + tokens.get("expires_in", 1199)
        vr = requests.get(
            SSO_VERIFY,
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
            timeout=15,
        )
        if vr.status_code != 200:
            log.error("[SSO] Verify failed: HTTP %s", vr.status_code)
            return
        verify = vr.json()
        char_id   = verify.get("CharacterID")
        char_name = verify.get("CharacterName")
        # Store in characters table
        conn = get_user()
        existing = conn.execute("SELECT COUNT(*) as c FROM characters").fetchone()["c"]
        conn.execute("""
            INSERT INTO characters (character_id, character_name, access_token, refresh_token, expires_at, is_primary)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(character_id) DO UPDATE SET
                character_name=excluded.character_name,
                access_token=excluded.access_token,
                refresh_token=excluded.refresh_token,
                expires_at=excluded.expires_at
        """, (char_id, char_name, tokens["access_token"],
              tokens.get("refresh_token"), tokens["expires_at"],
              1 if existing == 0 else 0))
        conn.commit(); conn.close()
        # Also keep legacy kv for backwards compat
        kv_set("auth", "tokens",         tokens)
        kv_set("auth", "character_id",   char_id)
        kv_set("auth", "character_name", char_name)
        log.info("[SSO] Authenticated as %s (%s)", char_name, char_id)
    except Exception as exc:
        log.error("[SSO] Auth error: %s", exc, exc_info=True)

# ---------------------------------------------------------------------------
# Structure helpers
# ---------------------------------------------------------------------------

def get_location_name(location_id: int) -> str:
    """Return a display name for any location_id (station or structure)."""
    if location_id in NPC_HUBS:
        return NPC_HUBS[location_id]["name"]
    conn = get_db()
    row  = conn.execute(
        "SELECT name FROM structure_names WHERE structure_id=?", (location_id,)
    ).fetchone()
    conn.close()
    if row and row["name"]:
        return row["name"]
    return f"Structure {location_id}"


def _esi_ui_post(endpoint: str, params: dict):
    token = sso_get_token()
    if not token:
        return jsonify({"error": "not_authenticated"}), 401
    try:
        r = requests.post(
            f"{ESI_BASE}{endpoint}",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        return jsonify({"ok": r.status_code == 204})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

# ---------------------------------------------------------------------------
# Routes — pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/donate")
def donate_page():
    return render_template("donate.html")

# ---------------------------------------------------------------------------
# Routes — SSO
# ---------------------------------------------------------------------------

@app.route("/api/auth/start", methods=["POST"])
def auth_start():
    client_id = kv_get("config", "esi_client_id") or ""
    if not client_id:
        return jsonify({"ok": False, "error": "No ESI Client ID saved."}), 400
    verifier  = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    kv_set("auth", "pkce_verifier", verifier)
    params = {
        "response_type":         "code",
        "client_id":             client_id,
        "redirect_uri":          SSO_REDIRECT,
        "scope":                 SSO_SCOPES,
        "code_challenge":        challenge,
        "code_challenge_method": "S256",
        "state":                 secrets.token_hex(8),
    }
    return jsonify({"ok": True, "url": SSO_AUTH_URL + "?" + urllib.parse.urlencode(params)})


@app.route("/callback")
def oauth_callback():
    code  = request.args.get("code", "")
    error = request.args.get("error", "")
    if error:
        log.error("[SSO] EVE returned error: %s", error)
    elif code:
        _complete_auth(code)
    return redirect("/")


@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    data    = request.get_json(force=True) or {}
    char_id = data.get("character_id")
    if char_id:
        conn = get_user()
        conn.execute("DELETE FROM characters WHERE character_id=?", (char_id,))
        # If this was primary, promote next character
        conn.execute("""
            UPDATE characters SET is_primary=1
            WHERE character_id=(SELECT character_id FROM characters LIMIT 1)
              AND NOT EXISTS (SELECT 1 FROM characters WHERE is_primary=1)
        """)
        conn.commit(); conn.close()
        log.info("[SSO] Removed character %s.", char_id)
    else:
        # Logout all
        conn = get_user()
        conn.execute("DELETE FROM characters")
        conn.commit(); conn.close()
        for key in ("tokens", "character_id", "character_name"):
            kv_set("auth", key, None)
        log.info("[SSO] All characters logged out.")
    return jsonify({"ok": True})


@app.route("/api/auth/set_primary", methods=["POST"])
def auth_set_primary():
    data    = request.get_json(force=True) or {}
    char_id = data.get("character_id")
    if not char_id:
        return jsonify({"ok": False}), 400
    conn = get_user()
    conn.execute("UPDATE characters SET is_primary=0")
    conn.execute("UPDATE characters SET is_primary=1 WHERE character_id=?", (char_id,))
    conn.commit(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/auth/save_client_id", methods=["POST"])
def auth_save_client_id():
    data = request.get_json(force=True) or {}
    cid  = (data.get("client_id") or "").strip()
    if not cid:
        return jsonify({"ok": False, "error": "Empty client ID"}), 400
    kv_set("config", "esi_client_id", cid)
    log.info("[SSO] Client ID saved.")
    return jsonify({"ok": True})


@app.route("/api/auth/status")
def api_auth_status():
    characters = get_all_characters()
    return jsonify({
        "authenticated":  len(characters) > 0,
        "characters":     characters,
        "character_name": characters[0]["character_name"] if characters else None,
        "character_id":   characters[0]["character_id"]   if characters else None,
        "has_client_id":  bool(kv_get("config", "esi_client_id")),
    })


@app.route("/api/auth/characters")
def api_auth_characters():
    return jsonify(get_all_characters())

# ---------------------------------------------------------------------------
# Routes — market locations
# ---------------------------------------------------------------------------

@app.route("/api/npc_hubs")
def api_npc_hubs():
    """Return the list of NPC hub stations with their IDs and short names."""
    return jsonify([
        {"id": sid, "name": h["name"], "short": h["short"]}
        for sid, h in NPC_HUBS.items()
    ])


@app.route("/api/freeports")
def api_freeports():
    """
    Return player market structures discovered via A4E scrape, sorted by sell orders.
    Falls back to empty list if scrape hasn't run yet.
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT structure_id, name, system, region, sell_orders FROM freeports "
        "ORDER BY sell_orders DESC LIMIT 50"
    ).fetchall()
    conn.close()
    return jsonify([{
        "id":          r["structure_id"],
        "name":        r["name"],
        "system":      r["system"],
        "region":      r["region"],
        "sell_orders": r["sell_orders"],
    } for r in rows])


@app.route("/api/freeports/refresh", methods=["POST"])
def api_freeports_refresh():
    """Force a fresh scrape of A4E market hubs."""
    kv_set("freeports", "last_scrape", None)
    threading.Thread(target=_scrape_freeports, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/systems/<int:system_id>/structures")
def api_structures_in_system(system_id):
    """Return market-capable structures visible to the authed character."""
    token = sso_get_token()
    if not token:
        return jsonify({"error": "not_authenticated"}), 401
    conn_u  = get_user()
    row_u   = conn_u.execute(
        "SELECT character_id FROM characters WHERE is_primary=1 LIMIT 1"
    ).fetchone()
    if not row_u:
        row_u = conn_u.execute("SELECT character_id FROM characters LIMIT 1").fetchone()
    conn_u.close()
    char_id = row_u["character_id"] if row_u else None
    if not char_id:
        return jsonify({"error": "not_authenticated"}), 401

    conn    = get_universe()
    sys_row = conn.execute(
        "SELECT name FROM map_systems WHERE system_id=?", (system_id,)
    ).fetchone()
    conn.close()
    if not sys_row:
        return jsonify({"error": "system_not_found"}), 404
    system_name = sys_row["name"]

    search_data, _ = esi_get_authed(
        f"/characters/{char_id}/search/", token,
        params={"categories": "structure", "search": system_name, "strict": "false"}
    )
    struct_ids = (search_data or {}).get("structure", [])
    log.info("[Structures] Search for '%s' returned %d IDs.", system_name, len(struct_ids))
    if not struct_ids:
        return jsonify([])

    conn       = get_db()
    cached_map = {}
    to_fetch   = []
    for sid in struct_ids:
        row = conn.execute(
            "SELECT name, solar_system_id, type_id FROM structure_names WHERE structure_id=?",
            (sid,)
        ).fetchone()
        if row and row["solar_system_id"] is not None:
            cached_map[sid] = dict(row)
        else:
            to_fetch.append(sid)
    conn.close()

    new_rows = []
    for sid in to_fetch:
        data, _ = esi_get_authed(f"/universe/structures/{sid}/", token)
        if data:
            entry = {
                "name":            data.get("name", f"Structure {sid}"),
                "solar_system_id": data.get("solar_system_id"),
                "type_id":         data.get("type_id"),
            }
            cached_map[sid] = entry
            new_rows.append((sid, entry["name"], entry["solar_system_id"], entry["type_id"]))

    if new_rows:
        now  = datetime.now(timezone.utc).isoformat()
        conn = get_db()
        conn.executemany(
            "INSERT OR REPLACE INTO structure_names "
            "(structure_id, name, solar_system_id, type_id, fetched_at) VALUES (?,?,?,?,?)",
            [(sid, name, sys_id, tid, now) for sid, name, sys_id, tid in new_rows]
        )
        conn.commit()
        conn.close()

    results = sorted(
        [
            {
                "id":        sid,
                "name":      info["name"],
                "type_id":   info.get("type_id"),
                "type_name": MARKET_STRUCTURE_TYPES.get(info.get("type_id"), "Unknown"),
            }
            for sid, info in cached_map.items()
            if info.get("solar_system_id") == system_id
            and info.get("type_id") in MARKET_STRUCTURE_TYPES
        ],
        key=lambda x: x["name"]
    )
    log.info("[Structures] %d market structures found in %s.", len(results), system_name)
    return jsonify(results)

# ---------------------------------------------------------------------------
# Routes — snapshots
# ---------------------------------------------------------------------------

@app.route("/api/snapshot/<int:location_id>", methods=["POST"])
def api_snapshot(location_id):
    """
    Fetch and store a market snapshot for any location.
    Automatically routes to NPC hub (Fuzzworks) or structure (ESI auth).
    """
    exclude_skins = request.args.get("exclude_skins", "1") == "1"
    try:
        if location_id in NPC_HUBS:
            return jsonify(fetch_npc_hub_snapshot(location_id, exclude_skins=exclude_skins))
        else:
            return jsonify(fetch_structure_snapshot(location_id, exclude_skins=exclude_skins))
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/api/snapshot/<int:location_id>/status")
def api_snapshot_status(location_id):
    today = date.today().isoformat()
    conn  = get_db()
    cnt   = conn.execute(
        "SELECT COUNT(*) as c FROM market_snapshots "
        "WHERE snapshot_date=? AND location_id=?",
        (today, location_id)
    ).fetchone()["c"]
    conn.close()
    return jsonify({
        "date":         today,
        "has_snapshot": cnt > 0,
        "count":        cnt,
        "name":         get_location_name(location_id),
    })


@app.route("/api/snapshots/dates/<int:location_id>")
def api_snapshot_dates(location_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT snapshot_date FROM market_snapshots "
        "WHERE location_id=? ORDER BY snapshot_date DESC LIMIT 30",
        (location_id,)
    ).fetchall()
    conn.close()
    return jsonify([r["snapshot_date"] for r in rows])

# ---------------------------------------------------------------------------
# Routes — comparison
# ---------------------------------------------------------------------------

@app.route("/api/compare/<int:src_id>/<int:dst_id>")
def api_compare(src_id, dst_id):
    snap_date = request.args.get("date", date.today().isoformat())
    return jsonify(get_comparison(src_id, dst_id, snap_date))


@app.route("/api/compare/<int:src_id>/<int:dst_id>/csv")
def api_compare_csv(src_id, dst_id):
    snap_date = request.args.get("date", date.today().isoformat())
    data      = get_comparison(src_id, dst_id, snap_date)
    src_name  = get_location_name(src_id).split(" - ")[0].replace(" ", "_")
    dst_name  = get_location_name(dst_id).split(" - ")[0].replace(" ", "_")
    filename  = f"caravanserai_{src_name}_vs_{dst_name}_{snap_date}.csv"

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow([
        "type_id", "item_name",
        "src_supply", "src_demand", "src_split", "src_sell", "src_buy",
        "dst_supply", "dst_demand", "dst_split", "dst_sell", "dst_buy",
        "src_spread_pct", "dst_spread_pct",
        "import_margin_pct", "export_margin_pct",
    ])
    for row in data:
        w.writerow([
            row["type_id"],        row["type_name"],
            row["src_supply"],     row["src_demand"],
            row["src_split"] or "", row["src_sell"] or "", row["src_buy"] or "",
            row["dst_supply"],     row["dst_demand"],
            row["dst_split"] or "", row["dst_sell"] or "", row["dst_buy"] or "",
            row["src_spread"]    or "",
            row["dst_spread"]    or "",
            row["import_margin"]  or "",
            row["export_margin"]  or "",
        ])
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ---------------------------------------------------------------------------
# Routes — SDE / system search
# ---------------------------------------------------------------------------

@app.route("/api/filter_options")
def api_filter_options():
    """Return sorted lists of unique category and group names for filter dropdowns."""
    conn  = get_universe()
    cats  = conn.execute(
        "SELECT DISTINCT category_name FROM type_groups "
        "WHERE category_name != '' ORDER BY category_name"
    ).fetchall()
    groups = conn.execute(
        "SELECT DISTINCT group_name FROM type_groups "
        "WHERE group_name != '' ORDER BY group_name"
    ).fetchall()
    conn.close()
    return jsonify({
        "categories": [r["category_name"] for r in cats],
        "groups":     [r["group_name"]     for r in groups],
    })


@app.route("/api/sde/status")
def api_sde_status():
    conn = get_universe()
    sys_count = conn.execute("SELECT COUNT(*) as c FROM map_systems").fetchone()["c"]
    reg_count = conn.execute("SELECT COUNT(*) as c FROM map_regions").fetchone()["c"]
    conn.close()
    return jsonify({"ready": sys_count > 0, "systems": sys_count, "regions": reg_count})


@app.route("/api/search/systems")
def api_search_systems():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    return jsonify(search_systems(q))

# ---------------------------------------------------------------------------
# Routes — in-game UI windows
# ---------------------------------------------------------------------------

@app.route("/api/orders/character")
def api_character_orders():
    """Return open orders for all authenticated characters, summed per type.

    Optional query params:
      src  — location_id of the source market
      dst  — location_id of the destination market

    When provided, only orders at src or dst are included. Orders at
    unrelated locations (e.g. a different trade hub) are excluded.
    """
    src_id = request.args.get("src", type=int)
    dst_id = request.args.get("dst", type=int)
    locations = {l for l in (src_id, dst_id) if l is not None}

    result = {}
    for char in get_all_characters():
        token = get_character_token(char["character_id"])
        if not token:
            continue
        orders, _ = esi_get_authed(
            f"/characters/{char['character_id']}/orders/", token
        )
        for o in (orders or []):
            if o.get("state", "open") != "open":
                continue
            if locations and o.get("location_id") not in locations:
                continue
            tid = str(o["type_id"])
            if tid not in result:
                result[tid] = {"char_buy": 0, "char_sell": 0}
            key = "char_buy" if o.get("is_buy_order", False) else "char_sell"
            result[tid][key] += o.get("volume_remain", 0)
    return jsonify(result)


@app.route("/api/orders/corporation")
def api_corporation_orders():
    """Return open orders for the primary character's corporation, summed per type.

    Optional query params:
      src  — location_id of the source market
      dst  — location_id of the destination market

    When provided, only orders at src or dst are included.
    """
    src_id = request.args.get("src", type=int)
    dst_id = request.args.get("dst", type=int)
    locations = {l for l in (src_id, dst_id) if l is not None}

    token = sso_get_token()
    conn  = get_user()
    row   = conn.execute(
        "SELECT character_id FROM characters WHERE is_primary=1 LIMIT 1"
    ).fetchone()
    if not row:
        row = conn.execute("SELECT character_id FROM characters LIMIT 1").fetchone()
    conn.close()
    if not token or not row:
        return jsonify({}), 401
    char_id = row["character_id"]
    char_info, _ = esi_get_authed(f"/characters/{char_id}/", token)
    if not char_info:
        return jsonify({})
    corp_id = char_info.get("corporation_id")
    if not corp_id:
        return jsonify({})
    orders = esi_get_authed_all_pages(
        f"/corporations/{corp_id}/orders/", token
    )
    result = {}
    for o in orders:
        if o.get("state", "open") != "open":
            continue
        if locations and o.get("location_id") not in locations:
            continue
        tid = str(o["type_id"])
        if tid not in result:
            result[tid] = {"corp_buy": 0, "corp_sell": 0}
        key = "corp_buy" if o.get("is_buy_order", False) else "corp_sell"
        result[tid][key] += o.get("volume_remain", 0)
    return jsonify(result)


@app.route("/api/ui/market/<int:type_id>", methods=["POST"])
def api_open_market_window(type_id):
    return _esi_ui_post("/ui/openwindow/marketdetails/", {"type_id": type_id})


@app.route("/api/ui/waypoint/<int:destination_id>", methods=["POST"])
def api_set_waypoint(destination_id):
    """Set autopilot destination for one or all authenticated characters.

    Query param:
      scope = 'primary' (default) | 'all'

    ESI params (all required by ESI, sent as query string):
      destination_id          — station or structure ID
      add_to_beginning=false  — replace destination, not prepend
      clear_other_waypoints=true
    """
    scope = request.args.get("scope", "primary")

    chars = get_all_characters()
    if not chars:
        return jsonify({"ok": False, "error": "No authenticated characters."}), 401

    if scope == "primary":
        primary = next((c for c in chars if c["is_primary"]), chars[0])
        targets = [primary]
    else:
        targets = chars

    results = []
    for char in targets:
        token = get_character_token(char["character_id"])
        if not token:
            results.append({"char": char["character_name"], "ok": False, "error": "token expired"})
            continue
        try:
            r = requests.post(
                f"{ESI_BASE}/ui/autopilot/waypoint/",
                params={
                    "destination_id":        destination_id,
                    "add_to_beginning":      "false",
                    "clear_other_waypoints": "true",
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            results.append({"char": char["character_name"], "ok": r.status_code == 204})
        except Exception as exc:
            results.append({"char": char["character_name"], "ok": False, "error": str(exc)})

    all_ok = all(r["ok"] for r in results)
    return jsonify({"ok": all_ok, "results": results})


@app.route("/api/ui/corp/<int:corp_id>", methods=["POST"])
def api_open_corp_info(corp_id):
    return _esi_ui_post("/ui/openwindow/information/", {"target_id": corp_id})


@app.route("/api/ui/openwindow/wallet", methods=["POST"])
def api_open_wallet_window():
    return _esi_ui_post("/ui/openwindow/wallet/", {})

# ---------------------------------------------------------------------------
# Auto-updater
# ---------------------------------------------------------------------------

GITHUB_REPO     = "Amalgamator/EVE-Caravanserai"
CURRENT_VERSION = "v0.2.5-alpha"   # keep in sync with git tags


def _is_git_repo() -> bool:
    """Return True if app.py is running inside a git working tree."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


@app.route("/api/update/check")
def api_update_check():
    """Check GitHub for a newer release tag.

    Tries /releases/latest first. If the repo has no releases (404),
    falls back to /tags to find the newest semver tag.
    Returns up_to_date=True and no badge when version cannot be determined.
    """
    headers = {"Accept": "application/vnd.github+json"}
    tag = url = notes = ""
    try:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            headers=headers, timeout=10,
        )
        if r.status_code == 200:
            data  = r.json()
            tag   = data.get("tag_name", "")
            url   = data.get("html_url", "")
            notes = data.get("body", "")[:500]
        elif r.status_code == 404:
            # No releases published yet — try tags
            r2 = requests.get(
                f"https://api.github.com/repos/{GITHUB_REPO}/tags",
                headers=headers, timeout=10,
            )
            if r2.status_code == 200:
                tags = r2.json()
                if tags:
                    tag = tags[0].get("name", "")
                    url = f"https://github.com/{GITHUB_REPO}/releases/tag/{tag}"
        else:
            log.debug("[Update] GitHub returned HTTP %s: %s", r.status_code, r.text[:200])
            return jsonify({"up_to_date": True, "current": CURRENT_VERSION,
                            "error": f"GitHub returned {r.status_code}"})
    except Exception as exc:
        log.debug("[Update] Could not reach GitHub: %s", exc)
        return jsonify({"up_to_date": True, "current": CURRENT_VERSION,
                        "error": str(exc)})

    if not tag:
        # No releases or tags found — nothing to report
        return jsonify({"up_to_date": True, "current": CURRENT_VERSION})

    up_to_date = (tag == CURRENT_VERSION)
    return jsonify({
        "current":    CURRENT_VERSION,
        "latest":     tag,
        "up_to_date": up_to_date,
        "url":        url,
        "notes":      notes,
        "can_update": _is_git_repo() and not up_to_date,
    })


@app.route("/api/update/apply", methods=["POST"])
def api_update_apply():
    """Pull the latest version from git and restart the process.

    Only works when running from a git clone. Returns immediately with
    {"ok": true} — the process restarts in a background thread so the
    response can be delivered first. The client should poll /api/update/check
    until the new version is reported, then reload.
    """
    if not _is_git_repo():
        return jsonify({"ok": False, "error": "Not running from a git repo. Update manually."}), 400

    app_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        # If .gitignore is correct, untracked files (DBs, etc.) are invisible
        # to git and won't block the pull. Only staged/modified tracked files
        # can block --ff-only. Reset any unintentional local modifications to
        # tracked files so the pull always succeeds cleanly.
        subprocess.run(
            ["git", "checkout", "--", "."],
            cwd=app_dir, capture_output=True, timeout=10,
        )
        result = subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=app_dir,
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            log.error("[Update] git pull failed: %s", result.stderr.strip())
            return jsonify({
                "ok":     False,
                "error":  "git pull failed",
                "detail": result.stderr.strip(),
            }), 500
        log.info("[Update] git pull succeeded: %s", result.stdout.strip())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    # Restart in a thread so this response is delivered first.
    # We cannot use os.execv directly because the Flask server still holds
    # the socket open — the replacement process would fail to bind port 8182.
    # Instead: spawn a fresh child process, then exit the current one.
    # The child waits 1s to ensure the parent has fully released the socket.
    def _restart():
        time.sleep(0.5)
        log.info("[Update] Restarting process...")
        subprocess.Popen(
            [sys.executable] + sys.argv,
            env={**os.environ, "_CARAVANSERAI_RESTART": "1"},
        )
        # Hard exit — releases the socket immediately
        os._exit(0)

    threading.Thread(target=_restart, daemon=False).start()
    return jsonify({"ok": True, "output": result.stdout.strip()})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # When restarting after an update, wait for the parent process to fully
    # release the socket before attempting to bind. The parent calls os._exit(0)
    # after spawning us, but the OS may take a moment to release the port.
    if os.environ.get("_CARAVANSERAI_RESTART"):
        time.sleep(1.5)

    init_db()
    if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Thread(target=_ensure_universe_cache, daemon=True).start()
        threading.Thread(target=_scrape_freeports, daemon=True).start()

    # Retry binding the port a few times — the previous process may still be
    # releasing the socket when we start (especially after an auto-update).
    port = 8182
    for attempt in range(5):
        try:
            app.run(debug=False, port=port)
            break
        except OSError as e:
            if "Address already in use" in str(e) and attempt < 4:
                log.warning("Port %d in use, retrying in 1s… (%d/4)", port, attempt + 1)
                time.sleep(1.0)
            else:
                raise