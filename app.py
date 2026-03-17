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
import bz2
import csv
import gzip
import hashlib
import io
import json
import logging
import os
import secrets
import sqlite3
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

DB_PATH           = "caravanserai.db"
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

# Pre-cached freeport structures (system → structure_id).
# These are pre-loaded so users don't need to search for the most common ones.
KNOWN_FREEPORTS = {
    "Perimeter":  1028858195912,  # Tranquility Trading Tower
    "Ashab":      1030049082711,  # Ashab - IChooseYou Trade Hub
    "Amamake":    1028979195912,  # Amamake - 3 Final Countdown
}

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
#   esi-ui.open_window.v1               open in-game windows
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
    "esi-markets.read_character_orders.v1",   # character buy/sell order quantities
    "esi-markets.read_corporation_orders.v1", # corporation buy/sell order quantities
])

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    conn = get_db()
    conn.executescript("""
        -- Unified market snapshots table.
        -- location_id is either a station_id (NPC hub) or a structure_id.
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

        CREATE TABLE IF NOT EXISTS structure_names (
            structure_id    INTEGER PRIMARY KEY,
            name            TEXT,
            solar_system_id INTEGER,
            type_id         INTEGER,
            is_freeport     INTEGER DEFAULT 0,
            fetched_at      TEXT
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
    """)
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Key-value store
# ---------------------------------------------------------------------------

def kv_get(namespace: str, key: str):
    conn = get_db()
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
    conn = get_db()
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
    conn    = get_db()
    c       = conn.cursor()
    missing = [
        tid for tid in type_ids
        if not c.execute("SELECT 1 FROM type_names WHERE type_id=?", (tid,)).fetchone()
    ]
    log.info("Type names: %d total, %d cached, %d to fetch",
             len(type_ids), len(type_ids) - len(missing), len(missing))
    for i in range(0, len(missing), 1000):
        batch = missing[i:i + 1000]
        try:
            r = requests.post(f"{ESI_BASE}/universe/names/", json=batch, timeout=20)
            if r.status_code == 200:
                for item in r.json():
                    c.execute("INSERT OR IGNORE INTO type_names VALUES (?,?)",
                              (item["id"], item["name"]))
            else:
                log.warning("/universe/names/ returned HTTP %s", r.status_code)
        except Exception as exc:
            log.warning("/universe/names/ failed: %s", exc)
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Apparel / SKIN filters
# ---------------------------------------------------------------------------

# SKIN name patterns — covers standard, plural, multi-ship, and special editions
_SKIN_SUFFIXES = (" SKIN", " SKINs", " Skin", "License")
_SKIN_SUBSTRINGS = ("SKIN License", "Day SKIN", "SKIN Collection")

def _is_skin_name(name: str) -> bool:
    if any(name.endswith(s) for s in _SKIN_SUFFIXES):
        return True
    if any(s in name for s in _SKIN_SUBSTRINGS):
        return True
    return False


_apparel_ids: set         = set()
_apparel_ids_loaded: bool = False


def get_apparel_ids() -> set:
    global _apparel_ids, _apparel_ids_loaded
    if _apparel_ids_loaded:
        return _apparel_ids
    conn = get_db()
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
    """Remove Apparel and SKIN items in-place. Returns removed count."""
    apparel   = get_apparel_ids()
    conn      = get_db()
    to_remove = set()
    for tid in type_ids:
        if tid in apparel:
            to_remove.add(tid)
        else:
            row = conn.execute(
                "SELECT name FROM type_names WHERE type_id=?", (tid,)
            ).fetchone()
            if row and _is_skin_name(row["name"]):
                to_remove.add(tid)
    conn.close()
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
        LEFT JOIN type_names tn
               ON tn.type_id = COALESCE(s.type_id, d.type_id)
        LEFT JOIN type_groups tg
               ON tg.type_id = COALESCE(s.type_id, d.type_id)
        WHERE s.location_id   = :src_id
          AND s.snapshot_date = :snap_date

        UNION

        SELECT
            d.type_id,
            tn.name,
            tg.group_name,
            tg.category_name,
            tg.meta_level,
            s.sell_units, s.buy_units, s.split_price,
            s.lowest_sell_10pct, s.highest_buy_10pct,
            d.sell_units, d.buy_units, d.split_price,
            d.lowest_sell_10pct, d.highest_buy_10pct
        FROM market_snapshots d
        LEFT JOIN market_snapshots s
               ON s.type_id      = d.type_id
              AND s.location_id  = :src_id
              AND s.snapshot_date = :snap_date
        LEFT JOIN type_names tn ON tn.type_id = d.type_id
        LEFT JOIN type_groups tg ON tg.type_id = d.type_id
        WHERE d.location_id   = :dst_id
          AND d.snapshot_date = :snap_date
          AND s.type_id IS NULL
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
    conn.close()
    return result

# ---------------------------------------------------------------------------
# SDE universe cache
# ---------------------------------------------------------------------------

def _ensure_universe_cache() -> None:
    conn  = get_db()
    count = conn.execute("SELECT COUNT(*) as c FROM map_systems").fetchone()["c"]
    conn.close()
    if count:
        return

    global _apparel_ids_loaded
    _apparel_ids_loaded = False
    log.info("[SDE] Building universe cache from Fuzzwork SDE CSVs...")

    def fetch_bz2(table: str) -> list:
        r = requests.get(f"{SDE_BASE}/{table}.csv.bz2", timeout=60)
        r.raise_for_status()
        return list(csv.DictReader(io.StringIO(bz2.decompress(r.content).decode("utf-8"))))

    def bulk_insert(table, rows, cols):
        conn = get_db()
        conn.executemany(
            f"INSERT OR IGNORE INTO {table} VALUES ({','.join(['?']*cols)})", rows
        )
        conn.commit()
        conn.close()

    try:
        rows = [(int(r["regionID"]), r["regionName"])
                for r in fetch_bz2("mapRegions") if r.get("regionID")]
        bulk_insert("map_regions", rows, 2)
        log.info("[SDE] %d regions.", len(rows))

        rows = [(int(r["constellationID"]), r["constellationName"], int(r["regionID"]))
                for r in fetch_bz2("mapConstellations") if r.get("constellationID")]
        bulk_insert("map_constellations", rows, 3)
        log.info("[SDE] %d constellations.", len(rows))

        rows = []
        for r in fetch_bz2("mapSolarSystems"):
            try:
                rows.append((int(r["solarSystemID"]), r["solarSystemName"],
                             int(r["constellationID"]), int(r["regionID"]),
                             float(r.get("security") or 0)))
            except (KeyError, ValueError):
                pass
        bulk_insert("map_systems", rows, 5)
        log.info("[SDE] %d solar systems.", len(rows))

        # invCategories: categoryID → name
        cat_names = {}
        for r in fetch_bz2("invCategories"):
            try:
                cat_names[int(r["categoryID"])] = r.get("categoryName", "")
            except (KeyError, ValueError):
                pass
        log.info("[SDE] %d categories.", len(cat_names))

        # invGroups: groupID → {categoryID, groupName}
        group_info = {}
        for r in fetch_bz2("invGroups"):
            try:
                gid = int(r["groupID"])
                cid = int(r["categoryID"])
                group_info[gid] = {"category_id": cid, "group_name": r.get("groupName", "")}
            except (KeyError, ValueError):
                pass
        log.info("[SDE] %d inventory groups.", len(group_info))

        # invTypes: typeID → group + category
        rows = []
        for r in fetch_bz2("invTypes"):
            try:
                tid = int(r["typeID"])
                gid = int(r["groupID"])
                gi  = group_info.get(gid)
                if gi is not None:
                    # metaGroupID: 1=T1, 2=T2, 3=Storyline, 4=Faction, 5=Officer,
                    #              6=Deadspace, 14=T3  — store raw for frontend filtering
                    meta = int(r["metaGroupID"]) if r.get("metaGroupID") else 0
                    rows.append((
                        tid, gid,
                        gi["group_name"],
                        gi["category_id"],
                        cat_names.get(gi["category_id"], ""),
                        meta,
                    ))
            except (KeyError, ValueError):
                pass
        bulk_insert("type_groups", rows, 6)
        log.info("[SDE] %d type→group→category mappings. Universe cache ready.", len(rows))

    except Exception as exc:
        log.error("[SDE] Cache build failed: %s", exc, exc_info=True)


def search_systems(query: str) -> list:
    q = query.strip()
    if not q:
        return []
    conn = get_db()
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
    conn = get_db()
    rows = conn.execute(
        "SELECT character_id, character_name, is_primary FROM characters ORDER BY is_primary DESC, character_name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_character_token(character_id: Optional[int] = None) -> Optional[str]:
    """Return a valid access token for the given character (or primary if None)."""
    conn = get_db()
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
            conn = get_db()
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


def _sso_refresh(tokens: dict) -> Optional[str]:
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
        verify = requests.get(
            SSO_VERIFY,
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
            timeout=15,
        ).json()
        char_id   = verify.get("CharacterID")
        char_name = verify.get("CharacterName")
        # Store in characters table
        conn = get_db()
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
        conn = get_db()
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
        conn = get_db()
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
    conn = get_db()
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
    Return known pre-cached freeports plus any the player can see.
    Pre-cached entries are always listed; auth-visible ones require a token.
    """
    results = []

    # Always include known freeports from our pre-defined list
    conn = get_db()
    for system, sid in KNOWN_FREEPORTS.items():
        row = conn.execute(
            "SELECT name FROM structure_names WHERE structure_id=?", (sid,)
        ).fetchone()
        name = row["name"] if row else f"{system} Freeport"
        results.append({"id": sid, "name": name, "system": system, "known": True})
    conn.close()

    return jsonify(results)


@app.route("/api/systems/<int:system_id>/structures")
def api_structures_in_system(system_id):
    """Return market-capable structures visible to the authed character."""
    token   = sso_get_token()
    char_id = kv_get("auth", "character_id")
    if not token or not char_id:
        return jsonify({"error": "not_authenticated"}), 401

    conn    = get_db()
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
    conn  = get_db()
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
    conn = get_db()
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
    """Return open orders for all authenticated characters, summed per type."""
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
            tid = str(o["type_id"])
            if tid not in result:
                result[tid] = {"char_buy": 0, "char_sell": 0}
            key = "char_buy" if o.get("is_buy_order", False) else "char_sell"
            result[tid][key] += o.get("volume_remain", 0)
    return jsonify(result)


@app.route("/api/orders/corporation")
def api_corporation_orders():
    """Return open orders for the primary character's corporation, summed per type."""
    token = sso_get_token()
    # Get char_id from characters table (primary), not legacy kv
    conn  = get_db()
    row   = conn.execute(
        "SELECT character_id FROM characters WHERE is_primary=1 LIMIT 1"
    ).fetchone()
    if not row:
        row = conn.execute("SELECT character_id FROM characters LIMIT 1").fetchone()
    conn.close()
    if not token or not row:
        return jsonify({}), 401
    char_id = row["character_id"]
    # Get corp ID
    char_info, _ = esi_get_authed(f"/characters/{char_id}/", token)
    if not char_info:
        return jsonify({})
    corp_id = char_info.get("corporation_id")
    if not corp_id:
        return jsonify({})
    # Fetch corp orders (all divisions)
    orders = esi_get_authed_all_pages(
        f"/corporations/{corp_id}/orders/", token
    )
    result = {}
    for o in orders:
        if o.get("state", "open") != "open":
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


@app.route("/api/ui/corp/<int:corp_id>", methods=["POST"])
def api_open_corp_info(corp_id):
    return _esi_ui_post("/ui/openwindow/information/", {"target_id": corp_id})


@app.route("/api/ui/openwindow/wallet", methods=["POST"])
def api_open_wallet_window():
    return _esi_ui_post("/ui/openwindow/wallet/", {})

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Thread(target=_ensure_universe_cache, daemon=True).start()
    app.run(debug=False, port=8182)