# -*- coding: utf-8 -*-
import os
import re
import time
import json
import logging
import threading
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask

# ==========================================================
# CONFIG
# ==========================================================
TELEGRAM_BOT_TOKEN = "8086252593:AAHrzxIVxZ3J-P6R6_IfrM1tyNcthuKsroU"
PORT = int(os.environ.get("PORT", 10000))

# Features
HOURLY_ENABLED = True
ALERTS_ENABLED = True
DAILY_SUMMARY_ENABLED = True

# Schedule (VN time)
DAILY_OPEN_HOUR = 8
DAILY_CLOSE_HOUR = 17

# Intervals
ALERT_POLL_SECONDS = 180
HEARTBEAT_INTERVAL_SEC = 600  # 10 phút heartbeat

# Thresholds
THRESHOLD_BAR_SELL = 800_000
THRESHOLD_RING_SELL = 800_000

# Networking
TG_CONNECT_TIMEOUT = 10
TG_READ_TIMEOUT = 35
UPDATES_LONGPOLL = 35
WEB_TIMEOUT = 25
CACHE_TTL = 15

# Storage
SUBSCRIBERS_FILE = "gold_subscribers.json"
STATE_FILE = "gold_state.json"

# ==========================================================
# TIMEZONE
# ==========================================================
try:
    from zoneinfo import ZoneInfo
    try:
        VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
    except:
        VN_TZ = timezone(timedelta(hours=7))
except:
    VN_TZ = timezone(timedelta(hours=7))

def now_vn() -> datetime:
    return datetime.now(VN_TZ)

def fmt_dt() -> str:
    return now_vn().strftime("%H:%M • %d/%m/%Y")

def today_key() -> str:
    return now_vn().strftime("%Y-%m-%d")

def hour_key() -> str:
    return now_vn().strftime("%Y-%m-%d %H")

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger("GOLD_BOT")

# ==========================================================
# GLOBALS
# ==========================================================
shutdown_event = threading.Event()
last_activity = {"time": datetime.now(), "type": "startup"}
bot_stats = {
    "start_time": datetime.now(),
    "fetch_success": 0,
    "fetch_fail": 0,
    "telegram_sent": 0,
    "alerts_sent": 0,
    "updates_processed": 0,
}

def update_activity(activity_type: str):
    last_activity["time"] = datetime.now()
    last_activity["type"] = activity_type

# ==========================================================
# HTTP SESSIONS
# ==========================================================
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/json,*/*",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

def make_session(total: int, backoff: float) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(DEFAULT_HEADERS)
    return s

WEB = make_session(total=3, backoff=0.6)
TG_HTTP = make_session(total=3, backoff=0.5)

# ==========================================================
# SOURCES
# ==========================================================
SJC_URL = "https://sjc.com.vn/bieu-do-gia-vang"
DOJI_URL = "https://giavang.doji.vn/"
PNJ_API = "https://edge-api.pnj.io/ecom-frontend/v3/get-gold-price"
PNJ_WEB = "https://www.giavang.pnj.com.vn/"
BTMC_URL = "https://btmc.vn/"

INSTR_BAR = "bar_sjc"
INSTR_RING = "ring_9999"

@dataclass
class PriceQuote:
    source: str
    instrument: str
    buy_luong: int
    sell_luong: int
    updated: str
    url: str

    @property
    def buy_chi(self) -> int:
        return self.buy_luong // 10

    @property
    def sell_chi(self) -> int:
        return self.sell_luong // 10

# ==========================================================
# HELPERS
# ==========================================================
def strip_html(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", html).strip()

def digits_only(s: str) -> str:
    return re.sub(r"[^\d]", "", s or "")

def parse_num_token(s: str) -> Optional[int]:
    token = digits_only(s)
    return int(token) if token else None

def fmt_vnd(n: int) -> str:
    return f"{n:,}".replace(",", ".")

def is_reasonable_luong(n: int) -> bool:
    return 50_000_000 <= n <= 300_000_000

# ==========================================================
# JSON STORAGE
# ==========================================================
_io_lock = threading.Lock()

def load_json(path: str, default: Any) -> Any:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except:
        pass
    return default

def save_json(path: str, data: Any):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"Save error: {e}")

def get_subs() -> Dict:
    return load_json(SUBSCRIBERS_FILE, {"subs": {}})

def set_subs(d: Dict):
    save_json(SUBSCRIBERS_FILE, d)

def subscribe(chat_id: Any):
    with _io_lock:
        d = get_subs()
        d.setdefault("subs", {})[str(chat_id)] = {"enabled": True, "since": fmt_dt()}
        set_subs(d)

def unsubscribe(chat_id: Any):
    with _io_lock:
        d = get_subs()
        if str(chat_id) in d.get("subs", {}):
            d["subs"][str(chat_id)]["enabled"] = False
            set_subs(d)

def list_subscribers() -> List[int]:
    d = get_subs()
    out = []
    for cid_str, meta in d.get("subs", {}).items():
        if meta.get("enabled"):
            try:
                out.append(int(cid_str))
            except:
                pass
    return out

def get_state() -> Dict:
    return load_json(STATE_FILE, {})

def set_state(s: Dict):
    save_json(STATE_FILE, s)

# ==========================================================
# TELEGRAM API
# ==========================================================
TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

def tg_call(method: str, *, params: Optional[Dict] = None, payload: Optional[Dict] = None,
            read_timeout: int = TG_READ_TIMEOUT, max_retries: int = 3) -> Dict:
    url = f"{TG_API}/{method}"
    timeout = (TG_CONNECT_TIMEOUT, read_timeout)
    
    for attempt in range(max_retries):
        try:
            if payload:
                r = TG_HTTP.post(url, json=payload, params=params, timeout=timeout)
            else:
                r = TG_HTTP.get(url, params=params, timeout=timeout)
            update_activity("telegram_api")
            return r.json()
        except requests.Timeout:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        except Exception as e:
            if attempt < max_retries - 1:
                log.warning(f"TG error attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
    
    return {"ok": False, "description": "Max retries"}

def tg_send(chat_id: Any, text: str, reply_markup: Optional[dict] = None) -> bool:
    chunks = [text[i:i+3900] for i in range(0, len(text), 3900)] or [""]
    
    for chunk in chunks:
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        d = tg_call("sendMessage", payload=payload, read_timeout=25)
        if not d.get("ok"):
            return False
        bot_stats["telegram_sent"] += 1
        update_activity("telegram_sent")
    return True

def tg_answer_callback(cq_id: str, text: str = ""):
    tg_call("answerCallbackQuery", payload={"callback_query_id": cq_id, "text": text}, read_timeout=15)

# ==========================================================
# UI
# ==========================================================
def kb_main() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "🟡 Vàng miếng", "callback_data": "BAR"},
                {"text": "💍 Nhẫn 9999", "callback_data": "RING"},
            ],
            [
                {"text": "📋 Cả 2", "callback_data": "ALL"},
                {"text": "🔄 Làm mới", "callback_data": "REFRESH"},
            ],
        ]
    }

# ==========================================================
# FETCHERS
# ==========================================================
def fetch_sjc_bar_hcm() -> Optional[PriceQuote]:
    try:
        r = WEB.get(SJC_URL, timeout=WEB_TIMEOUT)
        txt = strip_html(r.text)
        m = re.search(r"Hồ\s*Chí\s*Minh\s+(\d{1,3}(?:[,.]\d{3})+)\s+.*?\s+(\d{1,3}(?:[,.]\d{3})+)", txt, re.I)
        if not m:
            return None
        buy_k = parse_num_token(m.group(1))
        sell_k = parse_num_token(m.group(2))
        if not (buy_k and sell_k):
            return None
        buy, sell = buy_k * 1000, sell_k * 1000
        if not (is_reasonable_luong(buy) and is_reasonable_luong(sell)):
            return None
        dm = re.search(r"NGÀY\s+(\d{2}/\d{2}/\d{4})", txt, re.I)
        bot_stats["fetch_success"] += 1
        update_activity("fetch")
        return PriceQuote("SJC (HCM)", INSTR_BAR, buy, sell, dm.group(1) if dm else "", SJC_URL)
    except Exception as e:
        log.warning(f"SJC error: {e}")
        bot_stats["fetch_fail"] += 1
        return None

def fetch_doji_hcm() -> List[PriceQuote]:
    out = []
    try:
        r = WEB.get(DOJI_URL, timeout=WEB_TIMEOUT)
        txt = strip_html(r.text)
        um = re.search(r"Cập\s*nh\w*\s*lúc:\s*([0-9]{1,2}:[0-9]{2}\s+\d{2}/\d{2}/\d{4})", txt, re.I)
        updated = um.group(1) if um else ""
        sm = re.search(r"Bảng\s*giá\s*tại\s*Hồ\s*Chí\s*Minh(.*?)(Bảng\s*giá\s*tại|$)", txt, re.I | re.S)
        section = sm.group(1) if sm else txt
        
        def to_luong(t: int) -> int:
            return t * 10000
        
        m_bar = re.search(r"SJC\s*-\s*Bán\s*Lẻ\s+(\d{4,6})\s+(\d{4,6})", section, re.I)
        if m_bar:
            buy_t = parse_num_token(m_bar.group(1))
            sell_t = parse_num_token(m_bar.group(2))
            if buy_t and sell_t:
                buy, sell = to_luong(buy_t), to_luong(sell_t)
                if is_reasonable_luong(buy) and is_reasonable_luong(sell):
                    out.append(PriceQuote("DOJI (HCM)", INSTR_BAR, buy, sell, updated, DOJI_URL))
        
        m_ring = re.search(r"Nhẫn\s*Tròn\s*9999.*?Bán\s*Lẻ\s+(\d{4,6})\s+(\d{4,6})", section, re.I)
        if m_ring:
            buy_t = parse_num_token(m_ring.group(1))
            sell_t = parse_num_token(m_ring.group(2))
            if buy_t and sell_t:
                buy, sell = to_luong(buy_t), to_luong(sell_t)
                if is_reasonable_luong(buy) and is_reasonable_luong(sell):
                    out.append(PriceQuote("DOJI (Nhẫn HTV)", INSTR_RING, buy, sell, updated, DOJI_URL))
        
        if out:
            bot_stats["fetch_success"] += 1
            update_activity("fetch")
    except Exception as e:
        log.warning(f"DOJI error: {e}")
        bot_stats["fetch_fail"] += 1
    return out

def fetch_pnj() -> List[PriceQuote]:
    out = []
    try:
        r = WEB.get(PNJ_API, timeout=WEB_TIMEOUT)
        if r.status_code != 200:
            return out
        data = r.json()
        updated = str(data.get("updated_text") or "").strip()
        locations = data.get("locations") or []
        
        def is_hcm(name: str) -> bool:
            n = (name or "").lower()
            return any(k in n for k in ("tphcm", "tp.hcm", "hồ chí minh", "hcm"))
        
        for loc in locations:
            if is_hcm(str(loc.get("name", ""))):
                for gt in (loc.get("gold_type") or []):
                    if str(gt.get("name", "")).strip().upper() == "SJC":
                        buy_t = parse_num_token(str(gt.get("gia_mua", "")))
                        sell_t = parse_num_token(str(gt.get("gia_ban", "")))
                        if buy_t and sell_t:
                            buy, sell = buy_t * 1000, sell_t * 1000
                            if is_reasonable_luong(buy) and is_reasonable_luong(sell):
                                out.append(PriceQuote("PNJ (HCM)", INSTR_BAR, buy, sell, updated, PNJ_WEB))
                break
        
        for loc in locations:
            if "nữ trang" in str(loc.get("name", "")).lower():
                for gt in (loc.get("gold_type") or []):
                    n = str(gt.get("name", "")).lower()
                    if "nhẫn" in n and ("999.9" in n or "9999" in n):
                        buy_t = parse_num_token(str(gt.get("gia_mua", "")))
                        sell_t = parse_num_token(str(gt.get("gia_ban", "")))
                        if buy_t and sell_t:
                            buy, sell = buy_t * 1000, sell_t * 1000
                            if is_reasonable_luong(buy) and is_reasonable_luong(sell):
                                out.append(PriceQuote("PNJ (Nhẫn 999.9)", INSTR_RING, buy, sell, updated, PNJ_WEB))
                        break
                break
        
        if out:
            bot_stats["fetch_success"] += 1
            update_activity("fetch")
    except Exception as e:
        log.warning(f"PNJ error: {e}")
        bot_stats["fetch_fail"] += 1
    return out

def fetch_btmc() -> List[PriceQuote]:
    out = []
    try:
        r = WEB.get(BTMC_URL, timeout=WEB_TIMEOUT)
        txt = strip_html(r.text)
        um = re.search(r"Cập\s*nhật\s*lúc\s+(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", txt, re.I)
        updated = um.group(1) if um else ""
        
        def to_luong(t: int) -> int:
            return t * 10000
        
        m_bar = re.search(r"VÀNG\s*MIẾNG\s*SJC.*?\(24k\)\s+(\d{4,6})\s+(\d{4,6})", txt, re.I | re.S)
        if m_bar:
            buy_t = parse_num_token(m_bar.group(1))
            sell_t = parse_num_token(m_bar.group(2))
            if buy_t and sell_t:
                buy, sell = to_luong(buy_t), to_luong(sell_t)
                if is_reasonable_luong(buy) and is_reasonable_luong(sell):
                    out.append(PriceQuote("BTMC", INSTR_BAR, buy, sell, updated, BTMC_URL))
        
        m_ring = re.search(r"NHẪN\s*TRÒN\s*TRƠN.*?999\.9.*?\(24k\)\s+(\d{4,6})\s+(\d{4,6})", txt, re.I | re.S)
        if m_ring:
            buy_t = parse_num_token(m_ring.group(1))
            sell_t = parse_num_token(m_ring.group(2))
            if buy_t and sell_t:
                buy, sell = to_luong(buy_t), to_luong(sell_t)
                if is_reasonable_luong(buy) and is_reasonable_luong(sell):
                    out.append(PriceQuote("BTMC (Nhẫn 999.9)", INSTR_RING, buy, sell, updated, BTMC_URL))
        
        if out:
            bot_stats["fetch_success"] += 1
            update_activity("fetch")
    except Exception as e:
        log.warning(f"BTMC error: {e}")
        bot_stats["fetch_fail"] += 1
    return out

# ==========================================================
# CACHE + COLLECT
# ==========================================================
CACHE = {"ts": 0.0, "quotes": None}
FETCH_LOCK = threading.Lock()

def collect_all_quotes() -> Dict[str, List[PriceQuote]]:
    ts = time.time()
    if CACHE["quotes"] and (ts - CACHE["ts"] < CACHE_TTL):
        return CACHE["quotes"]
    
    with FETCH_LOCK:
        bar, ring = [], []
        
        q = fetch_sjc_bar_hcm()
        if q:
            bar.append(q)
        
        for q in fetch_doji_hcm():
            (bar if q.instrument == INSTR_BAR else ring).append(q)
        
        for q in fetch_pnj():
            (bar if q.instrument == INSTR_BAR else ring).append(q)
        
        for q in fetch_btmc():
            (bar if q.instrument == INSTR_BAR else ring).append(q)
        
        out = {INSTR_BAR: bar, INSTR_RING: ring}
        CACHE["ts"] = ts
        CACHE["quotes"] = out
        log.info(f"✅ Collected {len(bar)} bar + {len(ring)} ring")
        return out

# ==========================================================
# PICK PRIMARY
# ==========================================================
PREF_BAR = ["SJC (HCM)", "DOJI (HCM)", "PNJ (HCM)", "BTMC"]
PREF_RING = ["DOJI (Nhẫn HTV)", "PNJ (Nhẫn 999.9)", "BTMC (Nhẫn 999.9)"]

def pick_primary(quotes: List[PriceQuote], pref: List[str]) -> Optional[PriceQuote]:
    if not quotes:
        return None
    by = {q.source: q for q in quotes}
    for name in pref:
        if name in by:
            return by[name]
    return quotes[0]

# ==========================================================
# MESSAGES
# ==========================================================
ORDER = ["SJC (HCM)", "DOJI (HCM)", "PNJ (HCM)", "BTMC", "DOJI (Nhẫn HTV)", "PNJ (Nhẫn 999.9)", "BTMC (Nhẫn 999.9)"]

def sort_quotes(quotes: List[PriceQuote]) -> List[PriceQuote]:
    idx = {name: i for i, name in enumerate(ORDER)}
    return sorted(quotes, key=lambda q: idx.get(q.source, 999))

def render_source_block(q: PriceQuote) -> str:
    updated = f"  <i>({q.updated})</i>" if q.updated else ""
    return (
        f"🏷️ <b>{q.source.upper()}</b>{updated}\n"
        f"• <b>CHỈ</b>\n"
        f"  💰 <b>MUA</b>: <code>{fmt_vnd(q.buy_chi)}</code> đ/chỉ\n"
        f"  💵 <b>BÁN</b>: <code>{fmt_vnd(q.sell_chi)}</code> đ/chỉ\n"
        f"• <b>LƯỢNG</b>\n"
        f"  💰 <b>MUA</b>: <code>{fmt_vnd(q.buy_luong)}</code> đ/lượng\n"
        f"  💵 <b>BÁN</b>: <code>{fmt_vnd(q.sell_luong)}</code> đ/lượng\n"
        f"🟦 <a href='{q.url}'>Nguồn</a>"
    )

def build_instrument_message(title: str, icon: str, quotes: List[PriceQuote]) -> str:
    if not quotes:
        return f"⚠️ <b>KHÔNG LẤY ĐƯỢC {title}</b>\nBấm <b>🔄 Làm mới</b>."
    quotes = sort_quotes(quotes)
    msg = f"{icon} <b>{title}</b>\n🕒 <i>{fmt_dt()}</i>\n━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += "\n\n──────────────\n\n".join(render_source_block(q) for q in quotes)
    return msg

def msg_bar(quotes: List[PriceQuote]) -> str:
    return build_instrument_message("VÀNG MIẾNG SJC", "🟡", quotes)

def msg_ring(quotes: List[PriceQuote]) -> str:
    return build_instrument_message("VÀNG NHẪN 9999", "💍", quotes)

def msg_all(allq: Dict) -> str:
    return msg_bar(allq.get(INSTR_BAR, [])) + "\n\n════════════════════\n\n" + msg_ring(allq.get(INSTR_RING, []))

def msg_hourly_compact(allq: Dict) -> str:
    bar_q = pick_primary(allq.get(INSTR_BAR, []), PREF_BAR)
    ring_q = pick_primary(allq.get(INSTR_RING, []), PREF_RING)
    
    msg = f"🕐 <b>CẬP NHẬT GIÁ VÀNG</b>\n<i>{fmt_dt()}</i>\n━━━━━━━━━━━━━━━━━━━━\n"
    
    if bar_q:
        msg += f"\n🟡 <b>VÀNG MIẾNG</b> • <b>{bar_q.source}</b>\n"
        msg += f"CHỈ:  💰 <b>MUA</b> <code>{fmt_vnd(bar_q.buy_chi)}</code>  |  💵 <b>BÁN</b> <code>{fmt_vnd(bar_q.sell_chi)}</code>\n"
        msg += f"LƯỢNG: 💰 <b>MUA</b> <code>{fmt_vnd(bar_q.buy_luong)}</code> |  💵 <b>BÁN</b> <code>{fmt_vnd(bar_q.sell_luong)}</code>\n"
        msg += f"🟦 <a href='{bar_q.url}'>Nguồn</a>\n"
    
    if ring_q:
        msg += f"\n💍 <b>NHẪN 9999</b> • <b>{ring_q.source}</b>\n"
        msg += f"CHỈ:  💰 <b>MUA</b> <code>{fmt_vnd(ring_q.buy_chi)}</code>  |  💵 <b>BÁN</b> <code>{fmt_vnd(ring_q.sell_chi)}</code>\n"
        msg += f"LƯỢNG: 💰 <b>MUA</b> <code>{fmt_vnd(ring_q.buy_luong)}</code> |  💵 <b>BÁN</b> <code>{fmt_vnd(ring_q.sell_luong)}</code>\n"
        msg += f"🟦 <a href='{ring_q.url}'>Nguồn</a>\n"
    
    if not bar_q and not ring_q:
        msg += "\n⚠️ <b>Không lấy được dữ liệu</b>"
    
    return msg

def msg_heartbeat() -> str:
    uptime = (datetime.now() - bot_stats["start_time"]).total_seconds() / 3600
    subs = len(list_subscribers())
    return (
        f"💓 <b>HEARTBEAT</b>\n"
        f"🕒 <i>{fmt_dt()}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ Uptime: <b>{uptime:.1f}</b> giờ\n"
        f"👥 Subscribers: <b>{subs}</b>\n"
        f"📊 Fetch: ✅{bot_stats['fetch_success']} ❌{bot_stats['fetch_fail']}\n"
        f"📤 Telegram: ✅{bot_stats['telegram_sent']}\n"
        f"🚨 Alerts: {bot_stats['alerts_sent']}\n"
        f"📥 Updates: {bot_stats['updates_processed']}"
    )

# ==========================================================
# DAILY STATS
# ==========================================================
def _ensure_day(state: Dict) -> Dict:
    d = today_key()
    if state.get("day", {}).get("date") != d:
        state["day"] = {
            "date": d,
            "bar": {},
            "ring": {},
            "sent_open": False,
            "sent_close": False,
        }
    return state

def update_day_stats(state: Dict, instr: str, q: PriceQuote) -> Dict:
    state = _ensure_day(state)
    box = state["day"].get("bar" if instr == INSTR_BAR else "ring", {})
    sell = q.sell_luong
    if "high_sell" not in box or sell > box["high_sell"]:
        box["high_sell"] = sell
    if "low_sell" not in box or sell < box["low_sell"]:
        box["low_sell"] = sell
    box["close_buy"] = q.buy_luong
    box["close_sell"] = q.sell_luong
    box["close_at"] = fmt_dt()
    state["day"]["bar" if instr == INSTR_BAR else "ring"] = box
    return state

def set_day_open(state: Dict, instr: str, q: PriceQuote) -> Dict:
    state = _ensure_day(state)
    box = state["day"].get
