import time
import logging
import os
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Optional, List, Dict, Tuple
from threading import Thread, Event
import signal
import sys

import requests
from flask import Flask

# ==========================================================
# CONFIG
# ==========================================================
TELEGRAM_BOT_TOKEN = "8285842393:AAHIADmQQ0vYMmIOZp8lD-kdEID0bfDKIxU"
TELEGRAM_CHAT_ID = "@roamliquidity"

# SOLANA (ROAM)
WALLET_SOL = "DSjPt6AtYu7NvKvVzxPkL2BMxrA3M4zK9jQaN1yunktg"
CONTRACT_ROAM_SOL = "RoamA1USA8xjvpTJZ6RvvxyDRzNh6GCA1zVGKSiMVkn"
RPC_SOL_LIST = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-api.projectserum.com",
    "https://rpc.ankr.com/solana",
]

# BSC (ROAM)
WALLET_BSC = "0x3fefe29dA25BEa166fB5f6ADe7b5976D2b0e586B"
CONTRACT_ROAM_BSC = "0x3fefe29dA25BEa166fB5f6ADe7b5976D2b0e586B"
ROAM_BSC_DECIMALS = 6
RPC_BSC_LIST = [
    "https://bsc-dataseed.binance.org/",
    "https://bsc-dataseed1.defibit.io/",
    "https://bsc-dataseed1.ninicoin.io/",
]

# Runtime
POLL_INTERVAL_SEC = 15
SOL_POLL_INTERVAL_SEC = 30
ALERT_THRESHOLD = Decimal("1")
SEND_STARTUP_MESSAGE = True
HEARTBEAT_INTERVAL_SEC = 300

PORT = int(os.environ.get("PORT", 10000))

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ROAM_WATCHDOG")

# ==========================================================
# GLOBALS
# ==========================================================
shutdown_event = Event()
last_activity = {"time": datetime.now(), "type": "startup"}

# ==========================================================
# HELPERS
# ==========================================================
SEP = "──────────────"

def now_str() -> str:
    """Trả về thời gian hiện tại"""
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def to_decimal(x) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None

def fmt_int_trunc(x: Decimal) -> str:
    n = x.quantize(Decimal("1"), rounding=ROUND_DOWN)
    return f"{n:,}"

def keccak_topic_transfer() -> str:
    return "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def pad_address_topic(addr: str) -> str:
    a = addr.lower().replace("0x", "")
    return "0x" + ("0" * 48) + a

def update_activity(activity_type: str):
    last_activity["time"] = datetime.now()
    last_activity["type"] = activity_type

# ==========================================================
# TELEGRAM
# ==========================================================
class TelegramClient:
    def __init__(self, session: requests.Session):
        self.session = session
        self.url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        self.send_count = 0

    def send_html(self, message: str, retry: int = 3) -> bool:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        
        for attempt in range(retry):
            try:
                r = self.session.post(self.url, json=payload, timeout=15)
                if r.status_code == 200:
                    self.send_count += 1
                    log.info("✅ Telegram sent (#%d)", self.send_count)
                    update_activity("telegram_sent")
                    return True
                else:
                    log.error("❌ Telegram error %s: %s", r.status_code, r.text[:300])
                    if attempt < retry - 1:
                        time.sleep(2 ** attempt)
            except Exception as e:
                log.error("❌ Telegram error (attempt %d/%d): %s", attempt + 1, retry, e)
                if attempt < retry - 1:
                    time.sleep(2 ** attempt)
        return False

# ==========================================================
# SOLANA
# ==========================================================
class SolanaReader:
    def __init__(self, session: requests.Session):
        self.session = session
        self.current_rpc_idx = 0
        self.fail_count = 0
        self.success_count = 0
        self.last_success = None

    def _get_current_rpc(self) -> str:
        return RPC_SOL_LIST[self.current_rpc_idx]

    def _switch_rpc(self):
        self.current_rpc_idx = (self.current_rpc_idx + 1) % len(RPC_SOL_LIST)
        log.info("🔄 SOL RPC switched")

    def _rpc_call(self, payload: dict, timeout: int = 20):
        max_attempts = len(RPC_SOL_LIST) * 2
        
        for attempt in range(max_attempts):
            try:
                rpc = self._get_current_rpc()
                r = self.session.post(rpc, headers={"Content-Type": "application/json"}, 
                                    json=payload, timeout=timeout)
                
                if r.status_code == 429:
                    log.warning("⚠️ SOL rate limited")
                    self._switch_rpc()
                    time.sleep(3)
                    continue
                
                if r.status_code >= 500:
                    log.warning("⚠️ SOL server error %d", r.status_code)
                    self._switch_rpc()
                    time.sleep(2)
                    continue
                    
                r.raise_for_status()
                self.fail_count = 0
                self.success_count += 1
                self.last_success = datetime.now()
                update_activity("sol_rpc")
                return r.json()
                
            except requests.Timeout:
                self._switch_rpc()
                time.sleep(2)
            except Exception:
                if attempt < max_attempts - 1:
                    self._switch_rpc()
                    time.sleep(2)
                
        self.fail_count += 1
        return None

    def get_roam_balance(self) -> Optional[Decimal]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                WALLET_SOL,
                {"mint": CONTRACT_ROAM_SOL},
                {"encoding": "jsonParsed"},
            ],
        }
        
        data = self._rpc_call(payload)
        if data is None:
            return None

        try:
            value = (data.get("result") or {}).get("value") or []
            total = Decimal("0")
            for item in value:
                ui_amount = (item.get("account", {}).get("data", {})
                           .get("parsed", {}).get("info", {})
                           .get("tokenAmount", {}).get("uiAmount"))
                d = to_decimal(ui_amount)
                if d is not None:
                    total += d
            return total
        except Exception as e:
            log.warning("Parse SOL balance error: %s", e)
            return None

    def get_latest_tx_signature(self) -> Optional[str]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [WALLET_SOL, {"limit": 1}],
        }
        
        data = self._rpc_call(payload)
        if data is None:
            return None

        try:
            res = data.get("result") or []
            return res[0].get("signature") if res else None
        except Exception:
            return None

    def get_health_status(self) -> dict:
        return {
            "success": self.success_count,
            "fail": self.fail_count,
            "last_success": self.last_success.strftime("%H:%M:%S") if self.last_success else "Never"
        }

# ==========================================================
# BSC
# ==========================================================
class BscReader:
    def __init__(self, session: requests.Session):
        self.session = session
        self.current_rpc_idx = 0
        self.fail_count = 0
        self.success_count = 0
        self.last_success = None

    def _get_current_rpc(self) -> str:
        return RPC_BSC_LIST[self.current_rpc_idx]

    def _switch_rpc(self):
        self.current_rpc_idx = (self.current_rpc_idx + 1) % len(RPC_BSC_LIST)
        log.info("🔄 BSC RPC switched")

    def rpc(self, method: str, params, timeout: int = 15):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        max_attempts = len(RPC_BSC_LIST) * 2
        
        for attempt in range(max_attempts):
            try:
                rpc = self._get_current_rpc()
                r = self.session.post(rpc, json=payload, timeout=timeout)
                
                if r.status_code >= 500:
                    self._switch_rpc()
                    time.sleep(2)
                    continue
                
                r.raise_for_status()
                self.fail_count = 0
                self.success_count += 1
                self.last_success = datetime.now()
                update_activity("bsc_rpc")
                return r.json()
                
            except requests.Timeout:
                self._switch_rpc()
                time.sleep(2)
            except Exception:
                if attempt < max_attempts - 1:
                    self._switch_rpc()
                    time.sleep(2)
        
        self.fail_count += 1
        return None

    def get_latest_block(self) -> Optional[int]:
        data = self.rpc("eth_blockNumber", [])
        if data is None:
            return None
        try:
            return int(data["result"], 16)
        except Exception:
            return None

    def get_roam_balance(self) -> Optional[Decimal]:
        try:
            wallet_padded = WALLET_BSC[2:].lower().zfill(64)
            data_param = "0x70a08231" + wallet_padded
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": CONTRACT_ROAM_BSC, "data": data_param}, "latest"],
            }
            rpc = self._get_current_rpc()
            r = self.session.post(rpc, json=payload, timeout=15)
            r.raise_for_status()
            data = r.json()
            result = data.get("result")
            if not result or result == "0x":
                return Decimal("0")
            raw = int(result, 16)
            return Decimal(raw) / (Decimal(10) ** ROAM_BSC_DECIMALS)
        except Exception:
            return None

    def get_health_status(self) -> dict:
        return {
            "success": self.success_count,
            "fail": self.fail_count,
            "last_success": self.last_success.strftime("%H:%M:%S") if self.last_success else "Never"
        }

class BscTransferWatcher:
    def __init__(self, bsc: BscReader):
        self.bsc = bsc
        self.last_block: Optional[int] = None
        self.topic0 = keccak_topic_transfer()
        self.wallet_topic = pad_address_topic(WALLET_BSC)

    def _get_logs(self, from_block: int, to_block: int, direction: str) -> List[dict]:
        topics = [self.topic0, None, self.wallet_topic] if direction == "IN" else [self.topic0, self.wallet_topic, None]
        params = [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": CONTRACT_ROAM_BSC,
            "topics": topics,
        }]
        data = self.bsc.rpc("eth_getLogs", params)
        return data.get("result") or [] if data else []

    def poll(self) -> List[Dict]:
        latest = self.bsc.get_latest_block()
        if latest is None:
            return []

        if self.last_block is None:
            self.last_block = latest
            log.info("📍 BSC starting from block: %d", latest)
            return []

        if latest <= self.last_block:
            return []

        from_block = self.last_block + 1
        to_block = latest

        try:
            logs_in = self._get_logs(from_block, to_block, "IN")
            logs_out = self._get_logs(from_block, to_block, "OUT")
            if logs_in or logs_out:
                log.info("📥 BSC: %d IN, %d OUT", len(logs_in), len(logs_out))
        except Exception:
            logs_in, logs_out = [], []

        self.last_block = latest
        update_activity("bsc_poll")

        seen = set()
        parsed = []

        def parse_logs(logs: List[dict], direction: str):
            for lg in logs:
                txh = lg.get("transactionHash")
                log_index = int(lg.get("logIndex", "0x0"), 16)
                if not txh:
                    continue
                key = (txh, log_index)
                if key in seen:
                    continue
                seen.add(key)
                raw = int(lg.get("data", "0x0"), 16)
                amt = Decimal(raw) / (Decimal(10) ** ROAM_BSC_DECIMALS)
                parsed.append({
                    "direction": direction,
                    "amount": amt,
                    "tx": txh,
                    "block": int(lg.get("blockNumber", "0x0"), 16),
                    "logIndex": log_index,
                })

        parse_logs(logs_in, "IN")
        parse_logs(logs_out, "OUT")
        parsed.sort(key=lambda x: (x["block"], x["logIndex"]))
        return parsed

# ==========================================================
# MESSAGES
# ==========================================================
def msg_startup(sol_bal: Decimal, bsc_bal: Decimal) -> str:
    return (
        "🚀 <b>ROAM WATCHDOG STARTED</b>\n"
        f"{SEP}\n"
        f"<b>SOL</b>: <b>{fmt_int_trunc(sol_bal)}</b> ROAM\n"
        f"<b>BSC</b>: <b>{fmt_int_trunc(bsc_bal)}</b> ROAM\n"
        f"{SEP}\n"
        f"🕐 <code>{now_str()}</code>"
    )

def msg_sol_change(delta: Decimal, new_bal: Decimal, tx_sig: Optional[str]) -> str:
    is_in = delta > 0
    t = "NẠP" if is_in else "RÚT"
    sign = "+" if is_in else "-"
    amt = delta.copy_abs()
    tx_line = f"\n🔗 <a href='https://solscan.io/tx/{tx_sig}'>View TX</a>" if tx_sig else ""
    return (
        "🔔 <b>ROAM ALERT</b>\n"
        "<b>Network</b>: SOL\n"
        f"{SEP}\n"
        f"<b>Type</b>: {t}\n"
        f"<b>Amount</b>: <b>{sign}{fmt_int_trunc(amt)}</b> ROAM\n"
        f"<b>Balance</b>: <b>{fmt_int_trunc(new_bal)}</b> ROAM\n"
        f"{SEP}\n"
        f"🕐 <code>{now_str()}</code>"
        f"{tx_line}"
    )

def msg_bsc_transfer(direction: str, amount: Decimal, new_bal: Decimal, tx_hash: str) -> str:
    is_in = (direction == "IN")
    t = "NẠP" if is_in else "RÚT"
    sign = "+" if is_in else "-"
    return (
        "🔔 <b>ROAM ALERT</b>\n"
        "<b>Network</b>: BSC\n"
        f"{SEP}\n"
        f"<b>Type</b>: {t}\n"
        f"<b>Amount</b>: <b>{sign}{fmt_int_trunc(amount)}</b> ROAM\n"
        f"<b>Balance</b>: <b>{fmt_int_trunc(new_bal)}</b> ROAM\n"
        f"{SEP}\n"
        f"🕐 <code>{now_str()}</code>\n"
        f"🔗 <a href='https://bscscan.com/tx/{tx_hash}'>View TX</a>"
    )

# ==========================================================
# FLASK APP
# ==========================================================
app = Flask(__name__)

@app.route('/')
def health_check():
    uptime = (datetime.now() - last_activity["time"]).total_seconds()
    return {
        "status": "online",
        "service": "ROAM Watchdog",
        "time": now_str(),
        "last_activity": last_activity["type"],
        "seconds_since_activity": int(uptime),
        "healthy": uptime < 300
    }

@app.route('/health')
def health():
    uptime = (datetime.now() - last_activity["time"]).total_seconds()
    if uptime > 300:
        return {"status": "degraded"}, 503
    return {"status": "healthy", "uptime": int(uptime)}

@app.route('/ping')
def ping():
    update_activity("ping")
    return {"pong": now_str()}

# ==========================================================
# WATCHDOG THREAD
# ==========================================================
def run_watchdog():
    start_time = datetime.now()
    
    try:
        session = requests.Session()
        session.headers.update({'User-Agent': 'ROAM-Watchdog/2.0'})
        
        tele = TelegramClient(session)
        sol = SolanaReader(session)
        bsc = BscReader(session)
        bsc_watch = BscTransferWatcher(bsc)

        log.info("🔄 Fetching initial data...")
        
        last_sol = Decimal("0")
        last_bsc = Decimal("0")
        
        for attempt in range(5):
            temp_sol = sol.get_roam_balance()
            temp_bsc = bsc.get_roam_balance()
            if temp_sol is not None and temp_bsc is not None:
                last_sol = temp_sol
                last_bsc = temp_bsc
                break
            log.warning("⚠️ Initial fetch retry %d/5", attempt + 1)
            time.sleep(5)

        log.info("✅ Initial | SOL=%s | BSC=%s", fmt_int_trunc(last_sol), fmt_int_trunc(last_bsc))

        if SEND_STARTUP_MESSAGE:
            tele.send_html(msg_startup(last_sol, last_bsc))

        log.info("🛡️ Watchdog active")

        last_sol_check = time.time()
        last_heartbeat = time.time()
        check_count = 0

        while not shutdown_event.is_set():
            try:
                current_time = time.time()
                check_count += 1
                
                # SOL
                if current_time - last_sol_check >= SOL_POLL_INTERVAL_SEC:
                    log.info("🔍 [%d] Checking SOL...", check_count)
                    curr_sol = sol.get_roam_balance()
                    
                    if curr_sol is not None:
                        delta = curr_sol - last_sol
                        if delta != 0:
                            log.info("📊 SOL: %s -> %s (Δ%s)", last_sol, curr_sol, delta)
                        
                        if delta.copy_abs() >= ALERT_THRESHOLD:
                            log.info("🚨 SOL ALERT! Delta: %s", delta)
                            sig = sol.get_latest_tx_signature()
                            if tele.send_html(msg_sol_change(delta, curr_sol, sig)):
                                last_sol = curr_sol
                        else:
                            last_sol = curr_sol
                    
                    last_sol_check = current_time

                # BSC
                log.info("🔍 [%d] Checking BSC...", check_count)
                transfers = bsc_watch.poll()
                
                if transfers:
                    log.info("🚨 BSC: %d transfers", len(transfers))
                    curr_bsc = bsc.get_roam_balance() or last_bsc
                    
                    for t in transfers:
                        amt = t["amount"].copy_abs()
                        if amt < ALERT_THRESHOLD:
                            continue
                        if tele.send_html(msg_bsc_transfer(t["direction"], amt, curr_bsc, t["tx"])):
                            last_bsc = curr_bsc

                # Heartbeat (chỉ log, không gửi telegram)
                if current_time - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                    uptime_hours = (datetime.now() - start_time).total_seconds() / 3600
                    sol_health = sol.get_health_status()
                    bsc_health = bsc.get_health_status()
                    log.info("💓 Heartbeat | Uptime: %.1fh | SOL: ✅%d ❌%d | BSC: ✅%d ❌%d", 
                           uptime_hours, sol_health['success'], sol_health['fail'],
                           bsc_health['success'], bsc_health['fail'])
                    last_heartbeat = current_time

                time.sleep(POLL_INTERVAL_SEC)

            except KeyboardInterrupt:
                log.info("⛔ Stopped by user")
                break
            except Exception as e:
                log.exception("❌ Watchdog error: %s", e)
                time.sleep(POLL_INTERVAL_SEC)

    except Exception as e:
        log.exception("❌ Fatal error: %s", e)
    finally:
        log.info("👋 Watchdog stopped")

# ==========================================================
# SIGNAL HANDLING
# ==========================================================
def signal_handler(sig, frame):
    log.info("🛑 Signal %d received", sig)
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ==========================================================
# MAIN
# ==========================================================
def main():
    log.info("=" * 60)
    log.info("🚀 ROAM WATCHDOG v2.0")
    log.info("=" * 60)
    
    # Start watchdog thread
    watchdog_thread = Thread(target=run_watchdog, daemon=True, name="WatchdogThread")
    watchdog_thread.start()
    log.info("✅ Watchdog thread started")
    
    # Start Flask (blocking - keeps process alive)
    log.info("🌐 Starting Flask on 0.0.0.0:%d", PORT)
    try:
        app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        log.info("⛔ Flask stopped")
    except Exception as e:
        log.exception("❌ Flask error: %s", e)
    finally:
        shutdown_event.set()
        log.info("👋 Shutting down...")

if __name__ == "__main__":
    main()
