import time
import logging
import os
from datetime import datetime, timedelta
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
POLL_INTERVAL_SEC = 15  # Check BSC mỗi 15s
SOL_POLL_INTERVAL_SEC = 30  # Check SOL mỗi 30s
ALERT_THRESHOLD = Decimal("1")
SEND_STARTUP_MESSAGE = True

# Health check & keep-alive
HEARTBEAT_INTERVAL_SEC = 300  # Gửi heartbeat mỗi 5 phút
HEALTH_CHECK_INTERVAL_SEC = 60  # Self-check mỗi 1 phút

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
    """Cập nhật thời gian hoạt động cuối"""
    last_activity["time"] = datetime.now()
    last_activity["type"] = activity_type


# ==========================================================
# TELEGRAM
# ==========================================================
class TelegramClient:
    def __init__(self, session: requests.Session):
        self.session = session
        self.url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        self.last_send_time = None
        self.send_count = 0

    def send_html(self, message: str, retry: int = 3) -> bool:
        """Gửi tin nhắn với retry"""
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
                    self.last_send_time = datetime.now()
                    self.send_count += 1
                    log.info("✅ Telegram sent (#%d)", self.send_count)
                    update_activity("telegram_sent")
                    return True
                else:
                    log.error("❌ Telegram error %s: %s", r.status_code, r.text[:300])
                    if attempt < retry - 1:
                        time.sleep(2 ** attempt)
            except requests.RequestException as e:
                log.error("❌ Telegram network error (attempt %d/%d): %s", attempt + 1, retry, e)
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
        old_rpc = self._get_current_rpc()
        self.current_rpc_idx = (self.current_rpc_idx + 1) % len(RPC_SOL_LIST)
        new_rpc = self._get_current_rpc()
        log.info("🔄 SOL RPC: %s -> %s", old_rpc.split('/')[2], new_rpc.split('/')[2])

    def _rpc_call(self, payload: dict, timeout: int = 20):
        max_attempts = len(RPC_SOL_LIST) * 2  # Thử tất cả RPC 2 lần
        
        for attempt in range(max_attempts):
            try:
                rpc = self._get_current_rpc()
                r = self.session.post(
                    rpc,
                    headers={"Content-Type": "application/json"},
                    json=payload,
                    timeout=timeout,
                )
                
                if r.status_code == 429:
                    log.warning("⚠️ SOL rate limited, switching...")
                    self._switch_rpc()
                    time.sleep(3)
                    continue
                
                if r.status_code >= 500:
                    log.warning("⚠️ SOL server error %d, switching...", r.status_code)
                    self._switch_rpc()
                    time.sleep(2)
                    continue
                    
                r.raise_for_status()
                self.fail_count = 0
                self.success_count += 1
                self.last_success = datetime.now()
                update_activity("sol_rpc_success")
                return r.json()
                
            except requests.Timeout:
                log.warning("⏱️ SOL timeout (attempt %d/%d)", attempt + 1, max_attempts)
                self._switch_rpc()
                time.sleep(2)
            except requests.RequestException as e:
                log.warning("⚠️ SOL error (attempt %d/%d): %s", attempt + 1, max_attempts, str(e)[:100])
                if attempt < max_attempts - 1:
                    self._switch_rpc()
                    time.sleep(2)
                
        self.fail_count += 1
        log.error("❌ All SOL RPCs failed (fail_count: %d)", self.fail_count)
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
                ui_amount = (
                    item.get("account", {})
                    .get("data", {})
                    .get("parsed", {})
                    .get("info", {})
                    .get("tokenAmount", {})
                    .get("uiAmount")
                )
                d = to_decimal(ui_amount)
                if d is not None:
                    total += d

            return total
        except (ValueError, KeyError) as e:
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
            if not res:
                return None
            return res[0].get("signature")
        except (ValueError, KeyError) as e:
            log.warning("SOL getSignatures error: %s", e)
            return None

    def get_health_status(self) -> dict:
        return {
            "current_rpc": self._get_current_rpc().split('/')[2],
            "success_count": self.success_count,
            "fail_count": self.fail_count,
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
        old_rpc = self._get_current_rpc()
        self.current_rpc_idx = (self.current_rpc_idx + 1) % len(RPC_BSC_LIST)
        new_rpc = self._get_current_rpc()
        log.info("🔄 BSC RPC: %s -> %s", old_rpc.split('/')[2], new_rpc.split('/')[2])

    def rpc(self, method: str, params, timeout: int = 15):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        max_attempts = len(RPC_BSC_LIST) * 2
        
        for attempt in range(max_attempts):
            try:
                rpc = self._get_current_rpc()
                r = self.session.post(rpc, json=payload, timeout=timeout)
                
                if r.status_code >= 500:
                    log.warning("⚠️ BSC server error %d, switching...", r.status_code)
                    self._switch_rpc()
                    time.sleep(2)
                    continue
                
                r.raise_for_status()
                self.fail_count = 0
                self.success_count += 1
                self.last_success = datetime.now()
                update_activity("bsc_rpc_success")
                return r.json()
                
            except requests.Timeout:
                log.warning("⏱️ BSC timeout (attempt %d/%d)", attempt + 1, max_attempts)
                self._switch_rpc()
                time.sleep(2)
            except requests.RequestException as e:
                log.warning("⚠️ BSC error (attempt %d/%d): %s", attempt + 1, max_attempts, str(e)[:100])
                if attempt < max_attempts - 1:
                    self._switch_rpc()
                    time.sleep(2)
        
        self.fail_count += 1
        log.error("❌ All BSC RPCs failed (fail_count: %d)", self.fail_count)
        return None

    def get_latest_block(self) -> Optional[int]:
        data = self.rpc("eth_blockNumber", [])
        if data is None:
            return None
        try:
            return int(data["result"], 16)
        except Exception as e:
            log.warning("BSC blockNumber error: %s", e)
            return None

    def get_roam_balance(self) -> Optional[Decimal]:
        try:
            if not (WALLET_BSC.startswith("0x") and len(WALLET_BSC) == 42):
                log.error("Invalid WALLET_BSC: %s", WALLET_BSC)
                return None

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

        except Exception as e:
            log.warning("BSC balance error: %s", e)
            return None

    def get_health_status(self) -> dict:
        return {
            "current_rpc": self._get_current_rpc().split('/')[2],
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "last_success": self.last_success.strftime("%H:%M:%S") if self.last_success else "Never"
        }


class BscTransferWatcher:
    def __init__(self, bsc: BscReader):
        self.bsc = bsc
        self.last_block: Optional[int] = None
        self.topic0 = keccak_topic_transfer()
        self.wallet_topic = pad_address_topic(WALLET_BSC)
        self.transfers_detected = 0

    def _get_logs(self, from_block: int, to_block: int, direction: str) -> List[dict]:
        if direction == "IN":
            topics = [self.topic0, None, self.wallet_topic]
        else:
            topics = [self.topic0, self.wallet_topic, None]

        params = [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": CONTRACT_ROAM_BSC,
            "topics": topics,
        }]

        data = self.bsc.rpc("eth_getLogs", params)
        if data is None:
            return []
        return data.get("result") or []

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

        logs_in: List[dict] = []
        logs_out: List[dict] = []
        
        try:
            logs_in = self._get_logs(from_block, to_block, "IN")
            logs_out = self._get_logs(from_block, to_block, "OUT")
            
            if logs_in or logs_out:
                log.info("📥 BSC blocks %d-%d: %d IN, %d OUT", from_block, to_block, len(logs_in), len(logs_out))
        except Exception as e:
            log.warning("BSC getLogs error: %s", e)

        self.last_block = latest
        update_activity("bsc_poll")

        seen: set[Tuple[str, int]] = set()
        parsed: List[Dict] = []

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
        
        if parsed:
            self.transfers_detected += len(parsed)
        
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
        f"🕐 <code>{now_str()}</code>\n"
        f"⏱️ Poll: SOL={SOL_POLL_INTERVAL_SEC}s, BSC={POLL_INTERVAL_SEC}s"
    )


def msg_heartbeat(sol_health: dict, bsc_health: dict, uptime_hours: float) -> str:
    return (
        "💓 <b>HEARTBEAT</b>\n"
        f"{SEP}\n"
        f"⏱️ Uptime: <b>{uptime_hours:.1f}</b> hours\n"
        f"📊 SOL: ✅{sol_health['success_count']} ❌{sol_health['fail_count']}\n"
        f"📊 BSC: ✅{bsc_health['success_count']} ❌{bsc_health['fail_count']}\n"
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
        "healthy": uptime < 300  # Healthy nếu có activity trong 5 phút
    }

@app.route('/health')
def health():
    uptime = (datetime.now() - last_activity["time"]).total_seconds()
    if uptime > 300:
        return {"status": "degraded", "reason": "No activity > 5min"}, 503
    return {"status": "healthy", "uptime_seconds": int(uptime)}

@app.route('/ping')
def ping():
    update_activity("ping")
    return {"pong": now_str()}


# ==========================================================
# WATCHDOG THREAD
# ==========================================================
def run_watchdog():
    start_time = datetime.now()
    
    with requests.Session() as session:
        session.headers.update({'User-Agent': 'ROAM-Watchdog/1.0'})
        
        tele = TelegramClient(session)
        sol = SolanaReader(session)
        bsc = BscReader(session)
        bsc_watch = BscTransferWatcher(bsc)

        log.info("🔄 Fetching initial data...")
        
        # Retry initial fetch
        last_sol = None
        last_bsc = None
        for attempt in range(5):
            last_sol = sol.get_roam_balance()
            last_bsc = bsc.get_roam_balance()
            if last_sol is not None and last_bsc is not None:
                break
            log.warning("⚠️ Initial fetch failed, retry %d/5...", attempt + 1)
            time.sleep(5)
        
        if last_sol is None:
            last_sol = Decimal("0")
        if last_bsc is None:
            last_bsc = Decimal("0")

        log.info("✅ Initial | SOL=%s | BSC=%s", fmt_int_trunc(last_sol), fmt_int_trunc(last_bsc))

        if SEND_STARTUP_MESSAGE:
            tele.send_html(msg_startup(last_sol, last_bsc))

        log.info("🛡️ Watchdog active | SOL=%ds | BSC=%ds", SOL_POLL_INTERVAL_SEC, POLL_INTERVAL_SEC)

        last_sol_check = time.time()
        last_heartbeat = time.time()
        check_count = 0

        while not shutdown_event.is_set():
            try:
                current_time = time.time()
                check_count += 1
                
                # SOL: Check theo interval
                if current_time - last_sol_check >= SOL_POLL_INTERVAL_SEC:
                    log.info("🔍 [%d] Checking SOL...", check_count)
                    curr_sol = sol.get_roam_balance()
                    
                    if curr_sol is not None:
                        delta = curr_sol - last_sol
                        
                        if delta != 0:
                            log.info("📊 SOL: %s -> %s (delta: %s)", last_sol, curr_sol, delta)
                        
                        if delta.copy_abs() >= ALERT_THRESHOLD:
                            log.info("🚨 SOL ALERT! Delta: %s", delta)
                            sig = sol.get_latest_tx_signature()
                            if tele.send_html(msg_sol_change(delta, curr_sol, sig)):
                                last_sol = curr_sol
                        else:
                            last_sol = curr_sol
                    else:
                        log.warning("⚠️ SOL balance fetch failed")
                    
                    last_sol_check = current_time

                # BSC: Check transfers
                log.info("🔍 [%d] Checking BSC...", check_count)
                transfers = bsc_watch.poll()
                
                if transfers:
                    log.info("🚨 BSC ALERT! %d transfers detected", len(transfers))
                    curr_bsc = bsc.get_roam_balance()
                    if curr_bsc is None:
                        curr_bsc = last_bsc
                    
                    for t in transfers:
                        amt = t["amount"].copy_abs()
                        if amt < ALERT_THRESHOLD:
                            continue
                        if tele.send_html(msg_bsc_transfer(t["direction"], amt, curr_bsc, t["tx"])):
                            last_bsc = curr_bsc

                # Heartbeat
                if current_time - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                    uptime_hours = (datetime.now() - start_time).total_seconds() / 3600
                    sol_health = sol.get_health_status()
                    bsc_health = bsc.get_health_status()
                    
                    log.info("💓 Heartbeat | Uptime: %.1fh | Checks: %d", uptime_hours, check_count)
                    tele.send_html(msg_heartbeat(sol_health, bsc_health, uptime_hours))
                    last_heartbeat = current_time

                time.sleep(POLL_INTERVAL_SEC)

            except KeyboardInterrupt:
                log.info("⛔ Stopped by user")
                shutdown_event.set()
                break
            except Exception as e:
                log.exception("❌ Watchdog loop error: %s", e)
                time.sleep(POLL_INTERVAL_SEC)

        log.info("👋 Watchdog shutting down...")


# ==========================================================
# SIGNAL HANDLING
# ==========================================================
def signal_handler(sig, frame):
    log.info("🛑 Received signal %d, shutting down...", sig)
    shutdown_event.set()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ==========================================================
# MAIN
# ==========================================================
def main():
    log.info("=" * 60)
    log.info("🚀 ROAM WATCHDOG v2.0")
    log.info("=" * 60)
    
    # Start watchdog in background thread
    watchdog_thread = Thread(target=run_watchdog, daemon=True)
    watchdog_thread.start()
    
    # Start Flask server
    log.info(f"🌐 Starting Flask on 0.0.0.0:{PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
