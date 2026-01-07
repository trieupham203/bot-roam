import time
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Optional, List, Dict, Tuple

import requests

# ==========================================================
# CONFIG (SỬA Ở ĐÂY)
# ==========================================================
TELEGRAM_BOT_TOKEN = "8285842393:AAHIADmQQ0vYMmIOZp8lD-kdEID0bfDKIxU"
TELEGRAM_CHAT_ID = "@roamliquidity"  # Channel: bot phải là member/admin + quyền Post

# SOLANA (ROAM)
WALLET_SOL = "DSjPt6AtYu7NvKvVzxPkL2BMxrA3M4zK9jQaN1yunktg"
CONTRACT_ROAM_SOL = "RoamA1USA8xjvpTJZ6RvvxyDRzNh6GCA1zVGKSiMVkn"
RPC_SOL = "https://api.mainnet-beta.solana.com"

# BSC (ROAM)
WALLET_BSC = "0x3fefe29dA25BEa166fB5f6ADe7b5976D2b0e586B"
CONTRACT_ROAM_BSC = "0x3fefe29dA25BEa166fB5f6ADe7b5976D2b0e586B"
ROAM_BSC_DECIMALS = 6
RPC_BSC = "https://bsc-dataseed.binance.org/"

# Runtime
POLL_INTERVAL_SEC = 5
ALERT_THRESHOLD = Decimal("1")   # chỉ báo nếu |delta| >= 1 ROAM
SEND_STARTUP_MESSAGE = True

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ROAM_WATCHDOG")


# ==========================================================
# HELPERS
# ==========================================================
SEP = "──────────────"


def now_str() -> str:
    # dd/mm/YYYY HH:MM:SS
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def to_decimal(x) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None


def fmt_int_trunc(x: Decimal) -> str:
    """
    Cắt phần thập phân (truncate) - ROUND_DOWN cắt về phía 0.
    12.99 -> 12 ; -12.99 -> -12
    """
    n = x.quantize(Decimal("1"), rounding=ROUND_DOWN)
    return f"{n:,}"


def keccak_topic_transfer() -> str:
    # keccak256("Transfer(address,address,uint256)")
    return "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def pad_address_topic(addr: str) -> str:
    # 32 bytes topic: 0x + 24 bytes 0 + 20 bytes address
    a = addr.lower().replace("0x", "")
    return "0x" + ("0" * 48) + a  # 48 hex = 24 bytes


# ==========================================================
# TELEGRAM
# ==========================================================
class TelegramClient:
    def __init__(self, session: requests.Session):
        self.session = session
        self.url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    def send_html(self, message: str) -> None:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            r = self.session.post(self.url, json=payload, timeout=10)
            if r.status_code != 200:
                log.error("Telegram lỗi %s: %s", r.status_code, r.text[:500])
            else:
                log.info("✅ Sent Telegram.")
        except requests.RequestException as e:
            log.error("Lỗi mạng Telegram: %s", e)


# ==========================================================
# SOLANA
# ==========================================================
class SolanaReader:
    def __init__(self, session: requests.Session):
        self.session = session

    def get_roam_balance(self) -> Optional[Decimal]:
        """
        Tổng ROAM trên SOL (cộng dồn tất cả token accounts theo mint).
        """
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
        try:
            r = self.session.post(
                RPC_SOL,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()

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
        except (requests.RequestException, ValueError) as e:
            log.warning("SOL RPC lỗi: %s", e)
            return None

    def get_latest_tx_signature(self) -> Optional[str]:
        """
        Lấy signature mới nhất của address (để gắn link check transaction).
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [WALLET_SOL, {"limit": 1}],
        }
        try:
            r = self.session.post(
                RPC_SOL,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()
            res = data.get("result") or []
            if not res:
                return None
            return res[0].get("signature")
        except (requests.RequestException, ValueError) as e:
            log.warning("SOL getSignatures lỗi: %s", e)
            return None


# ==========================================================
# BSC
# ==========================================================
class BscReader:
    def __init__(self, session: requests.Session):
        self.session = session

    def rpc(self, method: str, params):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        r = self.session.post(RPC_BSC, json=payload, timeout=10)
        r.raise_for_status()
        return r.json()

    def get_latest_block(self) -> Optional[int]:
        try:
            data = self.rpc("eth_blockNumber", [])
            return int(data["result"], 16)
        except Exception as e:
            log.warning("BSC blockNumber lỗi: %s", e)
            return None

    def get_roam_balance(self) -> Optional[Decimal]:
        """
        balanceOf(wallet) qua eth_call
        """
        try:
            if not (WALLET_BSC.startswith("0x") and len(WALLET_BSC) == 42):
                log.error("WALLET_BSC không hợp lệ: %s", WALLET_BSC)
                return None

            wallet_padded = WALLET_BSC[2:].lower().zfill(64)
            data_param = "0x70a08231" + wallet_padded  # balanceOf

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": CONTRACT_ROAM_BSC, "data": data_param}, "latest"],
            }

            r = self.session.post(RPC_BSC, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()

            result = data.get("result")
            if not result or result == "0x":
                return Decimal("0")

            raw = int(result, 16)
            return Decimal(raw) / (Decimal(10) ** ROAM_BSC_DECIMALS)

        except Exception as e:
            log.warning("BSC balance lỗi: %s", e)
            return None


class BscTransferWatcher:
    """
    Quét Transfer logs của token contract để lấy tx hash (IN/OUT) liên tục.
    """
    def __init__(self, bsc: BscReader):
        self.bsc = bsc
        self.last_block: Optional[int] = None
        self.topic0 = keccak_topic_transfer()
        self.wallet_topic = pad_address_topic(WALLET_BSC)

    def _get_logs(self, from_block: int, to_block: int, direction: str) -> List[dict]:
        # IN  -> topic2 = wallet
        # OUT -> topic1 = wallet
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
        return data.get("result") or []

    def poll(self) -> List[Dict]:
        latest = self.bsc.get_latest_block()
        if latest is None:
            return []

        # Lần đầu: set mốc block hiện tại để không spam lịch sử
        if self.last_block is None:
            self.last_block = latest
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
        except Exception as e:
            log.warning("BSC getLogs lỗi: %s", e)

        self.last_block = latest

        # Parse + sort
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
        return parsed


# ==========================================================
# MESSAGE UI (DỄ NHÌN – RÕ RÀNG – ICON ĐƠN GIẢN)
# ==========================================================
def msg_startup(sol_bal: Decimal, bsc_bal: Decimal) -> str:
    return (
        "✅ <b>ROAM WATCH</b>\n"
        f"{SEP}\n"
        f"<b>SOL</b>: <b>{fmt_int_trunc(sol_bal)}</b> ROAM\n"
        f"<b>BSC</b>: <b>{fmt_int_trunc(bsc_bal)}</b> ROAM\n"
        f"{SEP}\n"
        f"🕒 <code>{now_str()}</code>"
    )


def msg_sol_change(delta: Decimal, new_bal: Decimal, tx_sig: Optional[str]) -> str:
    is_in = delta > 0
    t = "NẠP" if is_in else "RÚT"
    sign = "+" if is_in else "-"
    amt = delta.copy_abs()

    tx_line = f"\n🔗 <a href='https://solscan.io/tx/{tx_sig}'>Check transaction</a>" if tx_sig else ""

    return (
        "🔔 <b>ROAM UPDATE</b>\n"
        "<b>Network</b>: SOL\n"
        f"{SEP}\n"
        f"<b>Type</b>: {t}\n"
        f"<b>Amount</b>: <b>{sign}{fmt_int_trunc(amt)}</b> ROAM\n"
        f"<b>Balance</b>: <b>{fmt_int_trunc(new_bal)}</b> ROAM\n"
        f"{SEP}\n"
        f"🕒 <code>{now_str()}</code>"
        f"{tx_line}"
    )


def msg_bsc_transfer(direction: str, amount: Decimal, new_bal: Decimal, tx_hash: str) -> str:
    is_in = (direction == "IN")
    t = "NẠP" if is_in else "RÚT"
    sign = "+" if is_in else "-"

    return (
        "🔔 <b>ROAM UPDATE</b>\n"
        "<b>Network</b>: BSC\n"
        f"{SEP}\n"
        f"<b>Type</b>: {t}\n"
        f"<b>Amount</b>: <b>{sign}{fmt_int_trunc(amount)}</b> ROAM\n"
        f"<b>Balance</b>: <b>{fmt_int_trunc(new_bal)}</b> ROAM\n"
        f"{SEP}\n"
        f"🕒 <code>{now_str()}</code>\n"
        f"🔗 <a href='https://bscscan.com/tx/{tx_hash}'>Check transaction</a>"
    )


# ==========================================================
# MAIN
# ==========================================================
def main():
    with requests.Session() as session:
        tele = TelegramClient(session)

        sol = SolanaReader(session)
        bsc = BscReader(session)
        bsc_watch = BscTransferWatcher(bsc)

        log.info("🔄 Lấy dữ liệu lần đầu...")
        last_sol = sol.get_roam_balance() or Decimal("0")
        last_bsc = bsc.get_roam_balance() or Decimal("0")

        log.info("✅ OK | SOL=%s | BSC=%s", fmt_int_trunc(last_sol), fmt_int_trunc(last_bsc))

        if SEND_STARTUP_MESSAGE:
            tele.send_html(msg_startup(last_sol, last_bsc))

        log.info("🛡️ Canh gác liên tục... (%ss/lần)", POLL_INTERVAL_SEC)

        while True:
            try:
                # SOL: báo khi balance đổi + kèm link tx mới nhất
                curr_sol = sol.get_roam_balance()
                if curr_sol is not None:
                    delta = curr_sol - last_sol
                    if delta.copy_abs() >= ALERT_THRESHOLD:
                        sig = sol.get_latest_tx_signature()
                        tele.send_html(msg_sol_change(delta, curr_sol, sig))
                        last_sol = curr_sol

                # BSC: quét Transfer logs lấy tx thật sự (IN/OUT)
                transfers = bsc_watch.poll()
                if transfers:
                    curr_bsc = bsc.get_roam_balance() or last_bsc
                    for t in transfers:
                        amt = t["amount"].copy_abs()
                        if amt < ALERT_THRESHOLD:
                            continue
                        tele.send_html(msg_bsc_transfer(t["direction"], amt, curr_bsc, t["tx"]))
                    last_bsc = curr_bsc

                time.sleep(POLL_INTERVAL_SEC)

            except KeyboardInterrupt:
                log.info("⛔ Stopped by user.")
                break
            except Exception as e:
                log.exception("Lỗi vòng lặp: %s", e)
                time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
