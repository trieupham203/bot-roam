import json
import os
import time
import random
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
POLL_INTERVAL_SEC = 8                      # bạn có thể tăng 10-20s để giảm 429
ALERT_THRESHOLD = Decimal("1")             # chỉ báo nếu |amount| >= 1 ROAM
STATE_FILE = "roam_state.json"             # lưu state chống spam khi restart

# ==========================================================
# LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ROAM_TX_ONLY")


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
    # keccak256("Transfer(address,address,uint256)")
    return "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def pad_address_topic(addr: str) -> str:
    a = addr.lower().replace("0x", "")
    return "0x" + ("0" * 48) + a


def atomic_write_json(path: str, data: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ==========================================================
# STATE
# ==========================================================
class StateStore:
    def __init__(self, path: str):
        self.path = path
        self.data = {
            "sol_last_sig": None,
            "bsc_last_block": None,
        }
        self.load()

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict):
                self.data.update(obj)
        except Exception as e:
            log.warning("Không đọc được state file (%s): %s", self.path, e)

    def save(self) -> None:
        try:
            atomic_write_json(self.path, self.data)
        except Exception as e:
            log.warning("Không lưu được state file (%s): %s", self.path, e)

    @property
    def sol_last_sig(self) -> Optional[str]:
        return self.data.get("sol_last_sig")

    @sol_last_sig.setter
    def sol_last_sig(self, v: Optional[str]) -> None:
        self.data["sol_last_sig"] = v

    @property
    def bsc_last_block(self) -> Optional[int]:
        v = self.data.get("bsc_last_block")
        return int(v) if v is not None else None

    @bsc_last_block.setter
    def bsc_last_block(self, v: Optional[int]) -> None:
        self.data["bsc_last_block"] = v


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
            r = self.session.post(self.url, json=payload, timeout=12)
            if r.status_code != 200:
                log.error("Telegram lỗi %s: %s", r.status_code, r.text[:500])
            else:
                log.info("✅ Sent Telegram.")
        except requests.RequestException as e:
            log.error("Lỗi mạng Telegram: %s", e)


def msg_tx(chain: str, direction: str, amount: Decimal, balance: Optional[Decimal], tx_url: str) -> str:
    # direction: IN / OUT
    is_in = (direction == "IN")
    sign = "+" if is_in else "-"
    arrow = "⬆️" if is_in else "⬇️"

    bal_line = ""
    if balance is not None:
        bal_line = f"\nBalance: <b>{fmt_int_trunc(balance)}</b> ROAM"

    return (
        "🔔 <b>ROAM ALERT</b>\n"
        f"{SEP}\n"
        f"Network: <b>{chain}</b>\n"
        f"{arrow} {('NẠP' if is_in else 'RÚT')}\n"
        f"Amount: <b>{sign}{fmt_int_trunc(amount.copy_abs())}</b> ROAM"
        f"{bal_line}\n"
        f"{SEP}\n"
        f"🕒 <code>{now_str()}</code>\n"
        f"🔗 <a href=\"{tx_url}\">Transaction</a>"
    )


# ==========================================================
# RPC HELPERS (BACKOFF)
# ==========================================================
def post_json_with_backoff(
    session: requests.Session,
    url: str,
    payload: dict,
    headers: Optional[dict] = None,
    timeout: int = 10,
    max_backoff: int = 60,
) -> Optional[dict]:
    backoff = 2
    while True:
        try:
            r = session.post(url, json=payload, headers=headers, timeout=timeout)
            if r.status_code == 429:
                sleep_s = min(backoff, max_backoff) + random.uniform(0, 1.2)
                log.warning("RPC 429 (%s) — sleep %.1fs", url, sleep_s)
                time.sleep(sleep_s)
                backoff = min(backoff * 2, max_backoff)
                continue

            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            sleep_s = min(backoff, max_backoff) + random.uniform(0, 1.2)
            log.warning("RPC lỗi (%s): %s — sleep %.1fs", url, e, sleep_s)
            time.sleep(sleep_s)
            backoff = min(backoff * 2, max_backoff)
        except ValueError:
            log.warning("RPC trả JSON lỗi (%s)", url)
            return None


# ==========================================================
# SOLANA (CHỈ NHẮN KHI CÓ SIGNATURE MỚI)
# ==========================================================
class SolanaReader:
    def __init__(self, session: requests.Session):
        self.session = session

    def get_signatures(self, limit: int = 10) -> List[dict]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [WALLET_SOL, {"limit": limit}],
        }
        data = post_json_with_backoff(
            self.session,
            RPC_SOL,
            payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        return (data or {}).get("result") or []

    def get_transaction(self, signature: str) -> Optional[dict]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        }
        return post_json_with_backoff(
            self.session,
            RPC_SOL,
            payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )

    def parse_roam_delta_and_balance(self, tx: dict) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Trả về (delta, new_balance) dựa trên preTokenBalances/postTokenBalances
        cho đúng mint ROAM + đúng owner WALLET_SOL.
        """
        try:
            result = tx.get("result") or {}
            meta = result.get("meta") or {}
            pre = meta.get("preTokenBalances") or []
            post = meta.get("postTokenBalances") or []

            def sum_for(arr: list) -> Decimal:
                total = Decimal("0")
                for it in arr:
                    if it.get("mint") != CONTRACT_ROAM_SOL:
                        continue
                    if it.get("owner") != WALLET_SOL:
                        continue
                    ui = (((it.get("uiTokenAmount") or {}).get("uiAmount")))
                    d = to_decimal(ui)
                    if d is not None:
                        total += d
                return total

            pre_total = sum_for(pre)
            post_total = sum_for(post)

            delta = post_total - pre_total
            # Nếu tx không chạm ROAM mint/owner -> delta = 0
            return delta, post_total
        except Exception:
            return None, None


# ==========================================================
# BSC (CHỈ NHẮN KHI CÓ LOG MỚI)
# ==========================================================
class BscReader:
    def __init__(self, session: requests.Session):
        self.session = session

    def rpc(self, method: str, params):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        return post_json_with_backoff(self.session, RPC_BSC, payload, timeout=12)

    def get_latest_block(self) -> Optional[int]:
        try:
            data = self.rpc("eth_blockNumber", [])
            if not data or "result" not in data:
                return None
            return int(data["result"], 16)
        except Exception as e:
            log.warning("BSC blockNumber lỗi: %s", e)
            return None

    def get_roam_balance(self) -> Optional[Decimal]:
        try:
            wallet_padded = WALLET_BSC[2:].lower().zfill(64)
            data_param = "0x70a08231" + wallet_padded  # balanceOf

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": CONTRACT_ROAM_BSC, "data": data_param}, "latest"],
            }

            data = post_json_with_backoff(self.session, RPC_BSC, payload, timeout=12)
            if not data or not data.get("result") or data["result"] == "0x":
                return Decimal("0")

            raw = int(data["result"], 16)
            return Decimal(raw) / (Decimal(10) ** ROAM_BSC_DECIMALS)
        except Exception as e:
            log.warning("BSC balance lỗi: %s", e)
            return None


class BscTransferWatcher:
    def __init__(self, bsc: BscReader, start_block: Optional[int]):
        self.bsc = bsc
        self.last_block: Optional[int] = start_block
        self.topic0 = keccak_topic_transfer()
        self.wallet_topic = pad_address_topic(WALLET_BSC)

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
        return (data or {}).get("result") or []

    def poll(self) -> Tuple[List[Dict], Optional[int]]:
        latest = self.bsc.get_latest_block()
        if latest is None:
            return [], self.last_block

        # Nếu chưa có mốc: set mốc hiện tại (không gửi lịch sử)
        if self.last_block is None:
            self.last_block = latest
            return [], self.last_block

        if latest <= self.last_block:
            return [], self.last_block

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
        return parsed, self.last_block


# ==========================================================
# MAIN (CHỈ GỬI KHI CÓ TX MỚI)
# ==========================================================
def main():
    state = StateStore(STATE_FILE)

    with requests.Session() as session:
        tele = TelegramClient(session)
        sol = SolanaReader(session)
        bsc = BscReader(session)

        # Khởi tạo BSC watcher từ state (nếu có), nếu không có thì để None -> set mốc theo latest và không gửi lịch sử
        bsc_watch = BscTransferWatcher(bsc, start_block=state.bsc_last_block)

        # SOL: nếu chưa có last_sig -> set baseline = sig mới nhất (không gửi)
        if not state.sol_last_sig:
            sigs = sol.get_signatures(limit=1)
            if sigs:
                state.sol_last_sig = sigs[0].get("signature")
                state.save()
                log.info("SOL baseline set: %s", state.sol_last_sig)

        # BSC: nếu chưa có last_block -> poll 1 lần để set baseline
        if state.bsc_last_block is None:
            _, lastb = bsc_watch.poll()
            state.bsc_last_block = lastb
            state.save()
            log.info("BSC baseline set: %s", state.bsc_last_block)

        log.info("🛡️ Running (TX-only). Interval=%ss", POLL_INTERVAL_SEC)

        while True:
            try:
                # --------------------------
                # SOL: xử lý signature mới
                # --------------------------
                try:
                    sigs = sol.get_signatures(limit=15)
                    if sigs:
                        newest_sig = sigs[0].get("signature")

                        if state.sol_last_sig and newest_sig != state.sol_last_sig:
                            # Lấy các sig mới (từ newest về đến trước last_sig), rồi đảo để xử lý từ cũ -> mới
                            new_list = []
                            for it in sigs:
                                s = it.get("signature")
                                if not s:
                                    continue
                                if s == state.sol_last_sig:
                                    break
                                new_list.append(s)

                            for sig in reversed(new_list):
                                tx = sol.get_transaction(sig)
                                if not tx:
                                    continue

                                delta, new_bal = sol.parse_roam_delta_and_balance(tx)
                                if delta is None:
                                    continue

                                # Nếu tx không liên quan ROAM -> bỏ qua, nhưng vẫn update last_sig để không nhắn lại
                                if delta.copy_abs() < ALERT_THRESHOLD:
                                    state.sol_last_sig = sig
                                    state.save()
                                    continue

                                direction = "IN" if delta > 0 else "OUT"
                                tele.send_html(
                                    msg_tx(
                                        chain="SOL",
                                        direction=direction,
                                        amount=delta.copy_abs(),
                                        balance=new_bal,
                                        tx_url=f"https://solscan.io/tx/{sig}",
                                    )
                                )

                                state.sol_last_sig = sig
                                state.save()

                        # Nếu state rỗng hoặc chưa cập nhật, giữ cho chắc
                        if not state.sol_last_sig and newest_sig:
                            state.sol_last_sig = newest_sig
                            state.save()

                except Exception as e:
                    log.warning("SOL loop lỗi: %s", e)

                # --------------------------
                # BSC: xử lý logs mới
                # --------------------------
                transfers, last_block = bsc_watch.poll()
                if last_block is not None and last_block != state.bsc_last_block:
                    state.bsc_last_block = last_block
                    state.save()

                if transfers:
                    # chỉ khi có transfer mới gọi balance (đỡ tốn)
                    curr_bsc_bal = bsc.get_roam_balance()
                    for t in transfers:
                        amt = t["amount"]
                        if amt.copy_abs() < ALERT_THRESHOLD:
                            continue

                        direction = t["direction"]
                        tele.send_html(
                            msg_tx(
                                chain="BSC",
                                direction=direction,
                                amount=amt.copy_abs(),
                                balance=curr_bsc_bal,
                                tx_url=f"https://bscscan.com/tx/{t['tx']}",
                            )
                        )

                time.sleep(POLL_INTERVAL_SEC)

            except KeyboardInterrupt:
                log.info("⛔ Stopped by user.")
                break
            except Exception as e:
                log.exception("Main loop lỗi: %s", e)
                time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
