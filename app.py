import os
import re
import time
import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import ccxt
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from pydantic import BaseModel

load_dotenv()

# =====================
# CONFIG
# =====================
BINGX_API_KEY = os.getenv("BINGX_API_KEY", "").strip()
BINGX_SECRET = os.getenv("BINGX_SECRET", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

# Comma-separated list like BTCUSDT,ETHUSDT,SOLUSDT or *
ALLOWED_SYMBOLS_RAW = os.getenv("ALLOWED_SYMBOLS", "*").strip()
DEFAULT_SETTLE = os.getenv("DEFAULT_SETTLE", "USDT").strip().upper() or "USDT"
DEFAULT_MARGIN_MODE = os.getenv("DEFAULT_MARGIN_MODE", "cross").strip().lower() or "cross"
PORT = int(os.getenv("PORT", "8000"))
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in {"1", "true", "yes", "on"}

# in-memory duplicate protection
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "120"))
MAX_RECENT_EVENTS = int(os.getenv("MAX_RECENT_EVENTS", "2000"))

if ALLOWED_SYMBOLS_RAW == "*":
    ALLOWED_SYMBOLS = {"*"}
else:
    ALLOWED_SYMBOLS = {s.strip().upper().replace("-", "") for s in ALLOWED_SYMBOLS_RAW.split(",") if s.strip()}

# =====================
# LOGGING
# =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("bingx_hybrid_bot")

# =====================
# APP
# =====================
app = FastAPI(title="BingX Hybrid Webhook Bot")

exchange = ccxt.bingx(
    {
        "apiKey": BINGX_API_KEY,
        "secret": BINGX_SECRET,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "defaultMarginMode": DEFAULT_MARGIN_MODE,
        },
    }
)

_markets_loaded = False
_markets_lock = threading.Lock()
_recent_events: Dict[str, float] = {}
_recent_lock = threading.Lock()


class WebhookPayload(BaseModel):
    source: Optional[str] = None
    mode: Optional[str] = None
    token: Optional[str] = None

    action: str
    symbol: str
    side: Optional[str] = None
    reason: Optional[str] = None

    orderId: Optional[str] = None
    lotTag: Optional[str] = None

    qtyCoin: Optional[float] = None
    triggerPrice: Optional[float] = None
    fillPrice: Optional[float] = None
    entryPrice: Optional[float] = None
    theoreticalAvg: Optional[float] = None
    theoreticalPositionQty: Optional[float] = None

    tpPercent: Optional[float] = None
    tpPrice: Optional[float] = None
    lotTpPrice: Optional[float] = None
    currentClose: Optional[float] = None
    callbackPercent: Optional[float] = None
    trailingMin: Optional[float] = None
    trailStop: Optional[float] = None

    numSells: Optional[int] = None
    subCoverMode: Optional[str] = None

    barTime: Optional[int] = None
    timestamp: Optional[int] = None


# =====================
# HELPERS
# =====================
def ensure_exchange_ready() -> None:
    global _markets_loaded
    if _markets_loaded:
        return
    with _markets_lock:
        if _markets_loaded:
            return
        exchange.load_markets()
        _markets_loaded = True
        logger.info("Markets loaded: %s", len(exchange.markets))


@app.on_event("startup")
def on_startup() -> None:
    if not BINGX_API_KEY or not BINGX_SECRET:
        logger.warning("BINGX_API_KEY або BINGX_SECRET не задані. Біржові запити не працюватимуть.")
        return
    try:
        ensure_exchange_ready()
    except Exception as exc:
        logger.exception("Не вдалося load_markets() на старті: %s", exc)


@app.on_event("shutdown")
def on_shutdown() -> None:
    try:
        exchange.close()
    except Exception:
        pass


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def normalize_tv_symbol_to_ccxt(raw_symbol: str) -> Tuple[str, str]:
    """
    Converts common TradingView/BingX symbols like:
      BTCUSDT, BTCUSDT.P, BINGX:BTCUSDT.P, BTC-USDT, BTC/USDT, BTC/USDT:USDT
    into CCXT unified swap symbol like:
      BTC/USDT:USDT
    Returns: (ccxt_symbol, compact_symbol)
    """
    s = (raw_symbol or "").strip().upper()
    if not s:
        raise ValueError("Empty symbol")

    # TradingView tickerid can include an exchange prefix like BINGX:BTCUSDT.P
    if ":" in s and "/" not in s:
        s = s.split(":", 1)[1]

    # Common perpetual suffixes used by TradingView / exchanges.
    s = re.sub(r"(\.P|[-_]?PERP|[-_]?SWAP)$", "", s)

    # Already unified CCXT symbol
    if "/" in s and ":" in s:
        compact = s.split(":", 1)[0].replace("/", "")
        return s, compact

    # BTC/USDT -> BTC/USDT:USDT
    if "/" in s and ":" not in s:
        base, quote = s.split("/", 1)
        base = re.sub(r"[^A-Z0-9]", "", base)
        quote = re.sub(r"[^A-Z0-9]", "", quote)
        return f"{base}/{quote}:{quote}", f"{base}{quote}"

    compact = re.sub(r"[^A-Z0-9]", "", s)
    compact = re.sub(r"(PERP|SWAP|P)$", "", compact)

    if compact.endswith(DEFAULT_SETTLE):
        base = compact[: -len(DEFAULT_SETTLE)]
        quote = DEFAULT_SETTLE
        if not base:
            raise ValueError(f"Invalid symbol: {raw_symbol}")
        return f"{base}/{quote}:{quote}", compact

    raise ValueError(
        f"Cannot normalize symbol '{raw_symbol}'. Expected something like BTCUSDT or BTC/USDT:USDT"
    )


def is_symbol_allowed(compact_symbol: str) -> bool:
    return "*" in ALLOWED_SYMBOLS or compact_symbol.upper() in ALLOWED_SYMBOLS


def verify_secret(payload: WebhookPayload, request: Request) -> None:
    if not WEBHOOK_SECRET:
        return

    provided = payload.token or request.headers.get("X-Webhook-Secret") or request.query_params.get("token")
    if provided != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def cleanup_recent_events() -> None:
    cutoff = time.time() - DEDUP_TTL_SEC
    stale_keys = [k for k, ts in _recent_events.items() if ts < cutoff]
    for key in stale_keys:
        _recent_events.pop(key, None)

    # hard cap protection
    if len(_recent_events) > MAX_RECENT_EVENTS:
        for key in sorted(_recent_events, key=_recent_events.get)[: len(_recent_events) - MAX_RECENT_EVENTS]:
            _recent_events.pop(key, None)


def make_dedup_key(payload: WebhookPayload) -> str:
    return "|".join(
        [
            payload.action or "",
            payload.symbol or "",
            payload.orderId or "",
            payload.lotTag or "",
            str(payload.timestamp or ""),
            str(payload.qtyCoin or ""),
        ]
    )


def register_event_once(payload: WebhookPayload) -> bool:
    key = make_dedup_key(payload)
    now_ts = time.time()
    with _recent_lock:
        cleanup_recent_events()
        if key in _recent_events:
            return False
        _recent_events[key] = now_ts
        return True


def get_real_short_position(ccxt_symbol: str) -> Optional[Dict[str, Any]]:
    ensure_exchange_ready()
    positions = exchange.fetch_positions([ccxt_symbol])

    for pos in positions:
        symbol = pos.get("symbol")
        contracts = pos.get("contracts") or pos.get("contractSize") or 0
        try:
            contracts = float(contracts)
        except Exception:
            contracts = 0.0

        side = str(pos.get("side") or "").lower()
        if symbol == ccxt_symbol and contracts != 0 and side == "short":
            return {
                "symbol": ccxt_symbol,
                "contracts": abs(contracts),
                "entryPrice": float(pos.get("entryPrice") or 0),
                "markPrice": float(pos.get("markPrice") or 0),
                "unrealizedPnl": float(pos.get("unrealizedPnl") or 0),
                "raw": pos,
            }
    return None


def fetch_last_price(ccxt_symbol: str) -> float:
    ensure_exchange_ready()
    ticker = exchange.fetch_ticker(ccxt_symbol)
    last = ticker.get("last") or ticker.get("close") or ticker.get("mark")
    if last is None:
        raise RuntimeError(f"No market price for {ccxt_symbol}")
    return float(last)


def place_short_market_entry(ccxt_symbol: str, qty_coin: float, payload: WebhookPayload) -> Dict[str, Any]:
    if qty_coin <= 0:
        raise ValueError("qtyCoin must be > 0 for entry")

    if DRY_RUN:
        logger.info("[DRY_RUN] SELL market %s qty=%s action=%s", ccxt_symbol, qty_coin, payload.action)
        return {"id": "dry-run-entry", "symbol": ccxt_symbol, "amount": qty_coin, "side": "sell"}

    order = exchange.create_order(
        symbol=ccxt_symbol,
        type="market",
        side="sell",
        amount=qty_coin,
        params={},
    )
    logger.info("ENTRY executed | action=%s | symbol=%s | qty=%s | order_id=%s", payload.action, ccxt_symbol, qty_coin, order.get("id"))
    return order


def close_short_reduce_only(ccxt_symbol: str, qty_coin: float, reason: str) -> Dict[str, Any]:
    if qty_coin <= 0:
        raise ValueError("close qty must be > 0")

    params = {"reduceOnly": True}

    if DRY_RUN:
        logger.info("[DRY_RUN] BUY reduceOnly %s qty=%s reason=%s", ccxt_symbol, qty_coin, reason)
        return {"id": "dry-run-close", "symbol": ccxt_symbol, "amount": qty_coin, "side": "buy", "reduceOnly": True}

    order = exchange.create_order(
        symbol=ccxt_symbol,
        type="market",
        side="buy",
        amount=qty_coin,
        params=params,
    )
    logger.info("CLOSE executed | reason=%s | symbol=%s | qty=%s | order_id=%s", reason, ccxt_symbol, qty_coin, order.get("id"))
    return order


# =====================
# ACTION HANDLERS
# =====================
def handle_entry_like_action(payload: WebhookPayload, ccxt_symbol: str) -> Dict[str, Any]:
    qty = float(payload.qtyCoin or 0)
    order = place_short_market_entry(ccxt_symbol, qty, payload)
    return {"status": "executed", "action": payload.action, "symbol": ccxt_symbol, "qty": qty, "order_id": order.get("id")}


def handle_full_tp_close(payload: WebhookPayload, ccxt_symbol: str) -> Dict[str, Any]:
    real_pos = get_real_short_position(ccxt_symbol)
    if not real_pos:
        logger.info("No short position for %s; FULL_TP_CLOSE ignored", ccxt_symbol)
        return {"status": "no_position", "action": payload.action, "symbol": ccxt_symbol}

    real_avg = float(real_pos["entryPrice"])
    qty = float(real_pos["contracts"])
    tp_percent = float(payload.tpPercent or 0)
    if tp_percent <= 0:
        raise ValueError("tpPercent must be > 0 for FULL_TP_CLOSE")

    tp_price = real_avg * (1.0 - tp_percent / 100.0)
    current_price = fetch_last_price(ccxt_symbol)

    logger.info(
        "FULL_TP check | symbol=%s | real_avg=%.8f | tp_percent=%.4f | tp_price=%.8f | market=%.8f | qty=%.8f",
        ccxt_symbol,
        real_avg,
        tp_percent,
        tp_price,
        current_price,
        qty,
    )

    if current_price <= tp_price:
        order = close_short_reduce_only(ccxt_symbol, qty, payload.reason or "FULL_TP_CLOSE")
        return {
            "status": "executed",
            "action": payload.action,
            "symbol": ccxt_symbol,
            "real_avg": real_avg,
            "tp_price": tp_price,
            "market_price": current_price,
            "closed_qty": qty,
            "order_id": order.get("id"),
        }

    logger.warning("FULL_TP condition not met | symbol=%s | market=%.8f | tp=%.8f", ccxt_symbol, current_price, tp_price)
    return {
        "status": "tp_not_reached",
        "action": payload.action,
        "symbol": ccxt_symbol,
        "real_avg": real_avg,
        "tp_price": tp_price,
        "market_price": current_price,
    }


def handle_subcover_close(payload: WebhookPayload, ccxt_symbol: str) -> Dict[str, Any]:
    real_pos = get_real_short_position(ccxt_symbol)
    if not real_pos:
        logger.info("No short position for %s; SUBCOVER_CLOSE ignored", ccxt_symbol)
        return {"status": "no_position", "action": payload.action, "symbol": ccxt_symbol}

    requested_qty = float(payload.qtyCoin or 0)
    if requested_qty <= 0:
        raise ValueError("qtyCoin must be > 0 for SUBCOVER_CLOSE")

    real_qty = float(real_pos["contracts"])
    close_qty = min(requested_qty, real_qty)
    current_price = fetch_last_price(ccxt_symbol)

    lot_tp = float(payload.lotTpPrice or 0)
    if lot_tp > 0 and current_price > lot_tp:
        logger.warning(
            "SUBCOVER condition not met | symbol=%s | market=%.8f | lot_tp=%.8f",
            ccxt_symbol,
            current_price,
            lot_tp,
        )
        return {
            "status": "tp_not_reached",
            "action": payload.action,
            "symbol": ccxt_symbol,
            "lot_tp_price": lot_tp,
            "market_price": current_price,
        }

    order = close_short_reduce_only(ccxt_symbol, close_qty, "SUBCOVER_CLOSE")
    return {
        "status": "executed",
        "action": payload.action,
        "symbol": ccxt_symbol,
        "requested_qty": requested_qty,
        "closed_qty": close_qty,
        "market_price": current_price,
        "order_id": order.get("id"),
    }


def process_payload(payload: WebhookPayload) -> Dict[str, Any]:
    ccxt_symbol, compact_symbol = normalize_tv_symbol_to_ccxt(payload.symbol)
    if not is_symbol_allowed(compact_symbol):
        logger.warning("Symbol not allowed: %s (normalized %s)", payload.symbol, compact_symbol)
        return {"status": "ignored", "reason": "symbol_not_allowed", "symbol": compact_symbol}

    action = payload.action.strip().upper()

    if action == "TEST_ALERT":
        return {
            "status": "test_ok",
            "action": action,
            "symbol": ccxt_symbol,
            "compact_symbol": compact_symbol,
            "reason": payload.reason or "MANUAL_HYBRID_DEBUG",
        }

    if action in {"FIRST_SHORT", "RESTART_SHORT", "DCA_SHORT"}:
        return handle_entry_like_action(payload, ccxt_symbol)

    if action == "FULL_TP_CLOSE":
        return handle_full_tp_close(payload, ccxt_symbol)

    if action == "SUBCOVER_CLOSE":
        return handle_subcover_close(payload, ccxt_symbol)

    logger.info("Unsupported action ignored: %s", action)
    return {"status": "ignored", "reason": "unsupported_action", "action": action}


def background_process(payload: WebhookPayload) -> None:
    try:
        result = process_payload(payload)
        logger.info("Webhook processed | action=%s | result=%s", payload.action, result)
    except Exception as exc:
        logger.exception("Webhook processing failed | action=%s | symbol=%s | error=%s", payload.action, payload.symbol, exc)


# =====================
# ROUTES
# =====================
@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {exc}") from exc

    try:
        payload = WebhookPayload(**body)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid payload: {exc}") from exc

    verify_secret(payload, request)

    if not register_event_once(payload):
        logger.warning("Duplicate webhook ignored | action=%s | symbol=%s | orderId=%s", payload.action, payload.symbol, payload.orderId)
        return {
            "status": "duplicate_ignored",
            "action": payload.action,
            "symbol": payload.symbol,
            "server_time": now_iso(),
        }

    logger.info(
        "Webhook accepted | action=%s | symbol=%s | orderId=%s | qtyCoin=%s",
        payload.action,
        payload.symbol,
        payload.orderId,
        payload.qtyCoin,
    )

    background_tasks.add_task(background_process, payload)
    return {
        "status": "accepted",
        "action": payload.action,
        "symbol": payload.symbol,
        "server_time": now_iso(),
        "dry_run": DRY_RUN,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "time": now_iso(),
        "exchange": "BingX",
        "markets_loaded": _markets_loaded,
        "dry_run": DRY_RUN,
        "allowed_symbols": sorted(ALLOWED_SYMBOLS),
        "default_margin_mode": DEFAULT_MARGIN_MODE,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
