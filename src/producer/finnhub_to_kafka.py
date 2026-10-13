#finnhub_to_kafka.py
#!/usr/bin/env python3
# finnhub_to_kafka.py
#
# Goal:
# - Consume Finnhub WS trades (tick-by-tick)
# - Periodically fetch Finnhub REST quote (o/h/l/c/pc)
# - Enrich EACH trade with the latest quote snapshot
# - Send enriched events to Kafka

import os
import json
import time
import asyncio
import requests
import websockets
from kafka import KafkaProducer

WS_URL = "wss://ws.finnhub.io?token={token}"
REST_QUOTE_URL = "https://finnhub.io/api/v1/quote"


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_exchange_pair(symbol: str) -> tuple[str | None, str]:
    # Finnhub crypto symbol: "BINANCE:BTCUSDT"
    if ":" in symbol:
        ex, pair = symbol.split(":", 1)
        return ex, pair
    return None, symbol


def fetch_quote(symbol: str, api_key: str) -> dict:
    r = requests.get(
        REST_QUOTE_URL,
        params={"symbol": symbol, "token": api_key},
        timeout=8,
    )
    r.raise_for_status()
    return r.json()


def build_kafka_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        linger_ms=50,
        acks=1,
        retries=3,
    )


async def quote_loop(latest_quotes: dict, quotes_lock: asyncio.Lock, api_key: str, symbols: list[str], interval_s: int):
    """
    Keeps latest quote snapshot in memory:
      latest_quotes[symbol] = {"open":..., "high":..., "low":..., "close":..., "prev_close":..., "quote_event_ts_ms":...}
    """
    while True:
        try:
            for sym in symbols:
                # requests is blocking -> run it in a thread
                q = await asyncio.to_thread(fetch_quote, sym, api_key)

                snapshot = {
                    "open": q.get("o"),
                    "high": q.get("h"),
                    "low": q.get("l"),
                    "close": q.get("c"),
                    "prev_close": q.get("pc"),
                    "quote_event_ts_ms": (q.get("t") * 1000) if q.get("t") is not None else None,
                    "quote_ingest_ts_ms": now_ms(),
                }

                async with quotes_lock:
                    latest_quotes[sym] = snapshot

        except Exception as e:
            print(f"[QUOTE] error: {e}")

        await asyncio.sleep(interval_s)


async def ws_trade_loop(
    producer: KafkaProducer,
    topic: str,
    api_key: str,
    symbols: list[str],
    latest_quotes: dict,
    quotes_lock: asyncio.Lock,
):
    backoff = 2
    while True:
        try:
            url = WS_URL.format(token=api_key)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                # Subscribe to symbols
                for sym in symbols:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

                print("[WS] connected & subscribed:", ", ".join(symbols))
                backoff = 2

                async for msg in ws:
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if data.get("type") != "trade":
                        continue

                    trades = data.get("data") or []
                    ingest = now_ms()

                    for tr in trades:
                        sym = tr.get("s")
                        if not sym:
                            continue

                        ex, pair = parse_exchange_pair(sym)

                        price = tr.get("p")
                        vol = tr.get("v")
                        event_ts_ms = tr.get("t")  # WS trade timestamp is ms

                        # Attach latest quote snapshot (if available)
                        async with quotes_lock:
                            snap = latest_quotes.get(sym)

                        event = {
                            "source": "finnhub",
                            "asset_class": "crypto",
                            "event_type": "trade",
                            "symbol": sym,
                            "exchange": ex,
                            "pair": pair,

                            "price": price,
                            "volume": vol,
                            "notional": (price * vol) if (price is not None and vol is not None) else None,
 
                            "event_ts_ms": event_ts_ms,
                            "ingest_ts_ms": ingest,
                        }

                        # If we already fetched quotes, enrich the trade with OHLC
                        if snap:
                            event.update({
                                "open": snap.get("open"),
                                "high": snap.get("high"),
                                "low": snap.get("low"),
                                "close": snap.get("close"),
                                "prev_close": snap.get("prev_close"),
                                "quote_event_ts_ms": snap.get("quote_event_ts_ms"),
                                "quote_ingest_ts_ms": snap.get("quote_ingest_ts_ms"),
                            })

                        producer.send(topic, value=event)

        except Exception as e:
            print(f"[WS] error, reconnect in {backoff}s: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def main():
    api_key = os.environ.get("FINNHUB_API_KEY")
    if not api_key:
        raise SystemExit("FINNHUB_API_KEY is required (export FINNHUB_API_KEY=...)")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "crypto_trades")

    symbols_env = os.environ.get("SYMBOLS", "BINANCE:BTCUSDT")
    symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]

    quote_interval_s = int(os.environ.get("QUOTE_INTERVAL_S", "10"))

    producer = build_kafka_producer(bootstrap)

    latest_quotes = {}                 # shared cache {symbol: snapshot}
    quotes_lock = asyncio.Lock()       # protect shared cache

    await asyncio.gather(
        quote_loop(latest_quotes, quotes_lock, api_key, symbols, quote_interval_s),
        ws_trade_loop(producer, topic, api_key, symbols, latest_quotes, quotes_lock),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass