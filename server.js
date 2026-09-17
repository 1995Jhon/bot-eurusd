"""
Bot de SEÑALES (no ejecuta operaciones) para EUR/USD.
Analiza velas, calcula EMA9/EMA21 + RSI14 + MACD + ATR,
y envía un aviso a Telegram cuando las condiciones se alinean.

La ejecución de la operación (Comprar/Vender) la hacés VOS,
a mano, en tu plataforma (World Binary, IQ Option, etc.).

Requisitos:
    pip install requests pandas numpy

Variables de entorno necesarias (configuralas en Render o en tu .env):
    TWELVE_DATA_API_KEY   -> API key gratuita de https://twelvedata.com
    TELEGRAM_BOT_TOKEN    -> token de tu bot de Telegram (via @BotFather)
    TELEGRAM_CHAT_ID      -> tu chat id (via @userinfobot, por ejemplo)

    Opcionales:
    SYMBOL          (default "EUR/USD")
    INTERVAL        (default "5min")   -> 1min, 5min, 15min, etc.
    POLL_SECONDS    (default 60)       -> cada cuánto revisa el mercado
"""

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime

# ---------- Configuración ----------
TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SYMBOL = os.environ.get("SYMBOL", "EUR/USD")
INTERVAL = os.environ.get("INTERVAL", "5min")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))

TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"

# Evita mandar la misma señal repetida vela tras vela
last_signal_sent = None
last_candle_time = None


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("⚠️  Telegram no configurado, no se envía mensaje.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        if r.status_code != 200:
            log(f"⚠️  Error enviando Telegram: {r.text}")
    except Exception as e:
        log(f"⚠️  Excepción enviando Telegram: {e}")


def fetch_candles(symbol: str, interval: str, outputsize: int = 100) -> pd.DataFrame:
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVE_DATA_API_KEY,
        "format": "JSON",
    }
    r = requests.get(TWELVE_DATA_URL, params=params, timeout=15)
    data = r.json()
    if "values" not in data:
        raise RuntimeError(f"Error de Twelve Data: {data}")
    df = pd.DataFrame(data["values"])
    df = df.rename(columns={"datetime": "time"})
    df["time"] = pd.to_datetime(df["time"])
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df = df.sort_values("time").reset_index(drop=True)
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    # EMA
    df["ema9"] = close.ewm(span=9, adjust=False).mean()
    df["ema21"] = close.ewm(span=21, adjust=False).mean()

    # RSI14
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))

    # MACD (12,26,9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # ATR14 (volatilidad)
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - close.shift()).abs()
    low_close = (df["low"] - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr14"] = tr.rolling(14).mean()

    return df


def generate_signal(df: pd.DataFrame) -> dict:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    ema_cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    ema_cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]

    rsi_ok_buy = 40 < last["rsi14"] < 70   # evita sobrecompra extrema
    rsi_ok_sell = 30 < last["rsi14"] < 60  # evita sobreventa extrema

    macd_bullish = last["macd_hist"] > 0
    macd_bearish = last["macd_hist"] < 0

    # Filtro de volatilidad: si el ATR está muy bajo respecto al promedio
    # reciente, el mercado está plano -> no vale la pena operar.
    atr_mean = df["atr14"].tail(20).mean()
    volatility_ok = last["atr14"] >= 0.6 * atr_mean if not np.isnan(atr_mean) else False

    signal = "NO_TRADE"
    reason = "Sin alineación de condiciones"

    if ema_cross_up and rsi_ok_buy and macd_bullish and volatility_ok:
        signal = "BUY"
        reason = "Cruce EMA9>EMA21 + RSI neutral-alcista + MACD positivo + volatilidad ok"
    elif ema_cross_down and rsi_ok_sell and macd_bearish and volatility_ok:
        signal = "SELL"
        reason = "Cruce EMA9<EMA21 + RSI neutral-bajista + MACD negativo + volatilidad ok"
    elif not volatility_ok:
        reason = "Mercado con baja volatilidad (lateral), se descarta señal"

    return {
        "signal": signal,
        "reason": reason,
        "close": last["close"],
        "ema9": last["ema9"],
        "ema21": last["ema21"],
        "rsi14": last["rsi14"],
        "macd_hist": last["macd_hist"],
        "atr14": last["atr14"],
        "time": last["time"],
    }


def run_once():
    global last_signal_sent, last_candle_time

    df = fetch_candles(SYMBOL, INTERVAL)
    df = compute_indicators(df)
    result = generate_signal(df)

    candle_time = result["time"]

    log(
        f"{SYMBOL} | close={result['close']:.5f} EMA9={result['ema9']:.5f} "
        f"EMA21={result['ema21']:.5f} RSI={result['rsi14']:.2f} "
        f"MACDhist={result['macd_hist']:.6f} ATR={result['atr14']:.5f} "
        f"=> {result['signal']}"
    )

    # Evita reenviar la misma señal para la misma vela
    if candle_time == last_candle_time:
        return

    last_candle_time = candle_time

    if result["signal"] in ("BUY", "SELL") and result["signal"] != last_signal_sent:
        emoji = "🟢" if result["signal"] == "BUY" else "🔴"
        msg = (
            f"{emoji} SEÑAL {result['signal']} - {SYMBOL} ({INTERVAL})\n"
            f"Precio: {result['close']:.5f}\n"
            f"Motivo: {result['reason']}\n"
            f"RSI14: {result['rsi14']:.1f} | MACD hist: {result['macd_hist']:.6f}\n"
            f"Hora vela: {candle_time}\n\n"
            f"⚠️ Señal de análisis técnico, no es garantía de resultado. "
            f"La decisión y ejecución son tuyas."
        )
        send_telegram(msg)
        last_signal_sent = result["signal"]
    elif result["signal"] == "NO_TRADE":
        last_signal_sent = None  # resetea para poder volver a avisar cuando aparezca señal


def main():
    log(f"Iniciando bot de señales para {SYMBOL} ({INTERVAL})")
    if not TWELVE_DATA_API_KEY:
        log("❌ Falta TWELVE_DATA_API_KEY. El bot no puede pedir datos de mercado.")
        return
    while True:
        try:
            run_once()
        except Exception as e:
            log(f"❌ Error en el ciclo: {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
