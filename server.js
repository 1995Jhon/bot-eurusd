const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

const API_BASE = "https://api.derivws.com";
const DERIV_TOKEN = process.env.DERIV_TOKEN;
const DERIV_APP_ID = process.env.DERIV_APP_ID;

const SYMBOL = "frxEURUSD";
const AMOUNT = 1;
const DURATION = 5;
const DURATION_UNIT = "m";

const MAX_TRADES_PER_HOUR = 5;
const AUTO_TRADING = process.env.AUTO_TRADING !== "false";

let tradesThisHour = 0;
let hourStarted = Date.now();
let lastTradeCandle = null;
let lastSignal = "NO_TRADE";
let lastAutoRun = null;
let lastTrade = null;
let autoBusy = false;

function checkConfig() {
  if (!DERIV_TOKEN) throw new Error("Falta DERIV_TOKEN");
  if (!DERIV_APP_ID) throw new Error("Falta DERIV_APP_ID");
}

function resetHourlyCounter() {
  if (Date.now() - hourStarted >= 3600000) {
    tradesThisHour = 0;
    hourStarted = Date.now();
  }
}

function candleId() {
  return Math.floor(Date.now() / 300000);
}

async function getDemoAccount() {
  checkConfig();

  const r = await fetch(
    `${API_BASE}/trading/v1/options/accounts`,
    {
      headers: {
        Authorization: `Bearer ${DERIV_TOKEN}`,
        "Deriv-App-ID": DERIV_APP_ID
      }
    }
  );

  const data = await r.json();

  if (!r.ok) {
    throw new Error(
      data?.errors?.[0]?.message ||
      "No se pudieron obtener las cuentas"
    );
  }

  let accounts = data.data || [];
  if (!Array.isArray(accounts)) accounts = [accounts];

  const demo = accounts.find(
    a =>
      a.account_type === "demo" &&
      a.status === "active"
  );

  if (!demo?.account_id) {
    throw new Error("No se encontró cuenta DEMO activa");
  }

  console.log("Cuenta DEMO:", demo.account_id);

  return demo.account_id;
}

async function getTradingWebSocket(accountId) {
  checkConfig();

  const r = await fetch(
    `${API_BASE}/trading/v1/options/accounts/${encodeURIComponent(accountId)}/otp`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${DERIV_TOKEN}`,
        "Deriv-App-ID": DERIV_APP_ID
      }
    }
  );

  const data = await r.json();

  if (!r.ok) {
    throw new Error(
      data?.errors?.[0]?.message ||
      "No se pudo obtener OTP"
    );
  }

  if (!data?.data?.url) {
    throw new Error("Deriv no devolvió WebSocket");
  }

  return data.data.url;
}

function getCandles() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(
      `${API_BASE}/trading/v1/options/ws/public`
    );

    let done = false;

    const timeout = setTimeout(() => {
      finish(
        reject,
        new Error("Timeout obteniendo EUR/USD")
      );
    }, 15000);

    function finish(fn, value) {
      if (done) return;
      done = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch {}

      fn(value);
    }

    ws.on("open", () => {
      console.log("Mercado EUR/USD conectado");

      ws.send(
        JSON.stringify({
          ticks_history: SYMBOL,
          end: "latest",
          count: 100,
          style: "candles",
          granularity: 300,
          subscribe: 0,
          req_id: 1
        })
      );
    });

    ws.on("message", raw => {
      try {
        const data = JSON.parse(raw.toString());

        if (data.error) {
          return finish(
            reject,
            new Error(data.error.message)
          );
        }

        if (
          data.msg_type === "candles" &&
          Array.isArray(data.candles)
        ) {
          const candles = data.candles
            .map(c => ({
              open: Number(c.open),
              high: Number(c.high),
              low: Number(c.low),
              close: Number(c.close),
              epoch: Number(c.epoch)
            }))
            .filter(c => Number.isFinite(c.close));

          if (candles.length < 30) {
            return finish(
              reject,
              new Error("No hay suficientes velas")
            );
          }

          finish(resolve, candles);
        }
      } catch (e) {
        finish(reject, e);
      }
    });

    ws.on("error", e => finish(reject, e));
  });
}

function ema(values, period) {
  if (values.length < period) return null;

  const k = 2 / (period + 1);

  let result =
    values
      .slice(0, period)
      .reduce((a, b) => a + b, 0) / period;

  for (let i = period; i < values.length; i++) {
    result =
      values[i] * k +
      result * (1 - k);
  }

  return result;
}

function rsi(values, period = 14) {
  if (values.length <= period) return null;

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const change =
      values[i] - values[i - 1];

    if (change >= 0) gains += change;
    else losses += Math.abs(change);
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {
    const change =
      values[i] - values[i - 1];

    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);

    avgGain =
      (avgGain * (period - 1) + gain) /
      period;

    avgLoss =
      (avgLoss * (period - 1) + loss) /
      period;
  }

  if (avgLoss === 0) return 100;

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

function getSignal(candles) {
  const closes = candles.map(c => c.close);

  const fast = ema(closes, 9);
  const slow = ema(closes, 21);
  const current = closes.at(-1);
  const previous = closes.at(-2);
  const r = rsi(closes, 14);

  if (
    ![fast, slow, current, previous, r]
      .every(Number.isFinite)
  ) {
    return null;
  }

  if (
    fast > slow &&
    r >= 50 &&
    r <= 70 &&
    current > previous
  ) {
    return "CALL";
  }

  if (
    fast < slow &&
    r >= 30 &&
    r <= 50 &&
    current < previous
  ) {
    return "PUT";
  }

  return null;
}

function executeTrade(direction) {
  return new Promise(async (resolve, reject) => {
    let ws;

    try {
      checkConfig();

      resetHourlyCounter();

      if (tradesThisHour >= MAX_TRADES_PER_HOUR) {
        throw new Error(
          "Límite de 5 operaciones por hora"
        );
      }

      const candle = candleId();

      if (lastTradeCandle === candle) {
        throw new Error(
          "Ya hubo una operación en esta vela"
        );
      }

      const accountId =
        await getDemoAccount();

      const url =
        await getTradingWebSocket(accountId);

      ws = new WebSocket(url);

      let finished = false;

      const timeout = setTimeout(() => {
        if (!finished) {
          finished = true;

          try {
            ws.close();
          } catch {}

          reject(
            new Error(
              "Timeout con Deriv"
            )
          );
        }
      }, 15000);

      ws.on("open", () => {
        console.log(
          `WebSocket DEMO conectado | ${direction}`
        );

        ws.send(
          JSON.stringify({
            proposal: 1,
            amount: AMOUNT,
            basis: "stake",
            contract_type: direction,
            currency: "USD",
            duration: DURATION,
            duration_unit: DURATION_UNIT,
            underlying_symbol: SYMBOL,
            req_id: 1
          })
        );
      });

      ws.on("message", raw => {
        try {
          const data =
            JSON.parse(raw.toString());

          console.log(
            "Deriv:",
            JSON.stringify(data)
          );

          if (data.error) {
            if (!finished) {
              finished = true;
              clearTimeout(timeout);

              try {
                ws.close();
              } catch {}

              reject(
                new Error(
                  data.error.message ||
                  "Error de Deriv"
                )
              );
            }

            return;
          }

          if (
            data.msg_type === "proposal" &&
            data.proposal?.id
          ) {
            const proposalId =
              data.proposal.id;

            const price =
              Number(
                data.proposal.ask_price
              );

            if (!Number.isFinite(price)) {
              throw new Error(
                "Precio de propuesta inválido"
              );
            }

            console.log(
              "Propuesta:",
              proposalId
            );

            ws.send(
              JSON.stringify({
                buy: proposalId,
                price: price,
                req_id: 2
              })
            );

            return;
          }

          if (
            data.msg_type === "buy" &&
            data.buy
          ) {
            tradesThisHour++;
            lastTradeCandle = candle;

            lastTrade = {
              time: new Date().toISOString(),
              direction,
              amount: AMOUNT,
              duration: `${DURATION}m`,
              contract_id:
                data.buy.contract_id,
              account_id: accountId
            };

            if (!finished) {
              finished = true;
              clearTimeout(timeout);

              try {
                ws.close();
              } catch {}

              console.log(
                "OPERACIÓN DEMO CONFIRMADA",
                JSON.stringify(lastTrade)
              );

              resolve(lastTrade);
            }
          }
        } catch (e) {
          if (!finished) {
            finished = true;
            clearTimeout(timeout);

            try {
              ws.close();
            } catch {}

            reject(e);
          }
        }
      });

      ws.on("error", e => {
        if (!finished) {
          finished = true;
          clearTimeout(timeout);
          reject(e);
        }
      });

    } catch (e) {
      reject(e);
    }
  });
}

async function runAutoTrader() {
  if (!AUTO_TRADING || autoBusy) return;

  autoBusy = true;
  lastAutoRun =
    new Date().toISOString();

  try {
    console.log(
      "=============================="
    );

    console.log(
      "AUTO | Analizando EUR/USD..."
    );

    const candles =
      await getCandles();

    const closes =
      candles.map(c => c.close);

    const fast =
      ema(closes, 9);

    const slow =
      ema(closes, 21);

    const currentRsi =
      rsi(closes, 14);

    const signal =
      getSignal(candles);

    lastSignal =
      signal || "NO_TRADE";

    console.log(
      `AUTO | EMA9=${fast?.toFixed(6)}`
    );

    console.log(
      `AUTO | EMA21=${slow?.toFixed(6)}`
    );

    console.log(
      `AUTO | RSI14=${currentRsi?.toFixed(2)}`
    );

    console.log(
      `AUTO | SEÑAL=${lastSignal}`
    );

    if (!signal) {
      console.log(
        "AUTO | Sin operación"
      );

      return;
    }

    console.log(
      `AUTO | Ejecutando ${signal} DEMO $${AMOUNT}`
    );

    await executeTrade(signal);

  } catch (e) {
    console.error(
      "AUTO | ERROR:",
      e.message
    );
  } finally {
    autoBusy = false;
  }
}

app.get("/", (req, res) => {
  res.json({
    bot: "EUR/USD Auto Trader",
    status: "online",
    mode: "DEMO",
    symbol: SYMBOL,
    amount: AMOUNT,
    duration: "5m",
    auto_trading: AUTO_TRADING,
    last_signal: lastSignal,
    last_auto_run: lastAutoRun,
    last_trade: lastTrade
  });
});

app.get("/account-test", async (req, res) => {
  try {
    const account =
      await getDemoAccount();

    res.json({
      success: true,
      mode: "DEMO",
      account_id: account
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

app.post("/trade", async (req, res) => {
  try {
    const signal =
      String(
        req.body.signal || ""
      ).toUpperCase();

    if (
      signal !== "CALL" &&
      signal !== "PUT"
    ) {
      return res.status(400).json({
        success: false,
        error: "Usa CALL o PUT"
      });
    }

    const result =
      await executeTrade(signal);

    res.json({
      success: true,
      mode: "DEMO",
      result
    });

  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

app.listen(PORT, () => {
  console.log(
    `Bot ejecutándose en puerto ${PORT}`
  );

  console.log(
    "================================"
  );

  console.log(
    "MODO: DEMO"
  );

  console.log(
    `EUR/USD: ${SYMBOL}`
  );

  console.log(
    `Monto: $${AMOUNT}`
  );

  console.log(
    "Duración: 5 minutos"
  );

  console.log(
    `Auto trading: ${AUTO_TRADING}`
  );

  console.log(
    "TOKEN:",
    Boolean(DERIV_TOKEN)
  );

  console.log(
    "APP ID:",
    Boolean(DERIV_APP_ID)
  );

  if (AUTO_TRADING) {
    console.log(
      "AUTO | Monitor iniciado"
    );

    setTimeout(
      runAutoTrader,
      5000
    );

    setInterval(
      runAutoTrader,
      5 * 60 * 1000
    );
  }
});
