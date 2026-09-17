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

const AUTO_TRADING =
  process.env.AUTO_TRADING !== "false";

let tradesThisHour = 0;
let hourStarted = Date.now();

let lastTradeCandle = null;
let lastSignal = "NO_TRADE";
let lastAutoRun = null;
let lastTrade = null;

let autoBusy = false;

/* =========================
   CONFIGURACIÓN
========================= */

function checkConfig() {
  if (!DERIV_TOKEN) {
    throw new Error("Falta DERIV_TOKEN");
  }

  if (!DERIV_APP_ID) {
    throw new Error("Falta DERIV_APP_ID");
  }
}

/* =========================
   CONTROL DE OPERACIONES
========================= */

function resetHourlyCounter() {
  if (
    Date.now() - hourStarted >=
    60 * 60 * 1000
  ) {
    tradesThisHour = 0;
    hourStarted = Date.now();

    console.log(
      "AUTO | Contador horario reiniciado"
    );
  }
}

function getCandleId() {
  return Math.floor(
    Date.now() / (5 * 60 * 1000)
  );
}

/* =========================
   CUENTA DEMO
========================= */

async function getDemoAccount() {
  checkConfig();

  const response = await fetch(
    `${API_BASE}/trading/v1/options/accounts`,
    {
      method: "GET",
      headers: {
        Authorization:
          `Bearer ${DERIV_TOKEN}`,

        "Deriv-App-ID":
          DERIV_APP_ID,

        Accept:
          "application/json"
      }
    }
  );

  const data =
    await response.json();

  if (!response.ok) {
    throw new Error(
      data?.errors?.[0]?.message ||
      "No se pudieron obtener las cuentas"
    );
  }

  let accounts =
    data.data || [];

  if (!Array.isArray(accounts)) {
    accounts = [accounts];
  }

  const demo =
    accounts.find(
      account =>
        account.account_type ===
          "demo" &&
        account.status ===
          "active"
    );

  if (!demo?.account_id) {
    throw new Error(
      "No se encontró una cuenta DEMO activa"
    );
  }

  console.log(
    "Cuenta DEMO:",
    demo.account_id
  );

  return demo.account_id;
}

/* =========================
   WEBSOCKET AUTENTICADO
========================= */

async function getTradingWebSocket(
  accountId
) {
  checkConfig();

  const response =
    await fetch(
      `${API_BASE}/trading/v1/options/accounts/${encodeURIComponent(accountId)}/otp`,
      {
        method: "POST",

        headers: {
          Authorization:
            `Bearer ${DERIV_TOKEN}`,

          "Deriv-App-ID":
            DERIV_APP_ID,

          Accept:
            "application/json"
        }
      }
    );

  const data =
    await response.json();

  if (!response.ok) {
    throw new Error(
      data?.errors?.[0]?.message ||
      "No se pudo obtener el WebSocket"
    );
  }

  const url =
    data?.data?.url;

  if (!url) {
    throw new Error(
      "Deriv no devolvió una URL WebSocket"
    );
  }

  console.log(
    "WebSocket DEMO obtenido"
  );

  return url;
}

/* =========================
   DATOS EUR/USD
========================= */

function getCandles() {
  return new Promise(
    (resolve, reject) => {

      const ws =
        new WebSocket(
          `${API_BASE}/trading/v1/options/ws/public`
        );

      let finished = false;

      const timeout =
        setTimeout(() => {

          finish(
            reject,
            new Error(
              "Timeout obteniendo datos EUR/USD"
            )
          );

        }, 15000);

      function finish(
        callback,
        value
      ) {
        if (finished) return;

        finished = true;

        clearTimeout(timeout);

        try {
          ws.close();
        } catch {}

        callback(value);
      }

      ws.on("open", () => {

        console.log(
          "Mercado EUR/USD conectado"
        );

        /*
          IMPORTANTE:
          No usamos subscribe aquí.
          Es una petición única de historial.
        */

        const request = {
          ticks_history:
            SYMBOL,

          end:
            "latest",

          count:
            100,

          style:
            "candles",

          granularity:
            300,

          req_id:
            1
        };

        ws.send(
          JSON.stringify(request)
        );
      });

      ws.on(
        "message",
        raw => {

          try {

            const data =
              JSON.parse(
                raw.toString()
              );

            if (data.error) {

              return finish(
                reject,
                new Error(
                  data.error.message ||
                  "Error obteniendo mercado"
                )
              );
            }

            if (
              data.msg_type ===
                "candles" &&
              Array.isArray(
                data.candles
              )
            ) {

              const candles =
                data.candles
                  .map(
                    candle => ({
                      open:
                        Number(
                          candle.open
                        ),

                      high:
                        Number(
                          candle.high
                        ),

                      low:
                        Number(
                          candle.low
                        ),

                      close:
                        Number(
                          candle.close
                        ),

                      epoch:
                        Number(
                          candle.epoch
                        )
                    })
                  )
                  .filter(
                    candle =>
                      Number.isFinite(
                        candle.close
                      )
                  );

              if (
                candles.length <
                30
              ) {

                return finish(
                  reject,
                  new Error(
                    "No hay suficientes velas"
                  )
                );
              }

              console.log(
                `Mercado | ${candles.length} velas recibidas`
              );

              return finish(
                resolve,
                candles
              );
            }

          } catch (error) {

            finish(
              reject,
              error
            );
          }
        }
      );

      ws.on(
        "error",
        error => {

          finish(
            reject,
            error
          );
        }
      );
    }
  );
}

/* =========================
   EMA
========================= */

function ema(
  values,
  period
) {

  if (
    values.length <
    period
  ) {
    return null;
  }

  const multiplier =
    2 /
    (period + 1);

  let result =
    values
      .slice(
        0,
        period
      )
      .reduce(
        (sum, value) =>
          sum + value,
        0
      ) /
    period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {

    result =
      values[i] *
        multiplier +
      result *
        (1 - multiplier);
  }

  return result;
}

/* =========================
   RSI
========================= */

function rsi(
  values,
  period = 14
) {

  if (
    values.length <=
    period
  ) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {

    const change =
      values[i] -
      values[i - 1];

    if (
      change >= 0
    ) {
      gains += change;
    } else {
      losses +=
        Math.abs(change);
    }
  }

  let averageGain =
    gains / period;

  let averageLoss =
    losses / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {

    const change =
      values[i] -
      values[i - 1];

    const gain =
      Math.max(
        change,
        0
      );

    const loss =
      Math.max(
        -change,
        0
      );

    averageGain =
      (
        averageGain *
          (period - 1) +
        gain
      ) /
      period;

    averageLoss =
      (
        averageLoss *
          (period - 1) +
        loss
      ) /
      period;
  }

  if (
    averageLoss === 0
  ) {
    return 100;
  }

  const relativeStrength =
    averageGain /
    averageLoss;

  return (
    100 -
    100 /
      (
        1 +
        relativeStrength
      )
  );
}

/* =========================
   SEÑAL
========================= */

function getSignal(
  candles
) {

  const closes =
    candles.map(
      candle =>
        candle.close
    );

  const ema9 =
    ema(
      closes,
      9
    );

  const ema21 =
    ema(
      closes,
      21
    );

  const current =
    closes[
      closes.length - 1
    ];

  const previous =
    closes[
      closes.length - 2
    ];

  const currentRsi =
    rsi(
      closes,
      14
    );

  if (
    ![
      ema9,
      ema21,
      current,
      previous,
      currentRsi
    ].every(
      Number.isFinite
    )
  ) {
    return null;
  }

  /*
    CALL:
    EMA9 > EMA21
    RSI 50-70
    precio actual > anterior
  */

  if (
    ema9 > ema21 &&
    currentRsi >= 50 &&
    currentRsi <= 70 &&
    current > previous
  ) {
    return "CALL";
  }

  /*
    PUT:
    EMA9 < EMA21
    RSI 30-50
    precio actual < anterior
  */

  if (
    ema9 < ema21 &&
    currentRsi >= 30 &&
    currentRsi <= 50 &&
    current < previous
  ) {
    return "PUT";
  }

  return null;
}

/* =========================
   EJECUTAR OPERACIÓN DEMO
========================= */

function executeTrade(
  direction
) {

  return new Promise(
    async (
      resolve,
      reject
    ) => {

      let ws = null;

      try {

        checkConfig();

        resetHourlyCounter();

        if (
          tradesThisHour >=
          MAX_TRADES_PER_HOUR
        ) {

          throw new Error(
            "Límite de 5 operaciones por hora"
          );
        }

        const currentCandle =
          getCandleId();

        if (
          lastTradeCandle ===
          currentCandle
        ) {

          throw new Error(
            "Ya hubo una operación en esta vela"
          );
        }

        const accountId =
          await getDemoAccount();

        const wsUrl =
          await getTradingWebSocket(
            accountId
          );

        ws =
          new WebSocket(
            wsUrl
          );

        let finished =
          false;

        const timeout =
          setTimeout(() => {

            if (
              finished
            ) {
              return;
            }

            finished = true;

            try {
              ws.close();
            } catch {}

            reject(
              new Error(
                "Timeout con Deriv"
              )
            );

          }, 20000);

        function closeAndResolve(
          result
        ) {

          if (finished)
            return;

          finished = true;

          clearTimeout(
            timeout
          );

          try {
            ws.close();
          } catch {}

          resolve(result);
        }

        function closeAndReject(
          error
        ) {

          if (finished)
            return;

          finished = true;

          clearTimeout(
            timeout
          );

          try {
            ws.close();
          } catch {}

          reject(error);
        }

        ws.on(
          "open",
          () => {

            console.log(
              `DEMO | WebSocket conectado | ${direction}`
            );

            /*
              Propuesta CALL/PUT
            */

            const proposal = {

              proposal:
                1,

              amount:
                AMOUNT,

              basis:
                "stake",

              contract_type:
                direction,

              currency:
                "USD",

              duration:
                DURATION,

              duration_unit:
                DURATION_UNIT,

              underlying_symbol:
                SYMBOL,

              req_id:
                10
            };

            ws.send(
              JSON.stringify(
                proposal
              )
            );
          }
        );

        ws.on(
          "message",
          raw => {

            try {

              const data =
                JSON.parse(
                  raw.toString()
                );

              if (
                data.error
              ) {

                console.error(
                  "DEMO | Error:",
                  data.error.message
                );

                return closeAndReject(
                  new Error(
                    data.error.message ||
                    "Error de Deriv"
                  )
                );
              }

              /*
                PROPUESTA
              */

              if (
                data.msg_type ===
                  "proposal" &&
                data.proposal?.id
              ) {

                const proposalId =
                  data.proposal.id;

                const askPrice =
                  Number(
                    data.proposal.ask_price
                  );

                if (
                  !Number.isFinite(
                    askPrice
                  )
                ) {

                  return closeAndReject(
                    new Error(
                      "Precio de propuesta inválido"
                    )
                  );
                }

                console.log(
                  "DEMO | Propuesta recibida:",
                  proposalId
                );

                /*
                  COMPRA
                */

                ws.send(
                  JSON.stringify({
                    buy:
                      proposalId,

                    price:
                      askPrice,

                    req_id:
                      11
                  })
                );

                return;
              }

              /*
                COMPRA CONFIRMADA
              */

              if (
                data.msg_type ===
                  "buy" &&
                data.buy
              ) {

                tradesThisHour++;

                lastTradeCandle =
                  currentCandle;

                lastTrade = {

                  time:
                    new Date()
                      .toISOString(),

                  mode:
                    "DEMO",

                  direction:
                    direction,

                  amount:
                    AMOUNT,

                  duration:
                    `${DURATION}m`,

                  contract_id:
                    data.buy
                      .contract_id,

                  account_id:
                    accountId
                };

                console.log(
                  "================================"
                );

                console.log(
                  "OPERACIÓN DEMO CONFIRMADA"
                );

                console.log(
                  JSON.stringify(
                    lastTrade
                  )
                );

                console.log(
                  "================================"
                );

                return closeAndResolve(
                  lastTrade
                );
              }

            } catch (
              error
            ) {

              closeAndReject(
                error
              );
            }
          }
        );

        ws.on(
          "error",
          error => {

            closeAndReject(
              error
            );
          }
        );

      } catch (
        error
      ) {

        reject(
          error
        );
      }
    }
  );
}

/* =========================
   ANALIZADOR AUTOMÁTICO
========================= */

async function runAutoTrader() {

  if (
    !AUTO_TRADING
  ) {
    return;
  }

  if (
    autoBusy
  ) {
    console.log(
      "AUTO | Ya hay un análisis ejecutándose"
    );

    return;
  }

  autoBusy = true;

  lastAutoRun =
    new Date()
      .toISOString();

  try {

    console.log(
      "================================"
    );

    console.log(
      "AUTO | Analizando EUR/USD..."
    );

    resetHourlyCounter();

    const candles =
      await getCandles();

    const closes =
      candles.map(
        candle =>
          candle.close
      );

    const ema9 =
      ema(
        closes,
        9
      );

    const ema21 =
      ema(
        closes,
        21
      );

    const currentRsi =
      rsi(
        closes,
        14
      );

    const signal =
      getSignal(
        candles
      );

    lastSignal =
      signal ||
      "NO_TRADE";

    console.log(
      `AUTO | EMA9=${ema9?.toFixed(6)}`
    );

    console.log(
      `AUTO | EMA21=${ema21?.toFixed(6)}`
    );

    console.log(
      `AUTO | RSI14=${currentRsi?.toFixed(2)}`
    );

    console.log(
      `AUTO | SEÑAL=${lastSignal}`
    );

    if (
      !signal
    ) {

      console.log(
        "AUTO | Sin operación"
      );

      return;
    }

    console.log(
      `AUTO | Señal ${signal}`
    );

    console.log(
      `AUTO | Ejecutando DEMO $${AMOUNT}`
    );

    await executeTrade(
      signal
    );

  } catch (
    error
  ) {

    console.error(
      "AUTO | ERROR:",
      error.message
    );

  } finally {

    autoBusy =
      false;
  }
}

/* =========================
   ESTADO DEL BOT
========================= */

app.get(
  "/",
  (req, res) => {

    res.json({

      bot:
        "EUR/USD Auto Trader",

      status:
        "online",

      mode:
        "DEMO",

      symbol:
        SYMBOL,

      amount:
        AMOUNT,

      duration:
        "5m",

      auto_trading:
        AUTO_TRADING,

      trades_this_hour:
        tradesThisHour,

      max_trades_per_hour:
        MAX_TRADES_PER_HOUR,

      last_signal:
        lastSignal,

      last_auto_run:
        lastAutoRun,

      last_trade:
        lastTrade
    });
  }
);

/* =========================
   PRUEBA DE CUENTA
========================= */

app.get(
  "/account-test",
  async (
    req,
    res
  ) => {

    try {

      const account =
        await getDemoAccount();

      res.json({

        success:
          true,

        mode:
          "DEMO",

        account_id:
          account
      });

    } catch (
      error
    ) {

      res.status(
        500
      ).json({

        success:
          false,

        error:
          error.message
      });
    }
  }
);

/* =========================
   OPERACIÓN MANUAL DEMO
========================= */

app.post(
  "/trade",
  async (
    req,
    res
  ) => {

    try {

      const signal =
        String(
          req.body.signal ||
          ""
        ).toUpperCase();

      if (
        signal !== "CALL" &&
        signal !== "PUT"
      ) {

        return res
          .status(400)
          .json({

            success:
              false,

            error:
              "Usa CALL o PUT"
          });
      }

      const result =
        await executeTrade(
          signal
        );

      res.json({

        success:
          true,

        mode:
          "DEMO",

        result:
          result
      });

    } catch (
      error
    ) {

      res
        .status(500)
        .json({

          success:
            false,

          error:
            error.message
        });
    }
  }
);

/* =========================
   INICIO
========================= */

app.listen(
  PORT,
  () => {

    console.log(
      "================================"
    );

    console.log(
      "BOT EUR/USD INICIADO"
    );

    console.log(
      `Puerto: ${PORT}`
    );

    console.log(
      "MODO: DEMO"
    );

    console.log(
      `Símbolo: ${SYMBOL}`
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
      "DERIV TOKEN:",
      Boolean(
        DERIV_TOKEN
      )
    );

    console.log(
      "DERIV APP ID:",
      Boolean(
        DERIV_APP_ID
      )
    );

    console.log(
      "================================"
    );

    if (
      AUTO_TRADING
    ) {

      console.log(
        "AUTO | Monitor iniciado"
      );

      /*
        Primer análisis:
        5 segundos después
      */

      setTimeout(
        runAutoTrader,
        5000
      );

      /*
        Siguiente análisis:
        cada 5 minutos
      */

      setInterval(
        runAutoTrader,
        5 * 60 * 1000
      );
    }
  }
);
