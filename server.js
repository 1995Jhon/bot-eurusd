const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

const API_BASE = "https://api.derivws.com";

const PUBLIC_WS =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const SYMBOL = "frxEURUSD";
const AMOUNT = 1;

// DEMO solamente
const MAX_TRADES_PER_HOUR = 5;

// Para esta prueba usamos 5 TICKS.
// Evitamos las duraciones en minutos que Deriv estaba rechazando.
const DURATION = 5;
const DURATION_UNIT = "t";

const DERIV_TOKEN =
  process.env.DERIV_TOKEN;

const DERIV_APP_ID =
  process.env.DERIV_APP_ID;

const AUTO_TRADING =
  process.env.AUTO_TRADING !== "false";

let tradesThisHour = 0;
let hourStarted = Date.now();

let lastTradeCandle = null;
let autoBusy = false;

let lastSignal = "NO_TRADE";
let lastTrade = null;
let lastAutoRun = null;

/* =========================
   CONFIG
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
   CONTADOR
========================= */

function resetHourlyCounter() {
  const hour = 60 * 60 * 1000;

  if (
    Date.now() - hourStarted >=
    hour
  ) {
    tradesThisHour = 0;
    hourStarted = Date.now();

    console.log(
      "AUTO | Contador horario reiniciado"
    );
  }
}

/* =========================
   IDENTIFICADOR DE VELA
========================= */

function getCandleId() {
  return Math.floor(
    Date.now() /
      (5 * 60 * 1000)
  );
}

/* =========================
   CUENTA DEMO
========================= */

async function getDemoAccount() {
  checkConfig();

  const response =
    await fetch(
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
    `Cuenta DEMO: ${demo.account_id}`
  );

  return demo.account_id;
}

/* =========================
   OTP PARA WEBSOCKET DEMO
========================= */

async function getDemoWebSocket(
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
      "No se pudo obtener OTP"
    );
  }

  const url =
    data?.data?.url;

  if (!url) {
    throw new Error(
      "Deriv no devolvió la URL DEMO"
    );
  }

  console.log(
    "WebSocket DEMO obtenido"
  );

  return url;
}

/* =========================
   CONTRATOS DISPONIBLES
========================= */

function checkContracts() {
  return new Promise(
    (resolve, reject) => {

      const ws =
        new WebSocket(
          PUBLIC_WS
        );

      let finished = false;

      const timeout =
        setTimeout(
          () => {
            finishReject(
              new Error(
                "Timeout consultando contratos"
              )
            );
          },
          15000
        );

      function finishResolve(
        value
      ) {
        if (finished) return;

        finished = true;

        clearTimeout(
          timeout
        );

        try {
          ws.close();
        } catch {}

        resolve(value);
      }

      function finishReject(
        error
      ) {
        if (finished) return;

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
            "Mercado | Consultando contratos EUR/USD..."
          );

          ws.send(
            JSON.stringify({
              contracts_for:
                SYMBOL,

              req_id:
                10
            })
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

            if (data.error) {
              return finishReject(
                new Error(
                  data.error.message ||
                  "Error consultando contratos"
                )
              );
            }

            if (
              data.msg_type ===
              "contracts_for"
            ) {

              const available =
                data
                  .contracts_for
                  ?.available || [];

              const types =
                available
                  .map(
                    item =>
                      item.contract_type
                  )
                  .filter(Boolean);

              console.log(
                `Mercado | Contratos: ${types.join(", ")}`
              );

              const call =
                available.some(
                  item =>
                    item.contract_type ===
                    "CALL"
                );

              const put =
                available.some(
                  item =>
                    item.contract_type ===
                    "PUT"
                );

              finishResolve({
                call,
                put
              });
            }

          } catch (error) {
            finishReject(error);
          }
        }
      );

      ws.on(
        "error",
        error => {
          finishReject(error);
        }
      );
    }
  );
}

/* =========================
   VELAS
========================= */

function getCandles() {
  return new Promise(
    (resolve, reject) => {

      const ws =
        new WebSocket(
          PUBLIC_WS
        );

      let finished = false;

      const timeout =
        setTimeout(
          () => {
            finishReject(
              new Error(
                "Timeout obteniendo EUR/USD"
              )
            );
          },
          15000
        );

      function finishResolve(
        value
      ) {
        if (finished) return;

        finished = true;

        clearTimeout(
          timeout
        );

        try {
          ws.close();
        } catch {}

        resolve(value);
      }

      function finishReject(
        error
      ) {
        if (finished) return;

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
            "Mercado EUR/USD conectado"
          );

          ws.send(
            JSON.stringify({
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
                20
            })
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

            if (data.error) {
              return finishReject(
                new Error(
                  data.error.message ||
                  "Error de mercado"
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
                return finishReject(
                  new Error(
                    "No hay suficientes velas"
                  )
                );
              }

              console.log(
                `Mercado | ${candles.length} velas recibidas`
              );

              finishResolve(
                candles
              );
            }

          } catch (error) {
            finishReject(error);
          }
        }
      );

      ws.on(
        "error",
        error => {
          finishReject(error);
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
        (
          sum,
          value
        ) =>
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
    let i =
      period + 1;
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
    averageLoss ===
    0
  ) {
    return 100;
  }

  const rs =
    averageGain /
    averageLoss;

  return (
    100 -
    100 /
      (1 + rs)
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

  console.log(
    `AUTO | EMA9=${ema9?.toFixed(6)}`
  );

  console.log(
    `AUTO | EMA21=${ema21?.toFixed(6)}`
  );

  console.log(
    `AUTO | RSI14=${currentRsi?.toFixed(2)}`
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

  if (
    ema9 > ema21 &&
    currentRsi >= 50 &&
    currentRsi <= 70 &&
    current > previous
  ) {
    return "CALL";
  }

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
   OPERACIÓN DEMO
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

        resetHourlyCounter();

        if (
          tradesThisHour >=
          MAX_TRADES_PER_HOUR
        ) {
          throw new Error(
            "Límite de 5 operaciones por hora"
          );
        }

        const candleId =
          getCandleId();

        if (
          lastTradeCandle ===
          candleId
        ) {
          throw new Error(
            "Ya hubo una operación en esta vela"
          );
        }

        console.log(
          "DEMO | Verificando CALL/PUT..."
        );

        const contracts =
          await checkContracts();

        if (
          direction ===
            "CALL" &&
          !contracts.call
        ) {
          throw new Error(
            "CALL no está disponible para EUR/USD"
          );
        }

        if (
          direction ===
            "PUT" &&
          !contracts.put
        ) {
          throw new Error(
            "PUT no está disponible para EUR/USD"
          );
        }

        const accountId =
          await getDemoAccount();

        const wsUrl =
          await getDemoWebSocket(
            accountId
          );

        ws =
          new WebSocket(
            wsUrl
          );

        let finished =
          false;

        const timeout =
          setTimeout(
            () => {

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

            },
            30000
          );

        function finishResolve(
          result
        ) {

          if (
            finished
          ) {
            return;
          }

          finished = true;

          clearTimeout(
            timeout
          );

          try {
            ws.close();
          } catch {}

          resolve(
            result
          );
        }

        function finishReject(
          error
        ) {

          if (
            finished
          ) {
            return;
          }

          finished = true;

          clearTimeout(
            timeout
          );

          try {
            ws.close();
          } catch {}

          reject(
            error
          );
        }

        ws.on(
          "open",
          () => {

            console.log(
              `DEMO | WebSocket conectado | ${direction}`
            );

            console.log(
              `DEMO | Solicitando propuesta: ${DURATION} ticks`
            );

            ws.send(
              JSON.stringify({

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

                subscribe:
                  1,

                req_id:
                  50

              })
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

                return finishReject(
                  new Error(
                    data.error.message ||
                    "Error de Deriv"
                  )
                );
              }

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
                  return finishReject(
                    new Error(
                      "Deriv no devolvió un precio válido"
                    )
                  );
                }

                console.log(
                  "DEMO | PROPUESTA RECIBIDA"
                );

                console.log(
                  `DEMO | Precio: $${askPrice}`
                );

                console.log(
                  "DEMO | Comprando propuesta..."
                );

                ws.send(
                  JSON.stringify({

                    buy:
                      proposalId,

                    price:
                      askPrice,

                    req_id:
                      51

                  })
                );

                return;
              }

              if (
                data.msg_type ===
                  "buy" &&
                data.buy?.contract_id
              ) {

                const contractId =
                  data.buy.contract_id;

                tradesThisHour++;

                lastTradeCandle =
                  getCandleId();

                lastTrade = {

                  time:
                    new Date()
                      .toISOString(),

                  direction:
                    direction,

                  amount:
                    AMOUNT,

                  duration:
                    DURATION,

                  duration_unit:
                    DURATION_UNIT,

                  contract_id:
                    contractId,

                  account:
                    accountId,

                  status:
                    "DEMO_CONFIRMED"

                };

                console.log(
                  "================================"
                );

                console.log(
                  "OPERACIÓN DEMO CONFIRMADA"
                );

                console.log(
                  `DEMO | Dirección: ${direction}`
                );

                console.log(
                  `DEMO | Monto: $${AMOUNT}`
                );

                console.log(
                  `DEMO | Duración: ${DURATION} ticks`
                );

                console.log(
                  `DEMO | Contract ID: ${contractId}`
                );

                console.log(
                  `DEMO | Operaciones: ${tradesThisHour}/${MAX_TRADES_PER_HOUR}`
                );

                console.log(
                  "================================"
                );

                return finishResolve(
                  lastTrade
                );
              }

            } catch (error) {

              finishReject(
                error
              );
            }
          }
        );

        ws.on(
          "error",
          error => {

            finishReject(
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
   BOT AUTOMÁTICO
========================= */

async function runAutoTrader() {

  if (
    !AUTO_TRADING
  ) {

    console.log(
      "AUTO | Trading automático desactivado"
    );

    return;
  }

  if (
    autoBusy
  ) {

    console.log(
      "AUTO | Análisis anterior activo"
    );

    return;
  }

  autoBusy = true;

  lastAutoRun =
    new Date()
      .toISOString();

  try {

    resetHourlyCounter();

    console.log(
      "================================"
    );

    console.log(
      "AUTO | Analizando EUR/USD..."
    );

    const candles =
      await getCandles();

    const signal =
      getSignal(
        candles
      );

    lastSignal =
      signal ||
      "NO_TRADE";

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
      "AUTO | Ejecutando DEMO $1"
    );

    await executeTrade(
      signal
    );

  } catch (
    error
  ) {

    console.log(
      `AUTO | ERROR: ${error.message}`
    );

  } finally {

    autoBusy = false;

    console.log(
      "================================"
    );
  }
}

/* =========================
   PÁGINA
========================= */

app.get(
  "/",
  (req, res) => {

    res.json({

      bot:
        "EUR/USD DEMO",

      status:
        "running",

      mode:
        "DEMO",

      symbol:
        SYMBOL,

      amount:
        AMOUNT,

      duration:
        DURATION,

      duration_unit:
        DURATION_UNIT,

      auto_trading:
        AUTO_TRADING,

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
   TEST CUENTA
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

        ok:
          true,

        mode:
          "DEMO",

        account:
          account

      });

    } catch (
      error
    ) {

      res.status(
        500
      ).json({

        ok:
          false,

        error:
          error.message

      });
    }
  }
);

/* =========================
   TRADE MANUAL DEMO
========================= */

app.post(
  "/trade",
  async (
    req,
    res
  ) => {

    try {

      const direction =
        String(
          req.body?.direction ||
          ""
        ).toUpperCase();

      if (
        direction !==
          "CALL" &&
        direction !==
          "PUT"
      ) {

        return res.status(
          400
        ).json({

          ok:
            false,

          error:
            "direction debe ser CALL o PUT"

        });
      }

      const result =
        await executeTrade(
          direction
        );

      res.json({

        ok:
          true,

        result:
          result

      });

    } catch (
      error
    ) {

      res.status(
        500
      ).json({

        ok:
          false,

        error:
          error.message

      });
    }
  }
);

/* =========================
   SERVIDOR
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
      "Modo: DEMO"
    );

    console.log(
      `Símbolo: ${SYMBOL}`
    );

    console.log(
      `Monto: $${AMOUNT}`
    );

    console.log(
      `Duración: ${DURATION} ticks`
    );

    console.log(
      `Máximo: ${MAX_TRADES_PER_HOUR} operaciones/hora`
    );

    console.log(
      `DERIV_TOKEN configurado: ${Boolean(DERIV_TOKEN)}`
    );

    console.log(
      `DERIV_APP_ID configurado: ${Boolean(DERIV_APP_ID)}`
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

      runAutoTrader();

      setInterval(
        runAutoTrader,
        5 * 60 * 1000
      );
    }
  }
);
