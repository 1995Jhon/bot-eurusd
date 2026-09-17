const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

const API_BASE = "https://api.derivws.com";

const DERIV_TOKEN =
  process.env.DERIV_TOKEN;

const DERIV_APP_ID =
  process.env.DERIV_APP_ID;

/* =========================
   CONFIGURACIÓN
========================= */

const SYMBOL = "frxEURUSD";

const AMOUNT = 1;

/*
   El bot intentará estas duraciones
   automáticamente si una no está disponible.
*/
const DURATIONS = [
  1,
  2,
  3,
  5,
  10
];

const DURATION_UNIT = "m";

const MAX_TRADES_PER_HOUR = 5;

const AUTO_TRADING =
  process.env.AUTO_TRADING !== "false";

/* =========================
   ESTADO
========================= */

let tradesThisHour = 0;

let hourStarted =
  Date.now();

let lastTradeCandle =
  null;

let lastSignal =
  "NO_TRADE";

let lastAutoRun =
  null;

let lastTrade =
  null;

let autoBusy =
  false;

/* =========================
   CONFIGURACIÓN
========================= */

function checkConfig() {

  if (!DERIV_TOKEN) {
    throw new Error(
      "Falta DERIV_TOKEN"
    );
  }

  if (!DERIV_APP_ID) {
    throw new Error(
      "Falta DERIV_APP_ID"
    );
  }
}

/* =========================
   CONTADOR HORARIO
========================= */

function resetHourlyCounter() {

  const hour =
    60 * 60 * 1000;

  if (
    Date.now() -
      hourStarted >=
    hour
  ) {

    tradesThisHour = 0;

    hourStarted =
      Date.now();

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
      (
        5 *
        60 *
        1000
      )
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

  if (
    !Array.isArray(accounts)
  ) {

    accounts = [
      accounts
    ];
  }

  const demo =
    accounts.find(
      account =>
        account.account_type ===
          "demo" &&
        account.status ===
          "active"
    );

  if (
    !demo?.account_id
  ) {

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
      "No se pudo obtener WebSocket"
    );
  }

  const url =
    data?.data?.url;

  if (!url) {

    throw new Error(
      "Deriv no devolvió URL WebSocket"
    );
  }

  console.log(
    "WebSocket DEMO obtenido"
  );

  return url;
}

/* =========================
   VELAS EUR/USD
========================= */

function getCandles() {

  return new Promise(
    (resolve, reject) => {

      const ws =
        new WebSocket(
          `${API_BASE}/trading/v1/options/ws/public`
        );

      let finished =
        false;

      const timeout =
        setTimeout(
          () => {

            finish(
              reject,
              new Error(
                "Timeout obteniendo EUR/USD"
              )
            );

          },
          15000
        );

      function finish(
        callback,
        value
      ) {

        if (finished) {
          return;
        }

        finished = true;

        clearTimeout(
          timeout
        );

        try {
          ws.close();
        } catch {}

        callback(
          value
        );
      }

      ws.on(
        "open",
        () => {

          console.log(
            "Mercado EUR/USD conectado"
          );

          /*
             IMPORTANTE:
             No usamos subscribe.
             Solo pedimos historial.
          */

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
                1
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

              return finish(
                reject,
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

          } catch (
            error
          ) {

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
    (
      period +
      1
    );

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
          sum +
          value,
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
        (
          1 -
          multiplier
        );
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

      gains +=
        change;

    } else {

      losses +=
        Math.abs(
          change
        );
    }
  }

  let averageGain =
    gains /
    period;

  let averageLoss =
    losses /
    period;

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
          (
            period -
            1
          ) +
        gain
      ) /
      period;

    averageLoss =
      (
        averageLoss *
          (
            period -
            1
          ) +
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
      (
        1 +
        rs
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
      closes.length -
      1
    ];

  const previous =
    closes[
      closes.length -
      2
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
     CALL
  */

  if (
    ema9 >
      ema21 &&
    currentRsi >=
      50 &&
    currentRsi <=
      70 &&
    current >
      previous
  ) {

    return "CALL";
  }

  /*
     PUT
  */

  if (
    ema9 <
      ema21 &&
    currentRsi >=
      30 &&
    currentRsi <=
      50 &&
    current <
      previous
  ) {

    return "PUT";
  }

  return null;
}

/* =========================
   EJECUTAR TRADE
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

        let durationIndex =
          0;

        let selectedDuration =
          null;

        const timeout =
          setTimeout(
            () => {

              if (
                finished
              ) {
                return;
              }

              finished =
                true;

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

        function closeResolve(
          result
        ) {

          if (
            finished
          ) {
            return;
          }

          finished =
            true;

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

        function closeReject(
          error
        ) {

          if (
            finished
          ) {
            return;
          }

          finished =
            true;

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

        function sendProposal() {

          if (
            durationIndex >=
            DURATIONS.length
          ) {

            return closeReject(
              new Error(
                "No hay una duración disponible para este contrato en este momento"
              )
            );
          }

          selectedDuration =
            DURATIONS[
              durationIndex
            ];

          console.log(
            `DEMO | Probando duración ${selectedDuration} minutos`
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
                selectedDuration,

              duration_unit:
                DURATION_UNIT,

              underlying_symbol:
                SYMBOL,

              req_id:
                100 +
                durationIndex
            })
          );
        }

        ws.on(
          "open",
          () => {

            console.log(
              `DEMO | WebSocket conectado | ${direction}`
            );

            sendProposal();
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

              /*
                 ERROR DE DERIV
              */

              if (
                data.error
              ) {

                const message =
                  data.error.message ||
                  "Error de Deriv";

                console.error(
                  "DEMO | Error:",
                  message
                );

                /*
                   Si la duración no está disponible,
                   probamos automáticamente la siguiente.
                */

                if (
                  message
                    .toLowerCase()
                    .includes(
                      "not offered for this duration"
                    )
                ) {

                  durationIndex++;

                  console.log(
                    `DEMO | Duración ${selectedDuration} no disponible`
                  );

                  console.log(
                    "DEMO | Probando siguiente duración..."
                  );

                  sendProposal();

                  return;
                }

                return closeReject(
                  new Error(
                    message
                  )
                );
              }

              /*
                 PROPUESTA RECIBIDA
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

                  return closeReject(
                    new Error(
                      "Precio de propuesta inválido"
                    )
                  );
                }

                console.log(
                  "DEMO | Propuesta recibida:",
                  proposalId
                );

                console.log(
                  `DEMO | Duración aceptada: ${selectedDuration} minutos`
                );

                /*
                   COMPRAR
                */

                ws.send(
                  JSON.stringify({

                    buy:
                      proposalId,

                    price:
                      askPrice,

                    req_id:
                      200
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
                    `${selectedDuration}m`,

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
                  `DIRECCIÓN: ${direction}`
                );

                console.log(
                  `DURACIÓN: ${selectedDuration} minutos`
                );

                console.log(
                  `MONTO: $${AMOUNT}`
                );

                console.log(
                  `CONTRATO: ${data.buy.contract_id}`
                );

                console.log(
                  "================================"
                );

                return closeResolve(
                  lastTrade
                );
              }

            } catch (
              error
            ) {

              closeReject(
                error
              );
            }
          }
        );

        ws.on(
          "error",
          error => {

            closeReject(
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
   TRADER AUTOMÁTICO
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

  autoBusy =
    true;

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
   ESTADO
========================= */

app.get(
  "/",
  (
    req,
    res
  ) => {

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

      duration_mode:
        "AUTO",

      available_durations:
        DURATIONS,

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
   TRADE MANUAL DEMO
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
        signal !==
          "CALL" &&
        signal !==
          "PUT"
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
   ARRANQUE
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
      "Duración: AUTOMÁTICA"
    );

    console.log(
      `Duraciones: ${DURATIONS.join(", ")} minutos`
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
         Primer análisis
      */

      setTimeout(
        runAutoTrader,
        5000
      );

      /*
         Análisis cada 5 minutos
      */

      setInterval(
        runAutoTrader,
        5 *
        60 *
        1000
      );
    }
  }
);
