const express = require("express");
const cors = require("cors");
const WebSocket = require("ws");

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

// =====================================================
// CONFIGURACIÓN
// =====================================================

const API_BASE = "https://api.derivws.com";

const DERIV_TOKEN = process.env.DERIV_TOKEN;
const DERIV_APP_ID = process.env.DERIV_APP_ID;
const DERIV_ACCOUNT_ID = process.env.DERIV_ACCOUNT_ID;

// DEMO únicamente
const SYMBOL = "frxEURUSD";
const AMOUNT = 1;
const DURATION = 5;
const DURATION_UNIT = "m";

// Seguridad del bot
const MAX_TRADES_PER_HOUR = 5;

let tradesThisHour = 0;
let hourStarted = Date.now();

let lastTradeCandle = null;

// =====================================================
// COMPROBAR CONFIGURACIÓN
// =====================================================

function checkConfig() {
    if (!DERIV_TOKEN) {
        throw new Error("Falta DERIV_TOKEN");
    }

    if (!DERIV_APP_ID) {
        throw new Error("Falta DERIV_APP_ID");
    }

    if (!DERIV_ACCOUNT_ID) {
        throw new Error("Falta DERIV_ACCOUNT_ID");
    }
}

// =====================================================
// CONTROL DE LÍMITE
// =====================================================

function resetHourlyCounter() {
    const now = Date.now();

    if (now - hourStarted >= 60 * 60 * 1000) {
        tradesThisHour = 0;
        hourStarted = now;
    }
}

function getFiveMinuteCandle() {
    return Math.floor(Date.now() / (5 * 60 * 1000));
}

// =====================================================
// OBTENER WEBSOCKET AUTENTICADO
// =====================================================

async function getWebSocketUrl() {

    checkConfig();

    const url =
        `${API_BASE}/trading/v1/options/accounts/` +
        `${encodeURIComponent(DERIV_ACCOUNT_ID)}/otp`;

    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${DERIV_TOKEN}`,
            "Deriv-App-ID": DERIV_APP_ID
        }
    });

    const data = await response.json();

    if (!response.ok) {
        throw new Error(
            data?.errors?.[0]?.message ||
            "No se pudo obtener el WebSocket de Deriv"
        );
    }

    if (!data.data || !data.data.url) {
        throw new Error("Deriv no devolvió una URL WebSocket válida");
    }

    return data.data.url;
}

// =====================================================
// EJECUTAR OPERACIÓN
// =====================================================

function executeTrade(direction) {

    return new Promise(async (resolve, reject) => {

        try {

            checkConfig();

            if (direction !== "CALL" && direction !== "PUT") {
                return reject(
                    new Error("La señal debe ser CALL o PUT")
                );
            }

            resetHourlyCounter();

            if (tradesThisHour >= MAX_TRADES_PER_HOUR) {
                return reject(
                    new Error(
                        "Límite de seguridad: 5 operaciones por hora"
                    )
                );
            }

            const candle = getFiveMinuteCandle();

            if (lastTradeCandle === candle) {
                return reject(
                    new Error(
                        "Ya se ejecutó una operación en esta vela de 5 minutos"
                    )
                );
            }

            const wsUrl = await getWebSocketUrl();

            const ws = new WebSocket(wsUrl);

            let finished = false;

            const timeout = setTimeout(() => {

                if (!finished) {

                    finished = true;

                    try {
                        ws.close();
                    } catch {}

                    reject(
                        new Error(
                            "Tiempo de espera agotado con Deriv"
                        )
                    );
                }

            }, 15000);

            ws.on("open", () => {

                console.log(
                    "WebSocket conectado. Solicitando propuesta..."
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

            ws.on("message", (raw) => {

                try {

                    const data = JSON.parse(raw.toString());

                    console.log(
                        "Deriv:",
                        JSON.stringify(data)
                    );

                    // -----------------------------------------
                    // ERROR DE DERIV
                    // -----------------------------------------

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

                    // -----------------------------------------
                    // PROPUESTA RECIBIDA
                    // -----------------------------------------

                    if (
                        data.msg_type === "proposal" &&
                        data.proposal
                    ) {

                        const proposalId =
                            data.proposal.id;

                        const askPrice =
                            Number(
                                data.proposal.ask_price
                            );

                        if (!proposalId || !askPrice) {

                            if (!finished) {

                                finished = true;

                                clearTimeout(timeout);

                                try {
                                    ws.close();
                                } catch {}

                                reject(
                                    new Error(
                                        "Propuesta inválida"
                                    )
                                );
                            }

                            return;
                        }

                        console.log(
                            "Propuesta recibida:",
                            proposalId
                        );

                        // -----------------------------------------
                        // COMPRAR
                        // -----------------------------------------

                        ws.send(
                            JSON.stringify({
                                buy: proposalId,
                                price: askPrice,
                                req_id: 2
                            })
                        );

                        return;
                    }

                    // -----------------------------------------
                    // COMPRA CONFIRMADA
                    // -----------------------------------------

                    if (
                        data.msg_type === "buy" &&
                        data.buy
                    ) {

                        const contractId =
                            data.buy.contract_id;

                        tradesThisHour++;
                        lastTradeCandle = candle;

                        if (!finished) {

                            finished = true;

                            clearTimeout(timeout);

                            try {
                                ws.close();
                            } catch {}

                            resolve({
                                success: true,
                                symbol: SYMBOL,
                                direction: direction,
                                amount: AMOUNT,
                                duration: `${DURATION} ${DURATION_UNIT}`,
                                contract_id: contractId,
                                trades_this_hour:
                                    tradesThisHour
                            });
                        }
                    }

                } catch (error) {

                    if (!finished) {

                        finished = true;

                        clearTimeout(timeout);

                        try {
                            ws.close();
                        } catch {}

                        reject(error);
                    }
                }
            });

            ws.on("error", (error) => {

                if (!finished) {

                    finished = true;

                    clearTimeout(timeout);

                    reject(error);
                }
            });

            ws.on("close", () => {

                console.log(
                    "WebSocket cerrado"
                );

            });

        } catch (error) {

            reject(error);
        }

    });
}

// =====================================================
// RUTA DE PRUEBA
// =====================================================

app.get("/", (req, res) => {

    res.json({
        bot: "EUR/USD Auto Trader",
        status: "online",
        mode: "DEMO",
        symbol: SYMBOL,
        amount: AMOUNT,
        duration: `${DURATION} ${DURATION_UNIT}`
    });

});

// =====================================================
// RUTA PARA EJECUTAR
// =====================================================

app.post("/trade", async (req, res) => {

    try {

        const signal = String(
            req.body.signal || ""
        ).toUpperCase();

        if (
            signal !== "CALL" &&
            signal !== "PUT"
        ) {

            return res.status(400).json({
                success: false,
                error:
                    "La señal debe ser CALL o PUT"
            });
        }

        console.log(
            `Señal recibida: ${signal}`
        );

        const result =
            await executeTrade(signal);

        res.json(result);

    } catch (error) {

        console.error(
            "ERROR:",
            error.message
        );

        res.status(500).json({
            success: false,
            error: error.message
        });
    }

});

// =====================================================
// SERVIDOR
// =====================================================

app.listen(PORT, () => {

    console.log(
        `Bot ejecutándose en puerto ${PORT}`
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
        `Duración: ${DURATION} minutos`
    );

});
