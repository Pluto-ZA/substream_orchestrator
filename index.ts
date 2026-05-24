import type { Server } from 'node:http';
import express from 'express';
import dotenv from 'dotenv';
import { readPackageFromFile } from '@substreams/manifest';
import { applyParams, createRegistry, createRequest, streamBlocks } from '@substreams/core';
import { createConnectTransport } from '@connectrpc/connect-node';
import { createClient } from '@clickhouse/client';
import { Code, ConnectError } from '@connectrpc/connect';
// @ts-ignore
BigInt.prototype.toJSON = function () { return this.toString(); };

dotenv.config();

// --- CONFIGURATION ---
const PORT = 3000;
const SPKG_PATH = "./substream.spkg";
const SUBSTREAMS_ENDPOINT = "mainnet.sol.streamingfast.io:443";
const API_TOKEN = process.env.SUBSTREAMS_API_TOKEN;
const OUTPUT_MODULE = "map_balance_changes";
const TABLE_WATCHLIST = "solana.watched_wallets";
const TABLE_CURSORS = "solana.cursors_node";
const EXIT_ON_STREAM_ERROR = process.env.EXIT_ON_STREAM_ERROR === "true";
const STREAM_RESTART_DELAY_MS = 30_000;
const RECONNECT_DELAY_MS = 1_000;
const RETRY_DELAY_MS = 3_000;
const SHUTDOWN_TIMEOUT_MS = 5_000;

// --- CLICKHOUSE SETUP ---
const clickhouse = createClient({
    url: process.env.CLICKHOUSE_URL!,
    username: 'default',
    password: process.env.CLICKHOUSE_PASSWORD!,
    database: 'solana',
    
    request_timeout: 60_000,
    // 3. Disable keep_alive to prevent reusing dead sockets
    keep_alive: {
        enabled: false,
    }
});

// --- STATE MANAGEMENT ---
let activeStreamController: AbortController | null = null;
let pendingRestartTimer: ReturnType<typeof setTimeout> | null = null;
let pendingRestartDelayMs: number | null = null;
let server: Server | null = null;
let shuttingDown = false;

const app = express();
app.use(express.json());

function clearPendingRestart(): void {
    if (!pendingRestartTimer) {
        return;
    }
    clearTimeout(pendingRestartTimer);
    pendingRestartTimer = null;
}

function stopActiveStream(): void {
    if (!activeStreamController || activeStreamController.signal.aborted) {
        return;
    }
    activeStreamController.abort();
}

function scheduleSubstreamStart(delayMs: number): void {
    if (shuttingDown) {
        return;
    }
    clearPendingRestart();
    pendingRestartTimer = setTimeout(() => {
        pendingRestartTimer = null;
        if (activeStreamController) {
            console.log("[Orchestrator] Skipping scheduled start because a stream is still closing.");
            return;
        }
        void startSubstream();
    }, delayMs);
    pendingRestartTimer.unref?.();
}

function throwIfStreamStopRequested(signal: AbortSignal): void {
    if (!signal.aborted) {
        return;
    }
    throw new ConnectError("stream stop requested", Code.Canceled);
}

function closeHttpServer(): Promise<void> {
    if (!server) {
        return Promise.resolve();
    }
    const currentServer = server;
    server = null;
    return new Promise((resolve, reject) => {
        currentServer.close((closeErr) => {
            if (closeErr) {
                reject(closeErr);
                return;
            }
            resolve();
        });
        currentServer.closeAllConnections?.();
    });
}

function shutdownProcess(reason: string, exitCode: number, err?: unknown): void {
    if (shuttingDown) {
        console.error(`[Process] Shutdown already in progress. Forcing exit ${exitCode}.`);
        process.exit(exitCode);
    }

    shuttingDown = true;
    console.error(reason);
    if (err) {
        console.error(err);
    }

    clearPendingRestart();
    pendingRestartDelayMs = null;
    stopActiveStream();

    const forceExitTimer = setTimeout(() => {
        console.error(`[Process] Force exiting after ${SHUTDOWN_TIMEOUT_MS}ms.`);
        process.exit(exitCode);
    }, SHUTDOWN_TIMEOUT_MS);
    forceExitTimer.unref?.();

    void Promise.allSettled([
        closeHttpServer(),
        clickhouse.close(),
    ]).then((results) => {
        for (const result of results) {
            if (result.status === "rejected") {
                console.error("[Process] Cleanup error:", result.reason);
            }
        }
    }).finally(() => {
        clearTimeout(forceExitTimer);
        process.exit(exitCode);
    });
}

function exitForDockerRestart(reason: string, err?: unknown): void {
    shutdownProcess(reason, 1, err);
}

function exitGracefully(reason: string): void {
    shutdownProcess(reason, 0);
}

process.on("uncaughtException", (err) => {
    exitForDockerRestart("[Process] Uncaught exception. Exiting so Docker can restart the container.", err);
});

process.on("unhandledRejection", (reason) => {
    exitForDockerRestart("[Process] Unhandled rejection. Exiting so Docker can restart the container.", reason);
});

process.on("SIGTERM", () => {
    exitGracefully("[Process] SIGTERM received. Shutting down.");
});

process.on("SIGINT", () => {
    exitGracefully("[Process] SIGINT received. Shutting down.");
});

async function fetchWhitelist(): Promise<string[]> {
    try {
        const result = await clickhouse.query({
            query: `SELECT DISTINCT address FROM ${TABLE_WATCHLIST}`,
            format: "JSONEachRow"
        });
        const rows = await result.json();
        return rows.map((r: any) => r.address).filter((a: string) => !!a);
    } catch (err) {
        console.error("[DB] Failed to fetch whitelist:", err);
        return [];
    }
}
// ==========================================
//               HELPER FUNCTIONS
// ==========================================

// Helper to handle the stream restart logic safely
function triggerStreamRestart(delayMs = STREAM_RESTART_DELAY_MS) {
    if (shuttingDown) {
        return;
    }
    clearPendingRestart();
    if (activeStreamController) {
        pendingRestartDelayMs = delayMs;
        console.log(`[Orchestrator] Watchlist updated. Closing active stream before restarting in ${delayMs}ms...`);
        stopActiveStream();
    } else {
        pendingRestartDelayMs = null;
        console.log("[Orchestrator] Watchlist updated. Starting stream...");
        scheduleSubstreamStart(0);
    }
}


// Helper to normalize input (accepts string or array of strings)
function normalizeInput(input: any): string[] {
    if (typeof input === "string") return [input];
    if (Array.isArray(input)) return input;
    return [];
}

// ==========================================
//                  ROUTES
// ==========================================

app.get('/current-block', async (req, res) => {
    try {
        const resultSet = await clickhouse.query({
            query: `SELECT max(block_num) as current_block FROM ${TABLE_CURSORS}`,
            format: 'JSONEachRow',
        });
        
        const rows = await resultSet.json();
        // @ts-ignore
        const currentBlock = rows[0]?.current_block || 0;
        
        res.json({
            status: 'ok',
            current_block: currentBlock,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error("Failed to fetch block:", error);
        // @ts-ignore
        res.status(500).json({ error: error.message });
    }
});

app.post("/restart", async (req, res) => {
    try {
        triggerStreamRestart();
        res.json({status: "success"});
    } catch (e) {
        console.error(e);
        res.status(500)
    }
})

app.post("/add-address", async (req, res) => {
    const { address, addresses } = req.body;
    const list = normalizeInput(address || addresses);
    
    if (list.length === 0) return res.status(400).json({ error: "Invalid input" });
    
    try {
        // Insert into ClickHouse
        await clickhouse.insert({
            table: TABLE_WATCHLIST,
            values: list.map(addr => ({ address: addr })),
            format: 'JSONEachRow'
        });
        
        console.log(`[API] Added ${list.length} wallets to DB.`);
        
        // Restart stream to pick up changes
        triggerStreamRestart();
        
        res.json({ status: "success", added: list.length });
    } catch (error: any) {
        console.error("DB Error:", error);
        res.status(500).json({ error: "Database insert failed" });
    }
});

app.post("/delete-address", async (req, res) => {
    const { address, addresses } = req.body;
    const list = normalizeInput(address || addresses);
    
    if (list.length === 0) return res.status(400).json({ error: "Invalid input" });
    
    try {
        // ClickHouse Mutation to remove
        const listStr = list.map(a => `'${a}'`).join(',');
        await clickhouse.command({
            query: `ALTER TABLE ${TABLE_WATCHLIST} DELETE WHERE address IN (${listStr})`
        });
        
        console.log(`[API] Removed ${list.length} wallets from DB.`);
        
        // Restart stream to pick up changes
        triggerStreamRestart();
        
        res.json({ status: "success", removed: list.length });
    } catch (error: any) {
        console.error("DB Error:", error);
        res.status(500).json({ error: "Database delete failed" });
    }
});

app.get("/list-addresses", async (req, res) => {
    const list = await fetchWhitelist();
    res.json({ count: list.length, addresses: list });
});

// ==========================================
//             SUBSTREAM LOGIC
// ==========================================

async function startSubstream() {
    if (shuttingDown) {
        return;
    }
    if (activeStreamController) {
        console.log("[Orchestrator] Start skipped because a stream is already active.");
        return;
    }

    const controller = new AbortController();
    activeStreamController = controller;
    const signal = controller.signal;

    try {
        const addresses = await fetchWhitelist();
        throwIfStreamStopRequested(signal);

        if (addresses.length === 0) {
            console.log("[Orchestrator] DB Watchlist is empty. Stream idling...");
            return;
        }

        const paramsString = addresses.join(",");
        console.log(`[Orchestrator] Starting stream with ${addresses.length} whitelisted addresses...`);

        const substreamPackage = await readPackageFromFile(SPKG_PATH);
        throwIfStreamStopRequested(signal);
        
        const paramUpdates = [
            `map_balance_changes=${paramsString}`
        ];
        
        applyParams(paramUpdates, substreamPackage.modules!.modules);
        
        const registry = createRegistry(substreamPackage);
        
        const transport = createConnectTransport({
            baseUrl: `https://${SUBSTREAMS_ENDPOINT}`,
            httpVersion: "2",
            pingIntervalMs: 30_000,
            pingTimeoutMs: 10_000,
            interceptors: [
                (next) => async (req) => {
                    req.header.set("Authorization", `Bearer ${API_TOKEN}`);
                    return next(req);
                },
            ],
        });
        
        // 2. CURSOR HANDLING
        const cursorRes = await clickhouse.query({
            query: `SELECT cursor FROM ${TABLE_CURSORS} ORDER BY block_num DESC LIMIT 1`,
            format: "JSONEachRow"
        });
        const rows = await cursorRes.json();
        throwIfStreamStopRequested(signal);
        // @ts-ignore
        const startCursor = rows.length > 0 ? rows[0].cursor : undefined;
        
        const request = createRequest({
            substreamPackage,
            outputModule: OUTPUT_MODULE,
            productionMode: true,
            startBlockNum: 410335511,// 379226927, // pluto start block
            startCursor: startCursor
        });
        
        // @ts-expect-error Runtime is compatible; package typings disagree on ESM/CJS transport shapes.
        const stream = streamBlocks(transport, request, { signal });
        
        console.log("[Orchestrator] Stream connected.");
        
        let lastKnownCursor = startCursor;
        let lastLogTime = Date.now();
        
        for await (const response of stream) {
            // --- CASE 1: PROGRESS (Heartbeat) ---
            if (response.message.case === "progress") {
                const prog = response.message.value;
                const stages = prog.stages || [];
                
                let highestBlock = 0n;
                
                for (const stage of stages) {
                    for (const range of stage.completedRanges) {
                        if (range.endBlock > highestBlock) {
                            highestBlock = range.endBlock;
                        }
                    }
                }
                
                if (Date.now() - lastLogTime > 60000 && highestBlock > 0n) {
                    console.log(`[Sync] Scanned up to block ${highestBlock} (Searching for transactions...)`);
                    lastLogTime = Date.now();
                }
            }
            
            // --- CASE 2: REAL DATA ---
            if (response.message.case === "blockScopedData") {
                const output = response.message.value.output;
                const cursor = response.message.value.cursor;
                const clock = response.message.value.clock;
                
                lastKnownCursor = cursor;
                
                if (output && output.mapOutput) {
                    const decodedData = output.mapOutput.unpack(registry);
                    const changes = (decodedData as any).params;
                    
                    if (changes && Array.isArray(changes) && changes.length > 0) {
                        console.log(`✅ [Data] Block ${clock?.number}: Inserting ${changes.length} rows`);
                        
                        try {
                            await clickhouse.insert({
                                table: 'wallet_balance_changes',
                                values: changes.map((row: any) => ({
                                    id: `${row.txId}:${row.owner}:${row.mint}`,
                                    block_time: row.blockTime,
                                    block_slot: row.blockSlot,
                                    tx_id: row.txId,
                                    owner: row.owner,
                                    mint: row.mint,
                                    change_amount: parseFloat(row.changeAmount),
                                    new_balance: parseFloat(row.newBalance),
                                    decimals: Number(row.decimals),
                                    change_type: row.changeType || 'UNKNOWN',
                                    network_fee: parseFloat(row.networkFee),
                                })),
                                format: 'JSONEachRow'
                            });
                            console.log(`✨ [DB] Successfully committed block ${clock?.number}`);
                        } catch (dbError) {
                            console.error("❌ [DB Error] Insert failed:", dbError);
                            // CRITICAL FIX: Throw the error so we exit the loop and do NOT update the cursor.
                            // This triggers the outer catch block which restarts the stream.
                            throw dbError;
                        }
                    }
                }
                
                await clickhouse.command({
                    query: `INSERT INTO ${TABLE_CURSORS} (id, cursor, block_num) VALUES (1, '${cursor}', ${clock?.number})`
                });
            }
        }
    } catch (err: any) {
        if (signal.aborted || (err instanceof ConnectError && err.code === Code.Canceled)) {
            console.log("[Orchestrator] Stream stopped cleanly.");
            return;
        }

        if (err?.rawMessage?.includes("Concurrent stream limit exceeded")) {
            console.log("[Orchestrator] Concurrent stream limit hit. Retrying in 30 seconds...");
            scheduleSubstreamStart(STREAM_RESTART_DELAY_MS);
            return;
        }
        
        // 2. Handle Server Reconnect Requests
        // Code 13 (Internal) with "shutting down" is a standard "please reconnect" signal
        const isReconnectSignal =
            (err instanceof ConnectError && err.code === Code.Unavailable) ||
            (err.code === Code.Internal && err.rawMessage?.includes("endpoint is shutting down"));
        
        if (isReconnectSignal) {
            console.log("[Orchestrator] Endpoint requested reconnect (shutting down or unavailable). Restarting...");
            // Short delay for a polite reconnect
            scheduleSubstreamStart(RECONNECT_DELAY_MS);
            return;
        }
        
        // 3. Handle Generic Errors
        console.error("[Orchestrator] Stream Error:", err);
        if (EXIT_ON_STREAM_ERROR) {
            exitForDockerRestart("[Orchestrator] Exiting so Docker can restart the container.");
            return;
        }
        console.log("[Orchestrator] Retrying in 3 seconds...");
        scheduleSubstreamStart(RETRY_DELAY_MS);
    } finally {
        if (activeStreamController === controller) {
            activeStreamController = null;
        }

        if (!shuttingDown && pendingRestartDelayMs !== null) {
            const delayMs = pendingRestartDelayMs;
            pendingRestartDelayMs = null;
            console.log(`[Orchestrator] Stream closed. Restarting in ${delayMs}ms...`);
            scheduleSubstreamStart(delayMs);
        }
    }
}
// --- START SERVER ---
server = app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log(`endpoints: /add-address, /delete-address, /update-addresses, /list-addresses`);
    scheduleSubstreamStart(0);
});
