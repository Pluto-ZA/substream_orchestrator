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

// --- CLICKHOUSE SETUP ---
const clickhouse = createClient({
    url: process.env.CLICKHOUSE_URL!,
    username: 'default',
    password: process.env.CLICKHOUSE_PASSWORD!,
    database: 'solana',
});

// --- STATE MANAGEMENT ---
let activeStreamController: AbortController | null = null;

const app = express();
app.use(express.json());

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
async function triggerStreamRestart() {
    if (activeStreamController) {
        console.log("[Orchestrator] Watchlist updated. Restarting stream...");
        activeStreamController.abort();
        activeStreamController = null;
        
        setTimeout(() => startSubstream(), 2000);
    } else {
        // If not running, start it
        startSubstream();
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
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    if (activeStreamController) {
        if (!activeStreamController.signal.aborted) {
            activeStreamController.abort();
        }
        activeStreamController = null;
    }
    
    const controller = new AbortController();
    activeStreamController = controller;
    const signal = controller.signal;
    
    // 2. Fetch Params from DB
    const addresses = await fetchWhitelist();
    
    if (addresses.length === 0) {
        console.log("[Orchestrator] DB Watchlist is empty. Stream idling...");
        return;
    }
    
    const paramsString = addresses.join(",");
    console.log(`[Orchestrator] Starting stream with ${addresses.length} whitelisted addresses...`);
    
    try {
        const substreamPackage = await readPackageFromFile(SPKG_PATH);
        
        const paramUpdates = [
            `map_balance_changes=${paramsString}`
        ];
        
        applyParams(paramUpdates, substreamPackage.modules!.modules);
        
        const registry = createRegistry(substreamPackage);
        
        const transport = createConnectTransport({
            baseUrl: `https://${SUBSTREAMS_ENDPOINT}`,
            httpVersion: "2",
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
        // @ts-ignore
        const startCursor = rows.length > 0 ? rows[0].cursor : undefined;
        
        const request = createRequest({
            substreamPackage,
            outputModule: OUTPUT_MODULE,
            productionMode: true,
            startBlockNum: 379360000, // todo adjust to 1 year ago after testing
            startCursor: startCursor
        });
        
        // @ts-ignore
        const stream = streamBlocks(transport, request);
        
        console.log("[Orchestrator] Stream connected.");
        
        let lastKnownCursor = startCursor;
        let lastLogTime = Date.now();
        
        for await (const response of stream) {
            if (signal.aborted) break;
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
        if (signal.aborted) return;
        
        if (err instanceof ConnectError && err.code === Code.Unavailable) {
            console.log("[Orchestrator] Endpoint requested reconnect (shutting down). Restarting...");
            setTimeout(() => startSubstream(), 1000);
            return;
        }
        
        console.error("[Orchestrator] Stream Error:", err);
        console.log("[Orchestrator] Retrying in 3 seconds...");
        setTimeout(() => startSubstream(), 3000);
    }
}
// --- START SERVER ---
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log(`endpoints: /add-address, /delete-address, /update-addresses, /list-addresses`);
    startSubstream();
});