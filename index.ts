import express from "express";
import dotenv from "dotenv";
import { readPackageFromFile } from "@substreams/manifest";
import { createRegistry, createRequest, applyParams, streamBlocks } from '@substreams/core';
import { createConnectTransport } from "@connectrpc/connect-node";
import { createClient } from '@clickhouse/client';

dotenv.config();

// --- CONFIGURATION ---
const PORT = 3000;
const SPKG_PATH = "./jupiter_dex_substreams.spkg";
const SUBSTREAMS_ENDPOINT = "mainnet.sol.streamingfast.io:443";
const API_TOKEN = process.env.SUBSTREAMS_API_TOKEN;
const OUTPUT_MODULE = "map_balance_changes";

// --- CLICKHOUSE SETUP ---
const clickhouse = createClient({
    url: 'http://127.0.0.1:8123',
    username: 'default',
    password: '',
    database: 'solana',
});

// --- STATE MANAGEMENT ---
let activeStreamController: AbortController | null = null;

// Use a Set to ensure unique addresses automatically
let monitoredAddresses = new Set<string>();

const app = express();
app.use(express.json());

// ==========================================
//               HELPER FUNCTIONS
// ==========================================

// Helper to handle the stream restart logic safely
function triggerStreamRestart() {
    if (activeStreamController) {
        console.log("[Orchestrator] Aborting current stream...");
        activeStreamController.abort();
    }
    
    // Small delay to ensure clean socket closure
    setTimeout(() => {
        // Convert Set back to Array for processing
        startSubstream(Array.from(monitoredAddresses));
    }, 500);
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

// 1. UPDATE (Replace entire list)
app.post("/update-addresses", (req, res) => {
    const { addresses } = req.body;
    const list = normalizeInput(addresses);
    
    if (list.length === 0) return res.status(400).json({ error: "Invalid input" });
    
    monitoredAddresses = new Set(list);
    
    console.log(`\n[API] Reset list. Tracking ${monitoredAddresses.size} addresses.`);
    triggerStreamRestart();
    
    res.json({ status: "success", count: monitoredAddresses.size, addresses: Array.from(monitoredAddresses) });
});

// 2. ADD (Append to list)
app.post("/add-address", (req, res) => {
    const { address, addresses } = req.body; // Accept "address" (single) or "addresses" (array)
    const input = address || addresses;
    const list = normalizeInput(input);
    
    if (list.length === 0) return res.status(400).json({ error: "Provide 'address' string or 'addresses' array" });
    
    let changed = false;
    list.forEach(addr => {
        if (!monitoredAddresses.has(addr)) {
            monitoredAddresses.add(addr);
            changed = true;
        }
    });
    
    if (changed) {
        console.log(`\n[API] Added ${list.length} items. Total: ${monitoredAddresses.size}`);
        triggerStreamRestart();
    } else {
        console.log(`\n[API] Skipped add (Duplicate).`);
    }
    
    res.json({ status: "success", changed, count: monitoredAddresses.size, addresses: Array.from(monitoredAddresses) });
});

// 3. DELETE (Remove from list)
app.post("/delete-address", (req, res) => {
    const { address, addresses } = req.body;
    const input = address || addresses;
    const list = normalizeInput(input);
    
    if (list.length === 0) return res.status(400).json({ error: "Provide 'address' string or 'addresses' array" });
    
    let changed = false;
    list.forEach(addr => {
        if (monitoredAddresses.has(addr)) {
            monitoredAddresses.delete(addr);
            changed = true;
        }
    });
    
    if (changed) {
        console.log(`\n[API] Removed ${list.length} items. Total: ${monitoredAddresses.size}`);
        triggerStreamRestart();
    } else {
        console.log(`\n[API] Skipped delete (Not found).`);
    }
    
    res.json({ status: "success", changed, count: monitoredAddresses.size, addresses: Array.from(monitoredAddresses) });
});

// 4. LIST (View current)
app.get("/list-addresses", (req, res) => {
    res.json({ count: monitoredAddresses.size, addresses: Array.from(monitoredAddresses) });
});

// ==========================================
//             SUBSTREAM LOGIC
// ==========================================

async function startSubstream(addresses: string[]) {
    const controller = new AbortController();
    activeStreamController = controller;
    const signal = controller.signal;
    
    // If list is empty, don't start stream (saves money)
    if (addresses.length === 0) {
        console.log("[Orchestrator] No addresses to track. Stream paused.");
        return;
    }
    
    const paramsString = addresses.join(",");
    console.log(`[Orchestrator] Starting stream with ${addresses.length} whitelisted addresses...`);
    
    try {
        const substreamPackage = await readPackageFromFile(SPKG_PATH);
        
        // 1. APPLY PARAMS DYNAMICALLY
        // This sends the whitelist to the server.
        // The server filters the data BEFORE sending it to you (saving Egress costs).
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
        
        // 2. CURSOR HANDLING (CRITICAL)
        // We fetch the last cursor from ClickHouse so we don't start from scratch
        const cursorRes = await clickhouse.query({
            query: "SELECT cursor FROM solana.cursors_node LIMIT 1",
            format: "JSONEachRow"
        });
        const rows = await cursorRes.json();
        // @ts-ignore
        const startCursor = rows.length > 0 ? rows[0].cursor : undefined;
        
        const request = createRequest({
            substreamPackage,
            outputModule: OUTPUT_MODULE,
            productionMode: true,
            startBlockNum: 376967294, // Only used if cursor is undefined
            startCursor: startCursor
        });
        
        const stream = streamBlocks(transport, request);
        
        console.log("[Orchestrator] Stream connected.");
        
        for await (const response of stream) {
            if (signal.aborted) break;
            
            if (response.message.case === "blockScopedData") {
                const output = response.message.value.output;
                const cursor = response.message.value.cursor;
                const clock = response.message.value.clock;
                
                if (output && output.mapOutput) {
                    const decodedData = output.mapOutput.unpack(registry);
                    
                    if (decodedData && (decodedData as any).params) {
                        const changes = (decodedData as any).params; // The list of BalanceChange
                        
                        if (changes.length > 0) {
                            console.log(`[Data] Block ${clock?.number}: Inserting ${changes.length} rows`);
                            
                            // 3. INSERT INTO CLICKHOUSE
                            await clickhouse.insert({
                                table: 'wallet_balance_changes',
                                values: changes.map((row: any) => ({
                                    id: `${row.txId}:${row.owner}:${row.mint}`, // Generate ID
                                    block_time: row.blockTime, // Ensure proto field names match (camelCase usually in JS)
                                    block_slot: row.blockSlot,
                                    tx_id: row.txId,
                                    owner: row.owner,
                                    mint: row.mint,
                                    change_amount: parseFloat(row.changeAmount),
                                    new_balance: parseFloat(row.newBalance),
                                    decimals: row.decimals
                                })),
                                format: 'JSONEachRow'
                            });
                        }
                    }
                }
                
                // 4. SAVE CURSOR
                // Save cursor every block so we resume correctly on restart
                await clickhouse.command({
                    query: `INSERT INTO solana.cursors_node (id, cursor, block_num) VALUES (1, '${cursor}', ${clock?.number})`
                });
            }
        }
    } catch (err: any) {
        if (signal.aborted) return;
        console.error("[Orchestrator] Stream Error:", err);
        console.log("[Orchestrator] Retrying in 3 seconds...");
        setTimeout(() => startSubstream(Array.from(monitoredAddresses)), 3000);
    }
}
// --- START SERVER ---
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log(`endpoints: /add-address, /delete-address, /update-addresses, /list-addresses`);
});