import express from "express";
import dotenv from "dotenv";
import { readPackageFromFile } from "@substreams/manifest";
import { createRegistry, createRequest, applyParams, streamBlocks } from '@substreams/core';
import { createConnectTransport } from "@connectrpc/connect-node";
import { createSubstream } from "@substreams/core";

dotenv.config();

// --- CONFIGURATION ---
const PORT = 3000;
const SPKG_PATH = "./jupiter_dex_substreams.spkg";
const SUBSTREAMS_ENDPOINT = "mainnet.sol.streamingfast.io:443";
const API_TOKEN = process.env.SUBSTREAMS_API_TOKEN;
const OUTPUT_MODULE = "map_relevant_transactions";

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
    
    if (addresses.length === 0) {
        console.log("[Orchestrator] No addresses to track. Idling...");
        return;
    }
    
    const paramsString = addresses.join(",");
    console.log(`[Orchestrator] Starting stream for ${addresses.length} addresses...`);
    
    try {
        const substreamPackage = await readPackageFromFile(SPKG_PATH);
        
        const paramUpdates = [
            `map_jupiter_trading_data=${paramsString}`,
            `map_relevant_transactions=${paramsString}`,
            `map_jupiter_instructions=${paramsString}`
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
        
        const request = createRequest({
            substreamPackage,
            outputModule: OUTPUT_MODULE,
            productionMode: true,
        });
        
        const stream = streamBlocks(transport, request);
        
        for await (const response of stream) {
            if (signal.aborted) {
                console.log("[Orchestrator] Stream stopped cleanly.");
                break;
            }
            
            if (response.message.case === "blockScopedData") {
                const output = response.message.value.output;
                if (output && output.mapOutput) {
                    const decodedData = output.mapOutput.unpack(registry);
                    if (decodedData) {
                        const txs = (decodedData as any).transactions || [];
                        if (txs.length > 0) {
                            console.log(`[Data] Block ${response.message.value.clock?.number}: Found ${txs.length} txs`);
                            // Process data here...
                        }
                    }
                }
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