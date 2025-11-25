# 🪐 Pluto Substream Orchestrator

A dynamic Node.js controller for the **Pluto Substream**. This service acts as an orchestrator that manages a live connection to the StreamingFast Firehose, allowing you to **dynamically update the list of tracked Solana addresses** at runtime without redeploying code.

---

## ⚡️ Features

*   **Dynamic Filtering:** Update your tracking list via simple HTTP requests.
*   **Zero Downtime Updates:** Automatically handles the graceful restart of the Substream connection when parameters change.
*   **Dual-Tracking:** Monitors both **Jupiter Swaps** and **General Transaction History** (Transfers, Staking, etc.) for the target addresses.
*   **Deduplication:** Automatically manages a unique set of addresses.

---

## 🛠 Prerequisites

*   **Node.js** (v18 or higher)
*   **PNPM** or **Yarn**
*   A compiled Substream package (`.spkg` file) in your root directory.
*   A **StreamingFast API Token** (Get one at [The Graph](https://thegraph.com/) or Pinax).

---

## 🚀 Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/pluto-orchestrator.git
    cd pluto-orchestrator
    ```

2.  **Install dependencies:**
    ```bash
    pnpm install
    ```

3.  **Environment Setup:**
    Create a `.env` file in the root directory:
    ```env
    # Your StreamingFast/Pinax API Key
    SUBSTREAMS_API_TOKEN=server_12345...
    ```

4.  **Ensure your package is ready:**
    Place your compiled `jupiter_dex_substreams.spkg` in the root folder.

---

## 🏃‍♂️ Usage

Start the Orchestrator server.

```bash
# Using ts-node (Recommended)
npx ts-node index.ts

# OR (if compiled)
npm start
```

*Server defaults to port `3000`.*

---

## 📡 API Reference

The Orchestrator exposes a REST API to manage the address filter.

### 1. Add Addresses
Appends one or more addresses to the current tracking list.

*   **URL:** `/add-address`
*   **Method:** `POST`
*   **Body:**
    ```json
    {
      "addresses": [
        "JUPyiwrYJFskUPiHa7hkeR8VUtkqj82hUHz16NsQHK8",
        "So11111111111111111111111111111111111111112"
      ]
    }
    ```
    *(You can also pass a single string `"address": "..."`)*

### 2. Delete Addresses
Removes one or more addresses from the tracking list.

*   **URL:** `/delete-address`
*   **Method:** `POST`
*   **Body:**
    ```json
    {
      "address": "JUPyiwrYJFskUPiHa7hkeR8VUtkqj82hUHz16NsQHK8"
    }
    ```

### 3. Update (Replace) List
**Overwrites** the entire tracking list with a new set of addresses.

*   **URL:** `/update-addresses`
*   **Method:** `POST`
*   **Body:**
    ```json
    {
      "addresses": [
        "NewAddress1...",
        "NewAddress2..."
      ]
    }
    ```

### 4. List Addresses
Returns the current list of addresses being monitored.

*   **URL:** `/list-addresses`
*   **Method:** `GET`
*   **Response:**
    ```json
    {
      "count": 2,
      "addresses": [
        "JUPyiwrYJFskUPiHa7hkeR8VUtkqj82hUHz16NsQHK8",
        "So11111111111111111111111111111111111111112"
      ]
    }
    ```

---

## 🧠 Architecture

### How Dynamic Parameters Work
Substreams (WASM) cannot listen for HTTP requests directly. To solve this, the Orchestrator uses a **Restart Pattern**:

1.  The Node.js server holds the state (`Set<string>` of addresses).
2.  When an API request modifies the list, the Orchestrator sends an `abort` signal to the active gRPC stream.
3.  It generates a new `params` string (comma-separated addresses).
4.  It immediately initiates a **new** connection to the Substreams provider with the updated parameters.

> **Note:** You may see "Aborting current stream..." in the logs during updates. This is expected behavior.

---