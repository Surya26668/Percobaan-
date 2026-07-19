
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv belum terinstall")

try:
    from cryptography.fernet import Fernet
except ImportError:
    print("⚠️  cryptography belum terinstall")
    Fernet = None

import os
import io
import sys
import json
import zipfile
import subprocess
import threading
import re
import uuid
import time
import signal
import shutil
import hashlib
import asyncio
import socket
import httpx
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, deque

from fastapi import FastAPI, Form, Request, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
import anthropic

# ================================================
# DECRYPT API KEY
# ================================================
def decrypt_key(encrypted: str) -> str:
    if not encrypted:
        return ""
    if Fernet is None:
        return ""
    try:
        enc_key = os.getenv("ENCRYPTION_KEY", "")
        if not enc_key:
            return encrypted
        f = Fernet(enc_key.encode())
        return f.decrypt(encrypted.encode()).decode()
    except Exception as e:
        print(f"[DECRYPT] ❌ {e}")
        return ""

# ================================================
# SETTING
# ================================================
BASE_URL = os.getenv("BASE_URL", "https://ai.bluepack.my.id/anthropic")

_raw_keys = []
i = 1
while True:
    enc = os.getenv(f"API_KEY_{i}_ENC", "")
    raw = os.getenv(f"API_KEY_{i}", "")
    mdl = os.getenv(f"MODEL_{i}", "")
    if not enc and not raw:
        break
    api_key = decrypt_key(enc) if enc else raw
    model   = mdl or "claude-sonnet-4-5"
    if api_key:
        _raw_keys.append({"base_url": BASE_URL, "api_key": api_key, "model": model})
    i += 1

if not _raw_keys:
    _raw_keys = [{"base_url": BASE_URL, "api_key": os.getenv("API_KEY_1", ""), "model": "claude-sonnet-4-5"}]

API_KEYS = [k for k in _raw_keys if k["api_key"]]
if not API_KEYS:
    raise ValueError("❌ Tidak ada API Key valid!")

print(f"✅ {len(API_KEYS)} API Key berhasil dimuat")
for idx, k in enumerate(API_KEYS):
    hint = k["api_key"][:8] + "..." if len(k["api_key"]) > 8 else k["api_key"]
    print(f"   [{idx}] model={k['model']} | key={hint}")

MAX_FIX                     = 3

BASE_DIR                    = Path("workspace")
CACHE_DIR                   = Path(".cache")
BACKUPS_DIR                 = Path("backups")
BASE_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)
BACKUPS_DIR.mkdir(exist_ok=True)
Path("templates").mkdir(exist_ok=True)

# ============================================
# ⏱️ TIMEOUT & FILE SIZE — Kunci Stabilitas
# ============================================
REQUEST_TIMEOUT              = 300   # ✅ Naik dari 180 → cukup untuk file besar + model reasoning (Claude Sonnet bisa 3-5 menit untuk file kompleks)
MAX_FILES_PER_PROJ           = 100
MAX_LINES_PER_FILE           = 500   # ✅ Sweet spot: cukup besar untuk kode lengkap, cukup kecil untuk hindari timeout

# ============================================
# 🔄 UPSTREAM RETRY TUNING
# ============================================
UPSTREAM_MAX_RETRY           = 5     # Retry sebelum menyerah per-request
UPSTREAM_WAIT_BASE           = 8     # Detik, basis exponential backoff (8, 16, 32, 45, 45...)
UPSTREAM_MAX_WAIT            = 45    # Cap maksimum wait per retry

# ============================================
# 🔌 CIRCUIT BREAKER — FIXED (Konsisten & Realistis)
# ============================================
CIRCUIT_BREAKER_THRESHOLD    = 8     # ✅ FIXED: 8 failure berturut-turut baru open (bukan 100!)
                                       #    Terlalu rendah (3) = gampang trip karena 1-2 error sementara
                                       #    Terlalu tinggi (100) = sistem terus hajar API yang jelas down
CIRCUIT_BREAKER_TIMEOUT      = 90    # ✅ FIXED: 90 detik cooldown (bukan 300!)
                                       #    Cukup untuk provider recover, tidak bikin project stuck lama

MAX_FILE_RETRY                = 3    # ✅ Naik sedikit dari 2 → retry per-file lebih toleran
MAX_UPSTREAM_FAIL_PER_BUILD   = 5    # ✅ Naik dari 3 → jangan langsung skip semua file kalau ada 3 error sementara

# ============================================
# 🌐 SERVER & NETWORK
# ============================================
PORT_START           = 9001
PORT_END             = 9100

RATE_LIMIT_PER_MIN   = 30
CACHE_TTL_SECONDS    = 3600
AUTO_CLEANUP_MIN     = 60

app = FastAPI(title="AI Project Maker — ULTIMATE Edition v2.3")

projects: dict = {}
STATE_FILE = Path("projects_state.json")

running_processes: dict = {}
_proc_lock = threading.Lock()

ai_cache          : dict = {}
_cache_lock       = threading.Lock()

request_stats     : dict = defaultdict(lambda: {"count": 0, "success": 0, "failed": 0, "total_time": 0})
_stats_lock       = threading.Lock()

rate_limit_store  : dict = defaultdict(lambda: deque(maxlen=100))
_rate_lock        = threading.Lock()

websocket_connections: dict = defaultdict(set)
_ws_lock              = threading.Lock()

# ✅ Circuit breaker state
_circuit_state = {
    "is_open"    : False,
    "fail_count" : 0,
    "opened_at"  : 0.0,
    "total_opens": 0,
}
_circuit_lock = threading.Lock()

# ================================================
# STATE
# ================================================
def load_state():
    global projects
    if STATE_FILE.exists():
        try:
            projects = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            changed  = False
            for pid, pdata in projects.items():
                if pdata.get("status") == "loading":
                    pdata["status"] = "error"
                    pdata["error"]  = "Server restart saat proses berlangsung"
                    changed = True
            if changed:
                print(f"[STATE] Reset stuck projects")
            print(f"[STATE] Loaded {len(projects)} project")
        except Exception as e:
            print(f"[STATE] Gagal: {e}")
            projects = {}

def save_state():
    try:
        STATE_FILE.write_text(json.dumps(projects, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[STATE] Gagal simpan: {e}")

load_state()

# ================================================
# CACHE SYSTEM
# ================================================
def cache_key(system: str, user: str) -> str:
    content = f"{system}||{user}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]

def cache_get(system: str, user: str) -> str:
    key = cache_key(system, user)
    with _cache_lock:
        if key in ai_cache:
            entry = ai_cache[key]
            age   = time.time() - entry["timestamp"]
            if age < CACHE_TTL_SECONDS:
                print(f"[CACHE] ✨ HIT (age: {int(age)}s)")
                return entry["response"]
            else:
                del ai_cache[key]
    return None

def cache_set(system: str, user: str, response: str):
    key = cache_key(system, user)
    with _cache_lock:
        ai_cache[key] = {"response": response, "timestamp": time.time()}
        if len(ai_cache) > 200:
            sorted_items = sorted(ai_cache.items(), key=lambda x: x[1]["timestamp"])
            for k, _ in sorted_items[:50]:
                del ai_cache[k]

# ================================================
# RATE LIMITING
# ================================================
def check_rate_limit(client_id: str) -> bool:
    now = time.time()
    with _rate_lock:
        history = rate_limit_store[client_id]
        while history and history[0] < now - 60:
            history.popleft()
        if len(history) >= RATE_LIMIT_PER_MIN:
            return False
        history.append(now)
        return True

# ================================================
# ✅ CIRCUIT BREAKER — FIXED LOGIC
# ================================================
def circuit_is_open() -> bool:
    with _circuit_lock:
        if not _circuit_state["is_open"]:
            return False
        if time.time() - _circuit_state["opened_at"] > CIRCUIT_BREAKER_TIMEOUT:
            _circuit_state["is_open"]    = False
            _circuit_state["fail_count"] = 0
            print(f"[CIRCUIT] 🔄 Circuit CLOSED (auto-reset setelah {CIRCUIT_BREAKER_TIMEOUT}s)")
            return False
        remaining = int(CIRCUIT_BREAKER_TIMEOUT - (time.time() - _circuit_state["opened_at"]))
        print(f"[CIRCUIT] 🔴 Circuit OPEN (reset dalam {remaining}s)")
        return True

def circuit_record_failure():
    """Dipanggil HANYA setelah upstream_retries exhausted — bukan setiap 502"""
    with _circuit_lock:
        _circuit_state["fail_count"] += 1
        if (_circuit_state["fail_count"] >= CIRCUIT_BREAKER_THRESHOLD
                and not _circuit_state["is_open"]):
            _circuit_state["is_open"]     = True
            _circuit_state["opened_at"]   = time.time()
            _circuit_state["total_opens"] += 1
            print(f"[CIRCUIT] 🔴 Circuit OPENED! "
                  f"({_circuit_state['fail_count']} exhausted retries)")

def circuit_record_success():
    with _circuit_lock:
        _circuit_state["fail_count"] = 0
        if _circuit_state["is_open"]:
            _circuit_state["is_open"] = False
            print(f"[CIRCUIT] 🟢 Circuit CLOSED (success)")

def circuit_force_reset():
    with _circuit_lock:
        _circuit_state["is_open"]    = False
        _circuit_state["fail_count"] = 0
        print("[CIRCUIT] ♻️ Force reset")

# ================================================
# MULTI KEY MANAGER
# ================================================
class MultiKeyManager:
    def __init__(self, keys: list):
        self.keys           = keys
        self.current_idx    = 0
        self.error_counts   = [0] * len(keys)
        self.success_counts = [0] * len(keys)
        self.total_time     = [0.0] * len(keys)
        self.last_error     = [""] * len(keys)
        self.last_used      = [0.0] * len(keys)
        self.upstream_fails = [0] * len(keys)
        self._lock          = threading.Lock()

    def get_info(self) -> dict:
        with self._lock:
            self.last_used[self.current_idx] = time.time()
            return self.keys[self.current_idx]

    def get_best_key(self) -> int:
        with self._lock:
            best_idx   = 0
            best_score = float('inf')
            for i in range(len(self.keys)):
                score = (self.error_counts[i] * 5
                         + self.upstream_fails[i] * 15
                         - self.success_counts[i])
                if score < best_score:
                    best_score = score
                    best_idx   = i
            self.current_idx = best_idx
            return best_idx

    def next_key(self):
        with self._lock:
            self.current_idx = (self.current_idx + 1) % len(self.keys)

    def mark_error(self, error: str = ""):
        with self._lock:
            self.error_counts[self.current_idx] += 1
            self.last_error[self.current_idx]    = error[:100]
        self.next_key()

    def mark_upstream_error(self, status_code: int):
        with self._lock:
            self.upstream_fails[self.current_idx] += 1
            self.last_error[self.current_idx]      = f"Upstream {status_code}"
        self.next_key()

    def mark_success(self, elapsed: float):
        with self._lock:
            self.success_counts[self.current_idx] += 1
            self.total_time[self.current_idx]     += elapsed

    def reset_errors(self):
        with self._lock:
            self.error_counts   = [0] * len(self.keys)
            self.upstream_fails = [0] * len(self.keys)
            self.last_error     = [""] * len(self.keys)

    def status(self) -> list:
        with self._lock:
            result = []
            for i, key_info in enumerate(self.keys):
                api_key      = key_info["api_key"]
                total_req    = self.success_counts[i] + self.error_counts[i]
                avg_time     = (self.total_time[i] / self.success_counts[i]
                                if self.success_counts[i] > 0 else 0)
                success_rate = (self.success_counts[i] / total_req * 100
                                if total_req > 0 else 0)
                result.append({
                    "index"          : i,
                    "base_url"       : key_info["base_url"],
                    "model"          : key_info["model"],
                    "api_key_hint"   : api_key[:8] + "..." if len(api_key) > 8 else api_key,
                    "error_count"    : self.error_counts[i],
                    "upstream_fails" : self.upstream_fails[i],
                    "success_count"  : self.success_counts[i],
                    "success_rate"   : round(success_rate, 1),
                    "avg_time"       : round(avg_time, 2),
                    "last_error"     : self.last_error[i],
                    "last_used"      : self.last_used[i],
                    "active"         : i == self.current_idx,
                })
        return result

key_manager = MultiKeyManager(API_KEYS)

# ================================================
# ANTHROPIC CLIENT
# ================================================
def buat_client(base_url: str, api_key: str) -> anthropic.Anthropic:
    return anthropic.Anthropic(
        base_url    = base_url,
        api_key     = api_key,
        timeout     = float(REQUEST_TIMEOUT),
        max_retries = 0,
        http_client = httpx.Client(
            timeout=httpx.Timeout(
                connect=15.0, read=float(REQUEST_TIMEOUT),
                write=15.0,   pool=15.0,
            ),
            verify=False,
        )
    )

def ambil_text(resp) -> str:
    hasil = ""
    for block in resp.content:
        block_type = getattr(block, "type", "")
        if block_type == "text":
            hasil += getattr(block, "text", "")
        elif block_type == "thinking":
            print(f"[AI] 💭 ThinkingBlock diskip")
    return hasil.strip()

# ================================================
# ✅ TANYA AI — FIXED CIRCUIT BREAKER LOGIC
#
# PERBAIKAN UTAMA:
#   • circuit_record_failure() dipanggil HANYA setelah upstream_retries
#     benar-benar habis — bukan setiap kali 502 terjadi
#   • Setiap upstream retry → ganti key otomatis
#   • consecutive_upstream dihapus, cukup upstream_retries
# ================================================
def tanya_ai(system_prompt: str, user_prompt: str,
             max_tokens: int = 4096, use_cache: bool = True) -> str:

    # ── Cache check ──
    if use_cache:
        cached = cache_get(system_prompt, user_prompt)
        if cached:
            with _stats_lock:
                request_stats["cache_hit"]["count"] += 1
            return cached

    # ── Circuit breaker check ──
    if circuit_is_open():
        raise Exception(
            f"🔴 Provider sedang down (circuit breaker aktif). "
            f"Coba lagi dalam {CIRCUIT_BREAKER_TIMEOUT}s atau POST /circuit-reset"
        )

    last_error       = "no error"
    upstream_retries = 0
    total_attempts   = 0
    max_total        = len(API_KEYS) * 2 + UPSTREAM_MAX_RETRY

    key_manager.get_best_key()

    while total_attempts < max_total:
        total_attempts += 1
        key_info = key_manager.get_info()
        model    = key_info["model"]

        print(f"[AI] Try {total_attempts}/{max_total} | {model} | upstream_retry={upstream_retries}")
        t_start = time.time()

        try:
            client   = buat_client(key_info["base_url"], key_info["api_key"])
            response = client.messages.create(
                model      = model,
                max_tokens = max_tokens,
                temperature= 0.1,
                system     = system_prompt,
                messages   = [{"role": "user", "content": user_prompt}]
            )

            hasil   = ambil_text(response)
            elapsed = time.time() - t_start

            if not hasil:
                raise Exception("Response kosong")

            # ✅ Success → reset circuit & stats
            circuit_record_success()
            key_manager.mark_success(elapsed)
            with _stats_lock:
                request_stats["total"]["count"]      += 1
                request_stats["total"]["success"]    += 1
                request_stats["total"]["total_time"] += elapsed

            print(f"[AI] ✅ {elapsed:.1f}s | {len(hasil)} chars")

            if use_cache:
                cache_set(system_prompt, user_prompt, hasil)

            return hasil

        except anthropic.AuthenticationError as e:
            last_error = f"401: {str(e)[:200]}"
            print(f"[AI] ❌ Auth: {last_error}")
            key_manager.mark_error(last_error)
            time.sleep(1)

        except anthropic.RateLimitError as e:
            last_error = f"429: {str(e)[:200]}"
            print(f"[AI] ❌ RateLimit: {last_error}")
            key_manager.mark_error(last_error)
            time.sleep(5)

        except anthropic.APIStatusError as e:
            status     = e.status_code
            last_error = f"HTTP {status}: {str(e.message)[:200]}"
            print(f"[AI] ❌ {last_error}")

            if status in (502, 503, 504):
                upstream_retries += 1
                key_manager.mark_upstream_error(status)

                # ✅ KUNCI FIX: circuit_record_failure HANYA jika sudah exhausted
                if upstream_retries > UPSTREAM_MAX_RETRY:
                    circuit_record_failure()   # ← satu "failure event" ke circuit
                    with _stats_lock:
                        request_stats["total"]["failed"] += 1

                    if circuit_is_open():
                        raise Exception(
                            f"🔴 Provider sustained down (HTTP {status}, "
                            f"{upstream_retries}x retry habis). "
                            f"Circuit breaker aktif. POST /circuit-reset setelah provider pulih."
                        )
                    raise Exception(
                        f"❌ Upstream {status} gagal {upstream_retries}x. "
                        f"Provider mungkin maintenance. Coba lagi nanti."
                    )

                # Masih dalam batas → exponential backoff + ganti key
                wait_time = min(
                    UPSTREAM_WAIT_BASE * (2 ** (upstream_retries - 1)),
                    UPSTREAM_MAX_WAIT
                )
                print(f"[AI] ⏳ Upstream {status}, wait {wait_time:.0f}s "
                      f"(retry {upstream_retries}/{UPSTREAM_MAX_RETRY})...")
                time.sleep(wait_time)
                key_manager.next_key()  # ✅ Ganti key setiap upstream retry

            else:
                key_manager.mark_error(last_error)
                time.sleep(2)

        except anthropic.APIConnectionError as e:
            last_error       = f"Connection: {str(e)[:200]}"
            upstream_retries += 1
            print(f"[AI] ❌ {last_error}")
            key_manager.mark_error(last_error)

            if upstream_retries > UPSTREAM_MAX_RETRY:
                circuit_record_failure()
                if circuit_is_open():
                    raise Exception("🔴 Koneksi terputus berulang. Circuit breaker aktif.")
                raise Exception(f"❌ Koneksi gagal {upstream_retries}x. {last_error}")

            time.sleep(3)

        except Exception as e:
            last_error = str(e)[:200]
            # Re-raise pesan circuit/upstream
            if "🔴" in last_error or "circuit" in last_error.lower():
                raise
            print(f"[AI] ❌ {last_error}")
            key_manager.mark_error(last_error)
            time.sleep(2)

    with _stats_lock:
        request_stats["total"]["failed"] += 1
    raise Exception(f"❌ Semua percobaan gagal ({total_attempts}x). Error: {last_error}")

# ================================================
# UTILS
# ================================================
def bersihkan_json(text: str) -> str:
    text = text.strip()
    if "```" in text:
        lines = [l for l in text.splitlines() if not l.strip().startswith("```")]
        text  = "\n".join(lines).strip()
    return text

def bersihkan_code(text: str) -> str:
    if not text:
        return text
    text  = text.strip()
    lines = text.split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```") and len(stripped) <= 15:
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()

# ================================================
# ✅ ULTRA TOLERAN JSON PARSER
# ================================================
def parse_json_toleran(text: str) -> dict:
    if not text or not text.strip():
        raise Exception("Empty response")

    text = bersihkan_json(text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 1 failed: {e}")

    try:
        match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 2 failed: {e}")

    try:
        start = text.find('{')
        end   = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end+1])
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 3 failed: {e}")

    try:
        fixed = re.sub(r'\\(?![\\"/bfnrtu])', r'\\\\', text)
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 4 failed: {e}")

    try:
        fixed = re.sub(r'\\(?![\\"/bfnrtu])', r'\\\\', text)
        fixed = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', fixed)
        start = fixed.find('{')
        end   = fixed.rfind('}')
        if start != -1 and end != -1:
            return json.loads(fixed[start:end+1])
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 5 failed: {e}")

    print("[JSON] Fallback: regex extract per field")
    result = {
        "analysis": "", "new_files": {}, "modified_files": {}, "run_cmd": "",
        "notes": "", "description": "", "tech_stack": "", "files": [],
        "install_cmd": "", "test_cmd": "", "project_type": "script", "fixed_files": {},
    }
    for field in ["analysis", "run_cmd", "notes", "description",
                  "tech_stack", "install_cmd", "test_cmd", "project_type"]:
        m = re.search(rf'"{field}"\s*:\s*"([^"]*(?:\\.[^"]*)*)"', text)
        if m:
            result[field] = m.group(1)

    m = re.search(r'"files"\s*:\s*\[(.*?)\]', text, re.DOTALL)
    if m:
        result["files"] = re.findall(r'"([^"]+)"', m.group(1))

    for match in re.finditer(
        r'"([a-zA-Z0-9_/\-\.]+\.(?:py|js|ts|html|css|json|txt|md|yaml|yml|toml))"'
        r'\s*:\s*"((?:[^"\\]|\\.)*)"',
        text
    ):
        fname, fcontent = match.group(1), match.group(2)
        fcontent = (fcontent.replace('\\n', '\n').replace('\\t', '\t')
                            .replace('\\"', '"').replace('\\\\', '\\'))
        result["modified_files"][fname] = fcontent

    if not any([result["modified_files"], result["new_files"], result["fixed_files"],
                result["files"], result["description"], result["run_cmd"]]):
        raise Exception("Tidak bisa parse JSON dari response AI")
    return result

# ================================================
# ✅ DEFAULT PLAN FALLBACK
# ================================================
def buat_default_plan(nama: str, deskripsi: str) -> dict:
    desc_lower = deskripsi.lower()

    if any(k in desc_lower for k in ["fastapi", "rest api", "endpoint", "swagger", "web api"]):
        ptype   = "fastapi"
        run_cmd = "uvicorn main:app --host 0.0.0.0 --port 8000"
        files   = ["main.py", "models.py", "database.py", "requirements.txt", "README.md"]
        tech    = "Python + FastAPI + SQLAlchemy"
    elif any(k in desc_lower for k in ["flask", "jinja"]):
        ptype   = "flask"
        run_cmd = "python main.py"
        files   = ["main.py", "requirements.txt", "README.md"]
        tech    = "Python + Flask"
    elif any(k in desc_lower for k in ["cli", "command line", "terminal", "argparse"]):
        ptype   = "cli"
        run_cmd = "python main.py"
        files   = ["main.py", "cli.py", "requirements.txt", "README.md"]
        tech    = "Python + Click/Argparse"
    elif any(k in desc_lower for k in ["ml ", "machine learning", "sklearn", "pandas",
                                        "tensorflow", "prediksi", "klasifikasi"]):
        ptype   = "script"
        run_cmd = "python main.py"
        files   = ["main.py", "train.py", "predict.py", "requirements.txt", "README.md"]
        tech    = "Python + scikit-learn + pandas"
    elif any(k in desc_lower for k in ["web", "http", "server", "html"]):
        ptype   = "fastapi"
        run_cmd = "uvicorn main:app --host 0.0.0.0 --port 8000"
        files   = ["main.py", "requirements.txt", "README.md"]
        tech    = "Python + FastAPI"
    else:
        ptype   = "script"
        run_cmd = "python main.py"
        files   = ["main.py", "requirements.txt", "README.md"]
        tech    = "Python"

    return {
        "description" : deskripsi[:150],
        "tech_stack"  : tech,
        "files"       : files[:MAX_FILES_PER_PROJ],
        "install_cmd" : "pip install -r requirements.txt",
        "run_cmd"     : run_cmd,
        "test_cmd"    : "python -m pytest -v",
        "project_type": ptype,
    }

# ================================================
# LOG + WEBSOCKET
# ================================================
_log_lock = threading.Lock()

def log(project_id: str, pesan: str, level: str = "info"):
    emoji_map = {
        "info": "ℹ️", "success": "✅", "error": "❌",
        "warning": "⚠️", "loading": "⏳", "fix": "🔧",
        "file": "📄", "folder": "📁", "run": "🧪",
        "done": "🎉", "lanjut": "🔄", "cache": "✨",
    }
    icon  = emoji_map.get(level, "•")
    entry = f"{icon} {pesan}"
    with _log_lock:
        if project_id in projects:
            projects[project_id]["logs"].append(entry)
            save_state()
    print(f"[{project_id}] {entry}")
    broadcast_ws(project_id, {"type": "log", "message": entry, "level": level})

def broadcast_ws(project_id: str, data: dict):
    with _ws_lock:
        conns = list(websocket_connections.get(project_id, set()))
    for ws in conns:
        try:
            asyncio.run_coroutine_threadsafe(ws.send_json(data), asyncio.get_event_loop())
        except Exception:
            pass

# ================================================
# COMMAND & FILE UTILS
# ================================================
def jalankan_cmd(cmd: str, cwd: str, timeout: int = 60) -> dict:
    try:
        hasil = subprocess.run(cmd, cwd=cwd, shell=True,
                               capture_output=True, text=True, timeout=timeout)
        return {"sukses": hasil.returncode == 0, "output": hasil.stdout, "error": hasil.stderr}
    except subprocess.TimeoutExpired:
        return {"sukses": False, "output": "", "error": f"TIMEOUT: '{cmd}'"}
    except Exception as e:
        return {"sukses": False, "output": "", "error": str(e)}

SKIP_DIRS = {"node_modules", "__pycache__", ".git", "venv", ".venv", ".mypy_cache"}
READ_EXTS = {".py", ".js", ".ts", ".html", ".css", ".json", ".txt",
             ".md", ".env", ".yaml", ".yml", ".toml"}

def baca_semua_file(project_dir: Path, max_chars: int = 400) -> dict:
    semua = {}
    for fp in sorted(project_dir.rglob("*")):
        if not fp.is_file(): continue
        if any(s in fp.parts for s in SKIP_DIRS): continue
        if fp.suffix not in READ_EXTS: continue
        if fp.name == ".ai_meta.json": continue
        try:
            rel        = str(fp.relative_to(project_dir))
            semua[rel] = fp.read_text(encoding="utf-8", errors="ignore")[:max_chars]
        except Exception:
            pass
    return semua

def baca_file_full(project_dir: Path) -> dict:
    semua = {}
    for fp in sorted(project_dir.rglob("*")):
        if not fp.is_file(): continue
        if any(s in fp.parts for s in SKIP_DIRS): continue
        if fp.suffix not in READ_EXTS: continue
        if fp.name == ".ai_meta.json": continue
        try:
            rel        = str(fp.relative_to(project_dir))
            semua[rel] = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            pass
    return semua

# ================================================
# BACKUP SYSTEM
# ================================================
def backup_project(project_dir: Path, nama: str) -> str:
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{nama}_{timestamp}.zip"
    backup_path = BACKUPS_DIR / backup_name
    try:
        with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for fp in project_dir.rglob("*"):
                if not fp.is_file(): continue
                if any(s in fp.parts for s in SKIP_DIRS): continue
                arcname = str(fp.relative_to(project_dir))
                zf.write(fp, arcname)
        print(f"[BACKUP] ✅ {backup_name}")
        return backup_name
    except Exception as e:
        print(f"[BACKUP] ❌ {e}")
        return ""

def list_backups(nama: str = None) -> list:
    hasil = []
    if not BACKUPS_DIR.exists():
        return hasil
    for fp in sorted(BACKUPS_DIR.iterdir(), reverse=True):
        if not fp.is_file() or fp.suffix != ".zip": continue
        if nama and not fp.name.startswith(nama + "_"): continue
        hasil.append({
            "name"   : fp.name,
            "size"   : fp.stat().st_size,
            "created": fp.stat().st_mtime,
            "project": fp.name.rsplit("_", 2)[0],
        })
    return hasil

# ================================================
# ✅ GENERATE SATU FILE — DENGAN SPLIT FILE BESAR
# ================================================
def generate_satu_file(nama_project: str, deskripsi: str,
                       filename: str, daftar_file: list) -> str:
    if filename.endswith("__init__.py"):
        return '"""Package init."""\n'

    desc_short = deskripsi[:200]   # ✅ lebih pendek dari 250 → hemat token
    files_hint = ", ".join(daftar_file[:6])

    ANTI_MARKDOWN = (
        "\n\nPENTING: Balas HANYA isi file mentah TANPA ```python, TANPA ```, "
        "TANPA penjelasan. Langsung code dari baris pertama."
    )
    PORT_HINT = (
        "\n\nCATATAN: Untuk FastAPI/Flask baca port dari env: "
        "port = int(os.environ.get('PORT', 8000)). Bind ke 0.0.0.0."
    )

    # ── requirements.txt ──
    if filename == "requirements.txt":
        system = "Tulis requirements.txt Python. Satu library per baris, tanpa versi kecuali perlu."
        user   = f"Project: {nama_project}\n{desc_short}\nTulis requirements.txt:" + ANTI_MARKDOWN
        return bersihkan_code(tanya_ai(system, user, max_tokens=350))

    # ── README.md ──
    elif filename == "README.md":
        system = "Tulis README.md singkat dan jelas."
        user   = (f"Project: {nama_project}\n{desc_short}\n"
                  "Tulis README.md: judul, deskripsi, install, cara pakai. Max 60 baris.")
        return bersihkan_code(tanya_ai(system, user, max_tokens=1000))

    # ── Test files ──
    elif "test" in filename.lower():
        system = "Tulis unit test pytest sederhana."
        user   = (f"Project: {nama_project}\n{desc_short}\n"
                  f"Tulis {filename}: 3-4 test dasar, gunakan mock jika perlu. Max 60 baris."
                  + ANTI_MARKDOWN)
        return bersihkan_code(tanya_ai(system, user, max_tokens=1200))

    # ── Config / Database ──
    elif filename in ["config.py", "database.py", "settings.py"]:
        system = "Tulis file konfigurasi Python ringkas."
        user   = (f"Project: {nama_project}\n{desc_short}\n"
                  f"Tulis {filename} ringkas, max 60 baris." + ANTI_MARKDOWN)
        return bersihkan_code(tanya_ai(system, user, max_tokens=1000))

    # ── Models / Schemas / CRUD ──
    elif filename in ["models.py", "schemas.py", "crud.py"]:
        system = "Programmer Python expert FastAPI + SQLAlchemy."
        user   = (f"Project: {nama_project}\n{desc_short}\nFile lain: {files_hint}\n\n"
                  f"Tulis {filename} lengkap, max 120 baris." + ANTI_MARKDOWN)
        return bersihkan_code(tanya_ai(system, user, max_tokens=2000))

    # ── File utama / kompleks ──
    else:
        # ✅ Deteksi file kompleks → pecah jadi 2 request lebih kecil
        is_complex = any(kw in filename.lower() for kw in [
            "stealth", "engine", "core", "manager", "handler",
            "processor", "controller", "service", "worker",
            "nexus", "main", "app",
        ])

        system = "Programmer Python expert. Tulis code ringkas, langsung jalan."

        if is_complex:
            # ── Part 1: struktur ──
            user_p1 = (
                f"Project: {nama_project}\n{desc_short}\nFile: {files_hint}\n\n"
                f"Tulis {filename} BAGIAN 1: imports, constants, class/function signatures. "
                f"Max 100 baris. Akhiri dengan komentar # === LANJUT ===\n"
                + ANTI_MARKDOWN
            )
            part1 = bersihkan_code(tanya_ai(system, user_p1, max_tokens=2000))

            # ── Part 2: implementasi ──
            user_p2 = (
                f"Project: {nama_project}\n{desc_short}\n\n"
                f"Tulis {filename} BAGIAN 2: implementasi method dan fungsi utama. "
                f"Lanjutan dari bagian 1. Max 100 baris. Jangan import ulang."
                + ANTI_MARKDOWN
            )
            part2 = bersihkan_code(tanya_ai(system, user_p2, max_tokens=2000))

            # Gabung + bersihkan marker
            part1 = part1.replace("# === LANJUT ===", "").rstrip()
            return part1 + "\n\n" + part2

        else:
            user = (
                f"Project: {nama_project}\n{desc_short}\nFile lain: {files_hint}\n\n"
                f"Tulis {filename} lengkap, langsung jalan. Max {MAX_LINES_PER_FILE} baris."
                + PORT_HINT + ANTI_MARKDOWN
            )
            return bersihkan_code(tanya_ai(system, user, max_tokens=3000))

# ================================================
# ✅ GENERATE FILE DENGAN RETRY PER-FILE
# ================================================
def generate_satu_file_with_retry(
    nama_project: str,
    deskripsi: str,
    filename: str,
    daftar_file: list,
) -> str:
    """
    Wrapper dengan retry khusus upstream error per file.
    Jika provider 502 → tunggu + force-reset circuit → coba lagi.
    """
    last_err = ""
    for attempt in range(1, MAX_FILE_RETRY + 1):
        try:
            return generate_satu_file(nama_project, deskripsi, filename, daftar_file)

        except Exception as e:
            last_err    = str(e)
            is_upstream = any(k in last_err for k in [
                "502", "503", "504", "circuit", "🔴",
                "down", "upstream", "Connection", "maintenance",
            ])

            print(f"[FILE-RETRY] {filename} attempt {attempt}/{MAX_FILE_RETRY}: {last_err[:120]}")

            if is_upstream and attempt < MAX_FILE_RETRY:
                wait_sec = 20 * attempt   # 20s lalu 40s
                print(f"[FILE-RETRY] Wait {wait_sec}s lalu reset circuit & coba lagi...")
                time.sleep(wait_sec)
                # ✅ Reset agar file berikutnya tidak langsung diblok circuit
                circuit_force_reset()
                key_manager.reset_errors()
                continue

            # Non-upstream atau sudah max retry → propagate ke caller
            raise Exception(last_err)

    raise Exception(last_err)

# ================================================
# PERBAIKI ERROR
# ================================================
def perbaiki_error(project_id: str, project_dir: Path, error_msg: str):
    log(project_id, "Menganalisis error...", "fix")
    semua_file = baca_file_full(project_dir)
    if not semua_file:
        return

    system = (
        "Debugger Python expert. Perbaiki error.\n"
        "Balas HANYA JSON valid tanpa markdown.\n"
        "Escape: \\ → \\\\, newline → \\n, kutip → \\\".\n"
        '{"analysis":"penjelasan","fixed_files":{"path/file.py":"code LENGKAP"}}'
    )
    context, total = {}, 0
    for k, v in semua_file.items():
        if total > 4000: break
        context[k] = v
        total      += len(v)

    user = f"ERROR:\n{error_msg[:600]}\n\nFILE:\n{json.dumps(context, ensure_ascii=False)}"

    try:
        raw  = tanya_ai(system, user, max_tokens=4000, use_cache=False)
        data = parse_json_toleran(raw)
        log(project_id, f"Analisis: {data.get('analysis', '-')}", "fix")
        for filename, new_code in (data.get("fixed_files") or {}).items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(bersihkan_code(new_code), encoding="utf-8")
            log(project_id, f"Diperbaiki: {filename}", "fix")
    except Exception as e:
        log(project_id, f"Error perbaiki: {e}", "error")

# ================================================
# ✅ BUAT PROJECT — FULL FIX v2.3
# ================================================
def buat_project_background(project_id: str, deskripsi: str, nama: str):
    try:
        log(project_id, "Memulai pembuatan project...", "loading")

        if circuit_is_open():
            msg = "🔴 Provider AI sedang down. POST /circuit-reset saat provider pulih."
            log(project_id, msg, "error")
            with _log_lock:
                projects[project_id]["status"] = "error"
                projects[project_id]["error"]  = msg
                save_state()
            broadcast_ws(project_id, {"type": "status", "status": "error"})
            return

        key_info = key_manager.get_info()
        log(project_id, f"Model: {key_info['model']}", "info")
        log(project_id, "Tahap 1: AI merancang struktur...", "loading")

        system_plan = (
            "Kamu arsitek software Python profesional.\n\n"
            "TUGAS: Balas HANYA dengan JSON valid. TIDAK ADA teks lain.\n"
            "TIDAK ADA ```json, TIDAK ADA penjelasan, TIDAK ADA markdown.\n"
            "Langsung mulai dengan { dan akhiri dengan }.\n\n"
            "FORMAT WAJIB:\n"
            "{\n"
            '  "description": "...",\n'
            '  "tech_stack": "Python + FastAPI",\n'
            '  "files": ["main.py", "requirements.txt", "README.md"],\n'
            '  "install_cmd": "pip install -r requirements.txt",\n'
            '  "run_cmd": "python main.py",\n'
            '  "test_cmd": "pytest",\n'
            '  "project_type": "script"\n'
            "}\n\n"
            "ATURAN:\n"
            "- project_type: cli | fastapi | flask | script\n"
            "- files: array of string\n"
            f"- MAKSIMAL {MAX_FILES_PER_PROJ} file, idealnya 4-6 file saja\n"
            "- Balas HANYA JSON\n"
        )
        user_plan = (
            f"Nama: {nama}\n"
            f"Deskripsi: {deskripsi[:300]}\n\n"
            "JSON:"
        )

        plan = None
        try:
            raw_plan = tanya_ai(system_plan, user_plan, max_tokens=600, use_cache=False)
            print(f"[PLAN] Raw (300 chars): {raw_plan[:300]}")
            plan = parse_json_toleran(raw_plan)
            log(project_id, "✓ Planning JSON valid", "success")

        except Exception as e:
            err_str = str(e)
            if any(kw in err_str for kw in ["🔴", "circuit", "502", "503", "504", "down"]):
                log(project_id, f"Provider down saat planning → default plan", "warning")
                plan = buat_default_plan(nama, deskripsi)
            else:
                log(project_id, "Retry planning...", "warning")
                try:
                    raw_retry = tanya_ai(
                        "JSON only. Start { end }.",
                        f"Project: {nama} - {deskripsi[:150]}\nJSON:",
                        max_tokens=500, use_cache=False
                    )
                    plan = parse_json_toleran(raw_retry)
                    log(project_id, "✓ Retry planning OK", "success")
                except Exception:
                    log(project_id, "Pakai default plan", "warning")
                    plan = buat_default_plan(nama, deskripsi)

        if not isinstance(plan, dict) or not plan:
            plan = buat_default_plan(nama, deskripsi)

        daftar_file  = plan.get("files") or ["main.py", "requirements.txt", "README.md"]
        if not isinstance(daftar_file, list):
            daftar_file = ["main.py", "requirements.txt", "README.md"]
        if len(daftar_file) > MAX_FILES_PER_PROJ:
            daftar_file = daftar_file[:MAX_FILES_PER_PROJ]

        install_cmd  = plan.get("install_cmd")  or "pip install -r requirements.txt"
        run_cmd      = plan.get("run_cmd")       or "python main.py"
        test_cmd     = plan.get("test_cmd")      or "pytest"
        tech_stack   = plan.get("tech_stack")    or "Python"
        description  = plan.get("description")   or deskripsi
        project_type = (plan.get("project_type") or "script").lower()

        if project_type not in ["cli", "fastapi", "flask", "script"]:
            project_type = "script"

        log(project_id, f"Struktur: {len(daftar_file)} file", "info")
        log(project_id, f"Tipe: {project_type}", "info")

        project_dir = BASE_DIR / nama
        project_dir.mkdir(parents=True, exist_ok=True)
        for f in daftar_file:
            (project_dir / f).parent.mkdir(parents=True, exist_ok=True)

        with _log_lock:
            projects[project_id]["folder"]       = str(project_dir.resolve())
            projects[project_id]["tech"]         = tech_stack
            projects[project_id]["desc"]         = description
            projects[project_id]["project_type"] = project_type
            save_state()

        log(project_id, f"Folder: workspace/{nama}/", "folder")
        log(project_id, "Tahap 2: Generate file...", "loading")

        file_berhasil       = []
        file_gagal          = []
        upstream_fail_count = 0

        for idx, filename in enumerate(daftar_file, 1):
            filename = str(filename).strip().lstrip("/")
            if not filename:
                continue

            # ✅ Skip sisanya jika terlalu banyak upstream failure
            if upstream_fail_count >= MAX_UPSTREAM_FAIL_PER_BUILD:
                log(project_id,
                    f"⚠️ {upstream_fail_count}x upstream down — skip sisa file. "
                    "POST /circuit-reset lalu 'Lanjutkan Project' untuk melanjutkan.",
                    "warning")
                for remaining in daftar_file[idx - 1:]:
                    remaining = str(remaining).strip().lstrip("/")
                    if remaining and remaining not in file_berhasil:
                        file_gagal.append(remaining)
                        placeholder = (
                            f"# {remaining} — BELUM DIBUAT (provider down)\n"
                            "# Gunakan 'Lanjutkan Project' saat provider pulih\n"
                        )
                        target = (project_dir / remaining).resolve()
                        if str(target).startswith(str(project_dir.resolve())):
                            target.parent.mkdir(parents=True, exist_ok=True)
                            target.write_text(placeholder, encoding="utf-8")
                break

            log(project_id, f"[{idx}/{len(daftar_file)}] {filename}...", "loading")
            t_start = time.time()

            try:
                # ✅ Pakai wrapper dengan per-file retry
                isi     = generate_satu_file_with_retry(nama, deskripsi, filename, daftar_file)
                elapsed = time.time() - t_start
                log(project_id, f"  ✓ {len(isi)} chars ({elapsed:.1f}s)", "success")
                upstream_fail_count = 0  # reset on success

            except Exception as e:
                elapsed = time.time() - t_start
                err_msg = str(e)[:200]
                is_upstream = any(kw in err_msg for kw in [
                    "🔴", "circuit", "502", "503", "504",
                    "down", "upstream", "maintenance",
                ])

                if is_upstream:
                    upstream_fail_count += 1
                    log(project_id,
                        f"  ⚠️ Upstream ({upstream_fail_count}x): {err_msg[:80]}",
                        "warning")
                else:
                    log(project_id, f"  ✗ Gagal: {err_msg[:100]}", "warning")

                file_gagal.append(filename)
                isi = (
                    f"# {filename} — GAGAL GENERATE\n"
                    f"# Error: {err_msg}\n"
                    "# Gunakan 'Lanjutkan Project' untuk regenerate\n"
                )

            target = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(isi, encoding="utf-8")
            file_berhasil.append(filename)
            log(project_id, f"  💾 {filename}", "file")

            with _log_lock:
                projects[project_id]["files"] = file_berhasil.copy()
                save_state()

        # ── Auto-add libs ke requirements.txt ──
        req_file = project_dir / "requirements.txt"
        if req_file.exists():
            current_req = req_file.read_text(encoding="utf-8")
            libs_add    = []
            if any("test" in f.lower() for f in file_berhasil) and "pytest" not in current_req.lower():
                libs_add.append("pytest")
            if project_type == "fastapi":
                if "fastapi" not in current_req.lower(): libs_add.append("fastapi")
                if "uvicorn" not in current_req.lower(): libs_add.append("uvicorn")
                if "jinja2"  not in current_req.lower(): libs_add.append("jinja2")
            if project_type == "flask" and "flask" not in current_req.lower():
                libs_add.append("flask")
            if libs_add:
                current_req = current_req.rstrip() + "\n" + "\n".join(libs_add) + "\n"
                req_file.write_text(current_req, encoding="utf-8")
                log(project_id, f"+ libs: {', '.join(libs_add)}", "info")

        with _log_lock:
            projects[project_id]["files"]   = file_berhasil
            projects[project_id]["run_cmd"] = run_cmd
            save_state()

        meta = {
            "nama": nama, "deskripsi": deskripsi, "tech_stack": tech_stack,
            "run_cmd": run_cmd, "install_cmd": install_cmd, "test_cmd": test_cmd,
            "files": file_berhasil, "project_type": project_type,
            "created_at": datetime.now().isoformat(),
            "failed_files": file_gagal,
        }
        (project_dir / ".ai_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # ── Install deps jika mayoritas file berhasil ──
        if len(file_gagal) < len(daftar_file) / 2 and req_file.exists():
            log(project_id, "Install dependencies...", "loading")
            hasil_install = jalankan_cmd(install_cmd, str(project_dir), timeout=180)
            if hasil_install["sukses"]:
                log(project_id, "Dependencies OK!", "success")
            else:
                log(project_id,
                    f"Warning install: {hasil_install['error'][:150]}", "warning")

        log(project_id, "=" * 40, "info")

        if file_gagal:
            log(project_id,
                f"Project '{nama}' selesai (partial: {len(file_gagal)} file gagal)!",
                "warning")
            log(project_id, f"File gagal: {', '.join(file_gagal[:5])}", "warning")
            log(project_id, "Gunakan 'Lanjutkan Project' untuk regenerate.", "info")
        else:
            log(project_id, f"Project '{nama}' selesai!", "done")

        log(project_id, f"Tipe: {project_type} | {len(file_berhasil)} file OK", "info")
        log(project_id, "Klik ▶️ RUN untuk jalankan!", "info")

        final_status = (
            "partial" if file_gagal and len(file_gagal) >= len(daftar_file) / 2
            else "done"
        )
        with _log_lock:
            projects[project_id]["status"] = final_status
            save_state()
        broadcast_ws(project_id, {"type": "status", "status": final_status})

    except Exception as e:
        import traceback
        msg = f"{str(e)}\n\n{traceback.format_exc()[:800]}"
        log(project_id, f"Error fatal: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
        broadcast_ws(project_id, {"type": "status", "status": "error"})

# ================================================
# ✅ LANJUT PROJECT
# ================================================
def lanjut_project_background(project_id: str, nama: str, permintaan: str):
    try:
        project_dir = BASE_DIR / nama
        if not project_dir.exists():
            log(project_id, f"Folder '{nama}' tidak ada", "error")
            projects[project_id]["status"] = "error"
            save_state()
            return

        if circuit_is_open():
            msg = "🔴 Provider AI sedang down. POST /circuit-reset saat provider pulih."
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        log(project_id, "Backup project...", "loading")
        backup_name = backup_project(project_dir, nama)
        if backup_name:
            log(project_id, f"Backup: {backup_name}", "info")

        log(project_id, f"Membaca project: {nama}", "lanjut")

        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            save_state()

        meta      = {}
        meta_file = project_dir / ".ai_meta.json"
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        semua_file = baca_semua_file(project_dir, max_chars=400)

        system_lanjut = (
            "Kamu programmer Python expert.\n\n"
            "TUGAS: Balas HANYA JSON valid. TIDAK ADA teks lain.\n"
            "TIDAK ADA ```json, TIDAK ADA markdown.\n"
            "Mulai dengan { akhiri dengan }.\n\n"
            "ESCAPE JSON:\n"
            "- \\ → \\\\\n- newline → \\n\n- \" → \\\"\n\n"
            "FORMAT:\n"
            "{\n"
            '  "analysis": "...",\n'
            '  "new_files": {"auth.py": "code..."},\n'
            '  "modified_files": {"main.py": "code..."},\n'
            '  "run_cmd": "python main.py",\n'
            '  "notes": "..."\n'
            "}\n\n"
            "HANYA JSON, tidak ada kata lain.\n"
        )

        context, total_ch = {}, 0
        for fname, fcontent in semua_file.items():
            if total_ch > 3000: break
            context[fname]  = fcontent
            total_ch       += len(fcontent)

        user_lanjut = (
            f"Project: {nama} | Tech: {meta.get('tech_stack', 'Python')}\n"
            f"Permintaan: {permintaan}\n\n"
            f"File:\n{json.dumps(context, ensure_ascii=False)}\n\nJSON:"
        )

        log(project_id, "AI mengembangkan...", "loading")

        hasil = None
        try:
            raw_lanjut = tanya_ai(system_lanjut, user_lanjut, max_tokens=5000, use_cache=False)
            print(f"[LANJUT] Raw (300 chars): {raw_lanjut[:300]}")
            hasil = parse_json_toleran(raw_lanjut)

        except Exception as e:
            err_str = str(e)
            if any(kw in err_str for kw in ["🔴", "circuit", "502", "503", "504", "down"]):
                msg = (
                    f"⚠️ Provider down saat mengembangkan.\n"
                    f"Backup: {backup_name}\n"
                    f"Coba lagi setelah POST /circuit-reset.\n"
                    f"Error: {err_str[:200]}"
                )
                log(project_id, msg, "error")
                projects[project_id]["status"] = "error"
                projects[project_id]["error"]  = msg
                save_state()
                return

            log(project_id, "Retry AI (JSON invalid)...", "warning")
            try:
                raw_retry = tanya_ai(
                    "JSON ONLY. Start { end }. No text.\n"
                    '{"analysis":"...","modified_files":{"file.py":"code"},'
                    '"new_files":{},"run_cmd":"python main.py","notes":"..."}',
                    f"Project: {nama}\nPermintaan: {permintaan[:200]}\n"
                    f"File: {list(semua_file.keys())[:5]}\nJSON:",
                    max_tokens=4000, use_cache=False
                )
                hasil = parse_json_toleran(raw_retry)
                log(project_id, "✓ Retry OK", "success")
            except Exception as e2:
                msg = f"AI gagal 2x: {str(e2)[:200]}"
                log(project_id, msg, "error")
                projects[project_id]["status"] = "error"
                projects[project_id]["error"]  = msg
                save_state()
                return

        if not isinstance(hasil, dict):
            hasil = {}

        log(project_id, f"Analisis: {hasil.get('analysis', '-')}", "info")

        new_files = hasil.get("new_files",      {}) or {}
        mod_files = hasil.get("modified_files", {}) or {}
        if not isinstance(new_files, dict): new_files = {}
        if not isinstance(mod_files, dict): mod_files = {}

        if not new_files and not mod_files:
            log(project_id, "AI tidak mengusulkan perubahan.", "warning")

        for filename, content in {**new_files, **mod_files}.items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(bersihkan_code(str(content)), encoding="utf-8")
            label = "Baru" if filename in new_files else "Update"
            log(project_id, f"{label}: {filename}", "file")

        all_files = list(semua_file.keys())
        for f in list(new_files.keys()) + list(mod_files.keys()):
            if f not in all_files:
                all_files.append(f)

        run_cmd                 = hasil.get("run_cmd") or meta.get("run_cmd", "")
        meta["files"]           = all_files
        meta["run_cmd"]         = run_cmd
        meta["last_modified"]   = datetime.now().isoformat()
        meta["failed_files"]    = []  # reset setelah lanjut berhasil
        meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        with _log_lock:
            projects[project_id]["files"]        = all_files
            projects[project_id]["run_cmd"]      = run_cmd
            projects[project_id]["tech"]         = meta.get("tech_stack", "")
            projects[project_id]["nama"]         = nama
            projects[project_id]["project_type"] = meta.get("project_type", "script")
            save_state()

        if "requirements.txt" in mod_files or "requirements.txt" in new_files:
            jalankan_cmd(
                meta.get("install_cmd", "pip install -r requirements.txt"),
                str(project_dir), timeout=180
            )

        was_running = project_id in running_processes
        if was_running:
            log(project_id, "Auto-restart...", "info")
            stop_project(project_id)
            time.sleep(1)
            run_project(project_id, project_dir, run_cmd, meta.get("project_type", "script"))
            log(project_id, "Restarted!", "success")

        log(project_id, "Pengembangan selesai!", "done")
        with _log_lock:
            projects[project_id]["status"] = "done"
            save_state()

    except Exception as e:
        import traceback
        msg = f"{str(e)}\n\n{traceback.format_exc()[:800]}"
        log(project_id, f"Error: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()

# ================================================
# RUN PROJECT
# ================================================
def cari_port_kosong() -> int:
    used_ports = set()
    with _proc_lock:
        for pinfo in running_processes.values():
            used_ports.add(pinfo.get("port", 0))
    for port in range(PORT_START, PORT_END):
        if port not in used_ports:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(0.5)
                    if s.connect_ex(("127.0.0.1", port)) != 0:
                        return port
            except Exception:
                return port
    return PORT_START

def stop_project(project_id: str) -> bool:
    with _proc_lock:
        info = running_processes.get(project_id)
        if not info: return False
        proc = info.get("process")
        if proc and proc.poll() is None:
            try:
                if sys.platform == "win32":
                    proc.terminate()
                else:
                    proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=5)
            except Exception:
                try: proc.kill()
                except: pass
        del running_processes[project_id]
        return True

def stream_output(project_id: str, proc):
    try:
        for line in iter(proc.stdout.readline, ''):
            if not line: break
            with _proc_lock:
                if project_id in running_processes:
                    running_processes[project_id]["logs"].append(line.rstrip())
                    if len(running_processes[project_id]["logs"]) > 500:
                        running_processes[project_id]["logs"] = \
                            running_processes[project_id]["logs"][-500:]
            broadcast_ws(project_id, {"type": "runlog", "line": line.rstrip()})
    except Exception as e:
        with _proc_lock:
            if project_id in running_processes:
                running_processes[project_id]["logs"].append(f"[STREAM ERROR] {e}")

def run_project(project_id: str, project_dir: Path,
                run_cmd: str, project_type: str) -> dict:
    stop_project(project_id)
    port = cari_port_kosong()
    env  = os.environ.copy()
    env["PORT"] = str(port)
    env["HOST"] = "127.0.0.1"

    if project_type == "fastapi":
        if "uvicorn" not in run_cmd.lower():
            for candidate in ["main:app", "app:app"]:
                mod_name = candidate.split(":")[0]
                if (project_dir / f"{mod_name}.py").exists():
                    run_cmd = f"uvicorn {candidate} --host 127.0.0.1 --port {port}"
                    break
        else:
            run_cmd = re.sub(r"--port\s+\d+",  f"--port {port}",      run_cmd)
            run_cmd = re.sub(r"--host\s+\S+",  "--host 127.0.0.1",   run_cmd)
            if "--port" not in run_cmd: run_cmd += f" --port {port}"
            if "--host" not in run_cmd: run_cmd += " --host 127.0.0.1"
    elif project_type == "flask":
        env["FLASK_RUN_PORT"] = str(port)
        env["FLASK_RUN_HOST"] = "127.0.0.1"

    print(f"[RUN] {project_id} → {run_cmd} @ :{port}")

    try:
        proc = subprocess.Popen(
            run_cmd, cwd=str(project_dir), shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env,
        )
        with _proc_lock:
            running_processes[project_id] = {
                "process"    : proc,
                "port"       : port,
                "logs"       : [f"[SYSTEM] {run_cmd}", f"[SYSTEM] Port: {port}"],
                "started_at" : time.time(),
                "run_cmd"    : run_cmd,
                "type"       : project_type,
                "last_access": time.time(),
            }
        threading.Thread(
            target=stream_output, args=(project_id, proc), daemon=True
        ).start()
        time.sleep(2)
        if proc.poll() is not None and proc.poll() != 0:
            with _proc_lock:
                logs = running_processes.get(project_id, {}).get("logs", [])
            return {
                "sukses": False,
                "error" : f"Process berhenti (exit: {proc.poll()})",
                "logs"  : logs[-20:],
            }
        is_web = project_type in ["fastapi", "flask"]
        url    = f"/proxy/{project_id}/" if is_web else None
        return {"sukses": True, "port": port, "run_cmd": run_cmd,
                "type": project_type, "url": url}
    except Exception as e:
        return {"sukses": False, "error": str(e)}

# ================================================
# AUTO CLEANUP
# ================================================
def cleanup_idle_processes():
    while True:
        try:
            time.sleep(300)
            now       = time.time()
            idle_pids = []
            with _proc_lock:
                for pid, info in running_processes.items():
                    idle = (now - info.get("last_access", now)) / 60
                    if idle > AUTO_CLEANUP_MIN:
                        idle_pids.append(pid)
            for pid in idle_pids:
                print(f"[CLEANUP] 🧹 Stop idle: {pid}")
                stop_project(pid)
        except Exception as e:
            print(f"[CLEANUP] Error: {e}")

threading.Thread(target=cleanup_idle_processes, daemon=True).start()

# ================================================
# ROUTES
# ================================================
@app.get("/", response_class=HTMLResponse)
async def halaman_utama():
    html_file = Path("templates/index.html")
    if not html_file.exists():
        return HTMLResponse("<h1>templates/index.html tidak ada</h1>", status_code=404)
    return HTMLResponse(html_file.read_text(encoding="utf-8"))

@app.get("/test-ai")
async def test_ai():
    hasil = []
    for i, k in enumerate(API_KEYS):
        try:
            client = buat_client(k["base_url"], k["api_key"])
            resp   = client.messages.create(
                model=k["model"], max_tokens=64,
                messages=[{"role": "user", "content": "Balas: OK"}]
            )
            reply  = ambil_text(resp) or "(kosong)"
            hasil.append({"index": i, "status": "✅ sukses", "model": k["model"], "reply": reply})
        except Exception as e:
            hasil.append({"index": i, "status": "❌ gagal", "model": k["model"], "error": str(e)})
    return JSONResponse({"keys": hasil})

@app.get("/health-upstream")
async def health_upstream():
    hasil = []
    for i, k in enumerate(API_KEYS):
        try:
            client = buat_client(k["base_url"], k["api_key"])
            client.messages.create(
                model=k["model"], max_tokens=10,
                messages=[{"role": "user", "content": "hi"}]
            )
            hasil.append({"index": i, "model": k["model"], "status": "✅ healthy"})
        except anthropic.APIStatusError as e:
            hasil.append({"index": i, "model": k["model"],
                          "status": f"⚠️ {e.status_code}", "error": str(e.message)[:150]})
        except Exception as e:
            hasil.append({"index": i, "model": k["model"],
                          "status": "❌ error", "error": str(e)[:150]})
    return JSONResponse({"keys": hasil})

@app.get("/circuit-status")
async def circuit_status():
    with _circuit_lock:
        state = dict(_circuit_state)
    remaining = 0
    if state["is_open"]:
        remaining = max(0, int(CIRCUIT_BREAKER_TIMEOUT - (time.time() - state["opened_at"])))
    return JSONResponse({
        "is_open"     : state["is_open"],
        "fail_count"  : state["fail_count"],
        "total_opens" : state["total_opens"],
        "reset_in_sec": remaining,
        "threshold"   : CIRCUIT_BREAKER_THRESHOLD,
        "timeout_sec" : CIRCUIT_BREAKER_TIMEOUT,
        "message"     : ("🔴 Circuit OPEN — AI calls ditolak"
                         if state["is_open"] else "🟢 Circuit CLOSED — normal"),
    })

@app.post("/circuit-reset")
async def circuit_reset():
    circuit_force_reset()
    key_manager.reset_errors()
    return JSONResponse({
        "success": True,
        "message": "✅ Circuit & key errors direset. Coba buat project lagi.",
    })

@app.get("/stats")
async def get_stats():
    with _stats_lock:
        stats = dict(request_stats)
    with _cache_lock:
        cache_size = len(ai_cache)
    with _proc_lock:
        running_count = len(running_processes)
    with _circuit_lock:
        circuit_info = dict(_circuit_state)

    total_projects = len(list(BASE_DIR.iterdir()))  if BASE_DIR.exists()    else 0
    total_backups  = len(list(BACKUPS_DIR.iterdir())) if BACKUPS_DIR.exists() else 0
    avg_time       = 0
    if stats.get("total", {}).get("success", 0) > 0:
        avg_time = stats["total"]["total_time"] / stats["total"]["success"]

    return JSONResponse({
        "requests": {
            "total"     : stats.get("total", {}).get("count",      0),
            "success"   : stats.get("total", {}).get("success",    0),
            "failed"    : stats.get("total", {}).get("failed",     0),
            "avg_time"  : round(avg_time, 2),
            "cache_hits": stats.get("cache_hit", {}).get("count",  0),
        },
        "cache"   : {"size": cache_size, "max": 200},
        "projects": {"total": total_projects, "running": running_count, "backups": total_backups},
        "circuit" : {
            "is_open"    : circuit_info["is_open"],
            "fail_count" : circuit_info["fail_count"],
            "total_opens": circuit_info["total_opens"],
        },
        "system"  : {
            "port_range"        : f"{PORT_START}-{PORT_END}",
            "rate_limit"        : f"{RATE_LIMIT_PER_MIN}/min",
            "cache_ttl"         : f"{CACHE_TTL_SECONDS}s",
            "upstream_max_retry": UPSTREAM_MAX_RETRY,
            "circuit_threshold" : CIRCUIT_BREAKER_THRESHOLD,
            "circuit_timeout"   : CIRCUIT_BREAKER_TIMEOUT,
            "max_file_retry"    : MAX_FILE_RETRY,
        },
    })

@app.get("/backups")
async def list_backups_route(nama: str = None):
    return JSONResponse({"backups": list_backups(nama)})

@app.post("/restore/{backup_name}")
async def restore_backup(backup_name: str):
    backup_path = BACKUPS_DIR / backup_name
    if not backup_path.exists():
        return JSONResponse({"error": "Backup tidak ada"}, status_code=404)
    try:
        project_name = backup_name.rsplit("_", 2)[0]
        target_dir   = BASE_DIR / project_name
        if target_dir.exists():
            backup_project(target_dir, project_name + "_before_restore")
        with zipfile.ZipFile(backup_path, "r") as zf:
            zf.extractall(target_dir)
        return JSONResponse({"success": True, "restored_to": str(target_dir)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/backups/{backup_name}")
async def delete_backup(backup_name: str):
    backup_path = BACKUPS_DIR / backup_name
    if not backup_path.exists():
        return JSONResponse({"error": "Not found"}, status_code=404)
    try:
        backup_path.unlink()
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/cache/clear")
async def clear_cache():
    with _cache_lock:
        count = len(ai_cache)
        ai_cache.clear()
    return JSONResponse({"cleared": count})

@app.post("/buat-project")
async def buat_project_route(request: Request,
                              deskripsi: str = Form(...), nama: str = Form(...)):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse({"error": f"Rate limit! Max {RATE_LIMIT_PER_MIN}/menit"},
                            status_code=429)

    if circuit_is_open():
        with _circuit_lock:
            remaining = max(0, int(
                CIRCUIT_BREAKER_TIMEOUT - (time.time() - _circuit_state["opened_at"])
            ))
        return JSONResponse({
            "error": f"🔴 Provider AI down. Coba dalam {remaining}s atau POST /circuit-reset."
        }, status_code=503)

    nama       = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project_kuliah"
    project_id = str(uuid.uuid4())[:8]
    with _log_lock:
        projects[project_id] = {
            "status": "loading", "logs": [], "files": [], "folder": "",
            "error": "", "run_cmd": "", "tech": "", "desc": "",
            "nama": nama, "project_type": "script",
            "created_at": datetime.now().isoformat(),
        }
        save_state()
    threading.Thread(
        target=buat_project_background, args=(project_id, deskripsi, nama), daemon=True
    ).start()
    return JSONResponse({"project_id": project_id, "nama": nama})

@app.post("/lanjut-project")
async def lanjut_project_route(request: Request,
                                nama: str = Form(...), permintaan: str = Form(...)):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse({"error": f"Rate limit! Max {RATE_LIMIT_PER_MIN}/menit"},
                            status_code=429)

    if circuit_is_open():
        with _circuit_lock:
            remaining = max(0, int(
                CIRCUIT_BREAKER_TIMEOUT - (time.time() - _circuit_state["opened_at"])
            ))
        return JSONResponse({
            "error": f"🔴 Provider AI down. Coba dalam {remaining}s."
        }, status_code=503)

    nama       = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project"
    project_id = str(uuid.uuid4())[:8]
    with _log_lock:
        projects[project_id] = {
            "status": "loading", "logs": [], "files": [], "folder": "",
            "error": "", "run_cmd": "", "tech": "", "desc": "",
            "nama": nama, "project_type": "script",
            "created_at": datetime.now().isoformat(),
        }
        save_state()
    threading.Thread(
        target=lanjut_project_background, args=(project_id, nama, permintaan), daemon=True
    ).start()
    return JSONResponse({"project_id": project_id, "nama": nama})

@app.get("/status/{project_id}")
async def cek_status(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Tidak ditemukan"}, status_code=404)
    data = dict(projects[project_id])
    with _proc_lock:
        info = running_processes.get(project_id)
        if info:
            proc   = info["process"]
            is_web = info.get("type") in ["fastapi", "flask"]
            data["running"] = {
                "alive"     : proc.poll() is None,
                "port"      : info["port"],
                "url"       : f"/proxy/{project_id}/" if is_web else None,
                "started_at": info["started_at"],
                "uptime"    : int(time.time() - info["started_at"]),
                "type"      : info.get("type"),
            }
        else:
            data["running"] = None
    return JSONResponse(data)

@app.get("/files/{project_id}")
async def lihat_files(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder = projects[project_id].get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"files": []})
    project_dir = Path(folder)
    hasil = []
    for fp in sorted(project_dir.rglob("*")):
        if not fp.is_file(): continue
        if fp.name == ".ai_meta.json": continue
        if any(s in fp.parts for s in SKIP_DIRS): continue
        rel = str(fp.relative_to(project_dir))
        try:
            isi = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            isi = "(tidak bisa dibaca)"
        hasil.append({"path": rel, "content": isi, "size": fp.stat().st_size})
    return JSONResponse({"files": hasil})

@app.post("/files/{project_id}/save")
async def save_file_route(project_id: str,
                          path: str = Form(...), content: str = Form(...)):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder = projects[project_id].get("folder", "")
    if not folder:
        return JSONResponse({"error": "No folder"}, status_code=400)
    project_dir = Path(folder)
    target      = (project_dir / path.lstrip("/")).resolve()
    if not str(target).startswith(str(project_dir.resolve())):
        return JSONResponse({"error": "Path unsafe"}, status_code=400)
    backup_project(project_dir, projects[project_id]["nama"])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    log(project_id, f"File di-edit manual: {path}", "info")
    return JSONResponse({"success": True})

@app.delete("/files/{project_id}/delete")
async def delete_file_route(project_id: str, path: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder      = projects[project_id].get("folder", "")
    project_dir = Path(folder)
    target      = (project_dir / path.lstrip("/")).resolve()
    if not str(target).startswith(str(project_dir.resolve())):
        return JSONResponse({"error": "Path unsafe"}, status_code=400)
    try:
        target.unlink()
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/list-projects")
async def list_projects_route():
    hasil = []
    if not BASE_DIR.exists():
        return JSONResponse({"projects": []})
    for folder in sorted(BASE_DIR.iterdir()):
        if not folder.is_dir(): continue
        meta      = {}
        meta_file = folder / ".ai_meta.json"
        if meta_file.exists():
            try: meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception: pass
        file_count = sum(
            1 for f in folder.rglob("*")
            if f.is_file() and f.name != ".ai_meta.json"
            and not any(s in f.parts for s in SKIP_DIRS)
        )
        is_running = False
        with _proc_lock:
            for pid in running_processes:
                if projects.get(pid, {}).get("nama") == folder.name:
                    is_running = True
                    break
        hasil.append({
            "nama"         : folder.name,
            "tech_stack"   : meta.get("tech_stack",   "-"),
            "deskripsi"    : meta.get("deskripsi",    "-"),
            "run_cmd"      : meta.get("run_cmd",      "-"),
            "project_type" : meta.get("project_type", "script"),
            "file_count"   : file_count,
            "created_at"   : meta.get("created_at",   ""),
            "last_modified": meta.get("last_modified", ""),
            "is_running"   : is_running,
            "failed_files" : meta.get("failed_files", []),
        })
    return JSONResponse({"projects": hasil})

@app.delete("/project/{nama}")
async def delete_project(nama: str):
    project_dir = BASE_DIR / nama
    if not project_dir.exists():
        return JSONResponse({"error": "Not found"}, status_code=404)
    with _proc_lock:
        pids_to_stop = [
            pid for pid in running_processes
            if projects.get(pid, {}).get("nama") == nama
        ]
    for pid in pids_to_stop:
        stop_project(pid)
    backup_project(project_dir, nama + "_before_delete")
    try:
        shutil.rmtree(project_dir)
        return JSONResponse({"success": True, "backup": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/key-status")
async def key_status_route():
    return JSONResponse({"current_index": key_manager.current_idx,
                         "keys": key_manager.status()})

@app.post("/key-reset")
async def key_reset_route():
    key_manager.reset_errors()
    return JSONResponse({"message": "Reset OK"})

@app.post("/run/{project_id}")
async def run_project_route(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    pd     = projects[project_id]
    folder = pd.get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"error": "Folder tidak ada"}, status_code=404)
    run_cmd = pd.get("run_cmd", "")
    if not run_cmd:
        meta_file = Path(folder) / ".ai_meta.json"
        if meta_file.exists():
            try:
                meta    = json.loads(meta_file.read_text(encoding="utf-8"))
                run_cmd = meta.get("run_cmd", "python main.py")
            except Exception:
                run_cmd = "python main.py"
    hasil = run_project(project_id, Path(folder), run_cmd, pd.get("project_type", "script"))
    return JSONResponse(hasil)

@app.post("/stop/{project_id}")
async def stop_project_route(project_id: str):
    stopped = stop_project(project_id)
    return JSONResponse({"success": stopped,
                         "message": "Dihentikan" if stopped else "No process"})

@app.post("/restart/{project_id}")
async def restart_project_route(project_id: str):
    stop_project(project_id)
    time.sleep(1)
    return await run_project_route(project_id)

@app.get("/run-logs/{project_id}")
async def run_logs_route(project_id: str):
    with _proc_lock:
        info = running_processes.get(project_id)
        if not info:
            return JSONResponse({"logs": [], "alive": False})
        info["last_access"] = time.time()
        proc   = info["process"]
        is_web = info.get("type") in ["fastapi", "flask"]
        return JSONResponse({
            "logs"     : info["logs"][-200:],
            "alive"    : proc.poll() is None,
            "port"     : info["port"],
            "url"      : f"/proxy/{project_id}/" if is_web else None,
            "exit_code": proc.poll(),
            "run_cmd"  : info.get("run_cmd", ""),
            "uptime"   : int(time.time() - info["started_at"]),
            "type"     : info.get("type"),
        })

@app.get("/download/{project_id}")
async def download_project(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder = projects[project_id].get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"error": "No folder"}, status_code=404)
    project_dir = Path(folder)
    nama        = projects[project_id].get("nama", "project")
    zip_buffer  = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in project_dir.rglob("*"):
            if not fp.is_file(): continue
            if any(s in fp.parts for s in SKIP_DIRS): continue
            zf.write(fp, str(fp.relative_to(project_dir)))
    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer, media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{nama}.zip"'},
    )

@app.get("/running-list")
async def running_list_route():
    hasil = []
    with _proc_lock:
        for pid, info in running_processes.items():
            proc   = info["process"]
            is_web = info.get("type") in ["fastapi", "flask"]
            hasil.append({
                "project_id": pid,
                "port"      : info["port"],
                "url"       : f"/proxy/{pid}/" if is_web else None,
                "alive"     : proc.poll() is None,
                "uptime"    : int(time.time() - info["started_at"]),
                "type"      : info.get("type"),
                "run_cmd"   : info.get("run_cmd", ""),
            })
    return JSONResponse({"running": hasil})

# ================================================
# WEBSOCKET
# ================================================
@app.websocket("/ws/{project_id}")
async def websocket_endpoint(websocket: WebSocket, project_id: str):
    await websocket.accept()
    with _ws_lock:
        websocket_connections[project_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        with _ws_lock:
            websocket_connections[project_id].discard(websocket)
    except Exception:
        with _ws_lock:
            websocket_connections[project_id].discard(websocket)

# ================================================
# PROXY
# ================================================
@app.api_route("/proxy/{project_id}",
    methods=["GET","POST","PUT","DELETE","PATCH","OPTIONS","HEAD"])
@app.api_route("/proxy/{project_id}/{path:path}",
    methods=["GET","POST","PUT","DELETE","PATCH","OPTIONS","HEAD"])
async def proxy_to_project(project_id: str, request: Request, path: str = ""):
    with _proc_lock:
        info = running_processes.get(project_id)
        if info:
            info["last_access"] = time.time()

    if not info:
        return HTMLResponse(content=f"""
        <html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
          <h2>⚠️ Project belum di-RUN</h2>
          <p>Klik ▶️ RUN untuk menjalankan project.</p>
          <p style="color:#64748b;font-size:.9rem">Project ID: {project_id}</p>
        </body></html>""", status_code=503)

    port = info.get("port")
    proc = info.get("process")
    if proc and proc.poll() is not None:
        return HTMLResponse(content=f"""
        <html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
          <h2>❌ Project berhenti (exit: {proc.poll()})</h2>
          <p>Cek tab ▶️ Run untuk error log.</p>
        </body></html>""", status_code=503)

    target_url = f"http://127.0.0.1:{port}/{path}"
    if request.url.query:
        target_url += "?" + request.url.query

    body    = await request.body()
    headers = dict(request.headers)
    for h in ["host", "content-length", "connection", "accept-encoding"]:
        headers.pop(h, None)

    try:
        async with httpx.AsyncClient(
            timeout=30.0, verify=False, follow_redirects=False
        ) as client:
            resp = await client.request(
                method=request.method, url=target_url,
                headers=headers, content=body
            )
        resp_headers = {}
        for k, v in resp.headers.items():
            if k.lower() in ["content-encoding", "transfer-encoding", "connection",
                              "x-frame-options", "content-security-policy",
                              "content-security-policy-report-only"]:
                continue
            resp_headers[k] = v
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location", "")
            if location.startswith("/"):
                resp_headers["location"] = f"/proxy/{project_id}{location}"
            elif location.startswith(f"http://127.0.0.1:{port}"):
                new_path = location.replace(f"http://127.0.0.1:{port}", "")
                resp_headers["location"] = f"/proxy/{project_id}{new_path}"
        return Response(
            content=resp.content, status_code=resp.status_code,
            headers=resp_headers, media_type=resp.headers.get("content-type"),
        )
    except httpx.ConnectError:
        return HTMLResponse(content=f"""
        <html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
          <h2>⏳ Loading...</h2>
          <p>Server belum siap di port {port}. Reload beberapa detik lagi.</p>
          <button onclick="location.reload()"
            style="padding:10px 20px;background:#6366f1;color:#fff;border:none;
                   border-radius:6px;cursor:pointer">🔄 Reload</button>
        </body></html>""", status_code=503)
    except httpx.TimeoutException:
        return HTMLResponse("<h2>Timeout</h2>", status_code=504)
    except Exception as e:
        return HTMLResponse(f"<h2>Error</h2><pre>{str(e)[:500]}</pre>", status_code=500)

@app.on_event("shutdown")
def cleanup_on_shutdown():
    print("[SHUTDOWN] Menghentikan running processes...")
    with _proc_lock:
        for pid in list(running_processes.keys()):
            try:
                proc = running_processes[pid]["process"]
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=3)
            except Exception:
                pass
        running_processes.clear()

# ================================================
print("=" * 60)
print("🚀 AI Project Maker ULTIMATE Edition v2.3")
print("=" * 60)
print(f"✨ Cache TTL       : {CACHE_TTL_SECONDS}s")
print(f"🛡️  Rate limit     : {RATE_LIMIT_PER_MIN}/menit per IP")
print(f"💾 Auto backup     : sebelum modifikasi")
print(f"🧹 Auto cleanup    : idle {AUTO_CLEANUP_MIN} menit")
print(f"🔴 Circuit breaker : threshold={CIRCUIT_BREAKER_THRESHOLD} | "
      f"timeout={CIRCUIT_BREAKER_TIMEOUT}s")
print(f"⚡ Upstream retry  : {UPSTREAM_MAX_RETRY}x | backoff cap {UPSTREAM_MAX_WAIT}s")
print(f"🔁 File retry      : {MAX_FILE_RETRY}x per file (upstream error)")
print(f"⛔ Skip threshold  : {MAX_UPSTREAM_FAIL_PER_BUILD}x upstream fail → skip sisanya")
print(f"✂️  Max lines/file  : {MAX_LINES_PER_FILE} | split file kompleks otomatis")
print(f"📊 Stats           : /stats | Circuit: /circuit-status | /circuit-reset")
print("=" * 60)
