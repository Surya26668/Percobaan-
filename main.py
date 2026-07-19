# ================================================
# AI PROJECT MAKER — ULTIMATE EDITION v3.0
# Support: Upload Merge, Figma Sync, Parallel Generation
# ================================================

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

import os, io, sys, json, zipfile, subprocess, threading, re, uuid, time
import signal, shutil, hashlib, asyncio, socket, httpx
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, Form, Request, WebSocket, WebSocketDisconnect, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
from typing import Optional, List
import anthropic

# ================================================
# DECRYPT API KEY
# ================================================
def decrypt_key(encrypted: str) -> str:
    if not encrypted or Fernet is None:
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
    model = mdl or "claude-sonnet-4-5"
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

MAX_FIX = 3
BASE_DIR = Path("workspace")
CACHE_DIR = Path(".cache")
BACKUPS_DIR = Path("backups")
UPLOADS_DIR = Path("uploads")
BASE_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)
BACKUPS_DIR.mkdir(exist_ok=True)
UPLOADS_DIR.mkdir(exist_ok=True)
Path("templates").mkdir(exist_ok=True)

# Timeouts & Sizes
REQUEST_TIMEOUT = 400
MAX_FILES_PER_PROJ = 100
MAX_LINES_PER_FILE = 700

# Chunking
CHUNK_SIZE_LINES = 150
MAX_CHUNKS_PER_FILE = 6
CHUNK_MAX_TOKENS = 4000
CONTEXT_TAIL_LINES = 15

# Upstream Retry
UPSTREAM_MAX_RETRY = 5
UPSTREAM_WAIT_BASE = 8
UPSTREAM_MAX_WAIT = 45

# Circuit Breaker
CIRCUIT_BREAKER_THRESHOLD = 8
CIRCUIT_BREAKER_TIMEOUT = 90
MAX_FILE_RETRY = 3
MAX_UPSTREAM_FAIL_PERCENT = 0.15

# Parallel
ENABLE_PARALLEL_GENERATION = True
MAX_PARALLEL_WORKERS = None
PARALLEL_MIN_FILES = 6

# Server
PORT_START = 9001
PORT_END = 9100
RATE_LIMIT_PER_MIN = 60
CACHE_TTL_SECONDS = 3600
AUTO_CLEANUP_MIN = 60

# Figma MCP
FIGMA_MCP_URL = os.getenv("FIGMA_MCP_URL", "https://mcp.figma.com/mcp")

app = FastAPI(title="AI Project Maker — ULTIMATE Edition v3.0")

projects = {}
STATE_FILE = Path("projects_state.json")
running_processes = {}
_proc_lock = threading.Lock()

ai_cache = {}
_cache_lock = threading.Lock()
request_stats = defaultdict(lambda: {"count": 0, "success": 0, "failed": 0, "total_time": 0})
_stats_lock = threading.Lock()
rate_limit_store = defaultdict(lambda: deque(maxlen=100))
_rate_lock = threading.Lock()
websocket_connections = defaultdict(set)
_ws_lock = threading.Lock()

_circuit_state = {"is_open": False, "fail_count": 0, "opened_at": 0.0, "total_opens": 0}
_circuit_lock = threading.Lock()

# ================================================
# STATE MANAGEMENT
# ================================================
def load_state():
    global projects
    if STATE_FILE.exists():
        try:
            projects = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            changed = False
            for pid, pdata in projects.items():
                if pdata.get("status") == "loading":
                    pdata["status"] = "error"
                    pdata["error"] = "Server restart saat proses berlangsung"
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
def cache_key(system, user):
    return hashlib.sha256(f"{system}||{user}".encode()).hexdigest()[:16]

def cache_get(system, user):
    key = cache_key(system, user)
    with _cache_lock:
        if key in ai_cache:
            entry = ai_cache[key]
            age = time.time() - entry["timestamp"]
            if age < CACHE_TTL_SECONDS:
                return entry["response"]
            del ai_cache[key]
    return None

def cache_set(system, user, response):
    key = cache_key(system, user)
    with _cache_lock:
        ai_cache[key] = {"response": response, "timestamp": time.time()}
        if len(ai_cache) > 200:
            for k, _ in sorted(ai_cache.items(), key=lambda x: x[1]["timestamp"])[:50]:
                del ai_cache[k]

# ================================================
# RATE LIMITING
# ================================================
def check_rate_limit(client_id):
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
# CIRCUIT BREAKER
# ================================================
def circuit_is_open():
    with _circuit_lock:
        if not _circuit_state["is_open"]:
            return False
        if time.time() - _circuit_state["opened_at"] > CIRCUIT_BREAKER_TIMEOUT:
            _circuit_state["is_open"] = False
            _circuit_state["fail_count"] = 0
            return False
        return True

def circuit_record_failure():
    with _circuit_lock:
        _circuit_state["fail_count"] += 1
        if _circuit_state["fail_count"] >= CIRCUIT_BREAKER_THRESHOLD and not _circuit_state["is_open"]:
            _circuit_state["is_open"] = True
            _circuit_state["opened_at"] = time.time()
            _circuit_state["total_opens"] += 1

def circuit_record_success():
    with _circuit_lock:
        _circuit_state["fail_count"] = 0
        _circuit_state["is_open"] = False

def circuit_force_reset():
    with _circuit_lock:
        _circuit_state["is_open"] = False
        _circuit_state["fail_count"] = 0

# ================================================
# MULTI KEY MANAGER
# ================================================
class MultiKeyManager:
    def __init__(self, keys):
        self.keys = keys
        self.current_idx = 0
        self.error_counts = [0] * len(keys)
        self.success_counts = [0] * len(keys)
        self.total_time = [0.0] * len(keys)
        self.last_error = [""] * len(keys)
        self.last_used = [0.0] * len(keys)
        self.upstream_fails = [0] * len(keys)
        self._lock = threading.Lock()
        self._round_robin = 0

    def get_info(self):
        with self._lock:
            self.last_used[self.current_idx] = time.time()
            return self.keys[self.current_idx]

    def get_info_round_robin(self):
        with self._lock:
            idx = self._round_robin % len(self.keys)
            self._round_robin += 1
            self.last_used[idx] = time.time()
            return self.keys[idx], idx

    def get_best_key(self):
        with self._lock:
            best_idx, best_score = 0, float('inf')
            for i in range(len(self.keys)):
                score = self.error_counts[i] * 5 + self.upstream_fails[i] * 15 - self.success_counts[i]
                if score < best_score:
                    best_score = score
                    best_idx = i
            self.current_idx = best_idx
            return best_idx

    def next_key(self):
        with self._lock:
            self.current_idx = (self.current_idx + 1) % len(self.keys)

    def mark_error(self, error="", idx=None):
        with self._lock:
            target = idx if idx is not None else self.current_idx
            self.error_counts[target] += 1
            self.last_error[target] = error[:100]
        if idx is None:
            self.next_key()

    def mark_upstream_error(self, status_code, idx=None):
        with self._lock:
            target = idx if idx is not None else self.current_idx
            self.upstream_fails[target] += 1
            self.last_error[target] = f"Upstream {status_code}"
        if idx is None:
            self.next_key()

    def mark_success(self, elapsed, idx=None):
        with self._lock:
            target = idx if idx is not None else self.current_idx
            self.success_counts[target] += 1
            self.total_time[target] += elapsed

    def reset_errors(self):
        with self._lock:
            self.error_counts = [0] * len(self.keys)
            self.upstream_fails = [0] * len(self.keys)
            self.last_error = [""] * len(self.keys)

    def status(self):
        with self._lock:
            result = []
            for i, key_info in enumerate(self.keys):
                total_req = self.success_counts[i] + self.error_counts[i]
                avg_time = self.total_time[i] / self.success_counts[i] if self.success_counts[i] > 0 else 0
                success_rate = self.success_counts[i] / total_req * 100 if total_req > 0 else 0
                result.append({
                    "index": i, "base_url": key_info["base_url"], "model": key_info["model"],
                    "api_key_hint": key_info["api_key"][:8] + "...",
                    "error_count": self.error_counts[i], "upstream_fails": self.upstream_fails[i],
                    "success_count": self.success_counts[i], "success_rate": round(success_rate, 1),
                    "avg_time": round(avg_time, 2), "last_error": self.last_error[i],
                    "last_used": self.last_used[i], "active": i == self.current_idx,
                })
            return result

key_manager = MultiKeyManager(API_KEYS)

# ================================================
# ANTHROPIC CLIENT
# ================================================
def buat_client(base_url, api_key):
    return anthropic.Anthropic(
        base_url=base_url, api_key=api_key, timeout=float(REQUEST_TIMEOUT), max_retries=0,
        http_client=httpx.Client(
            timeout=httpx.Timeout(connect=15.0, read=float(REQUEST_TIMEOUT), write=15.0, pool=15.0),
            verify=False
        )
    )

def ambil_text(resp):
    return "".join(getattr(block, "text", "") for block in resp.content if getattr(block, "type", "") == "text").strip()

# ================================================
# TANYA AI
# ================================================
def tanya_ai(system_prompt, user_prompt, max_tokens=4096, use_cache=True, force_key_idx=None):
    if use_cache:
        cached = cache_get(system_prompt, user_prompt)
        if cached:
            with _stats_lock:
                request_stats["cache_hit"]["count"] += 1
            return cached

    if circuit_is_open():
        raise Exception(f"🔴 Provider sedang down (circuit breaker aktif). Coba lagi dalam {CIRCUIT_BREAKER_TIMEOUT}s")

    last_error = "no error"
    upstream_retries = 0
    total_attempts = 0
    max_total = len(API_KEYS) * 2 + UPSTREAM_MAX_RETRY
    cur_idx = force_key_idx if force_key_idx is not None else key_manager.get_best_key()

    while total_attempts < max_total:
        total_attempts += 1
        key_info = key_manager.keys[cur_idx] if force_key_idx is not None else key_manager.get_info()
        model = key_info["model"]
        t_start = time.time()

        try:
            client = buat_client(key_info["base_url"], key_info["api_key"])
            response = client.messages.create(
                model=model, max_tokens=max_tokens, temperature=0.1,
                system=system_prompt, messages=[{"role": "user", "content": user_prompt}]
            )
            hasil = ambil_text(response)
            elapsed = time.time() - t_start
            if not hasil:
                raise Exception("Response kosong")
            circuit_record_success()
            key_manager.mark_success(elapsed, idx=cur_idx if force_key_idx is not None else None)
            with _stats_lock:
                request_stats["total"]["count"] += 1
                request_stats["total"]["success"] += 1
                request_stats["total"]["total_time"] += elapsed
            if use_cache:
                cache_set(system_prompt, user_prompt, hasil)
            return hasil

        except anthropic.AuthenticationError as e:
            last_error = f"401: {str(e)[:200]}"
            key_manager.mark_error(last_error, idx=cur_idx if force_key_idx is not None else None)
            if force_key_idx is not None:
                cur_idx = (cur_idx + 1) % len(API_KEYS)
            time.sleep(1)

        except anthropic.RateLimitError as e:
            last_error = f"429: {str(e)[:200]}"
            key_manager.mark_error(last_error, idx=cur_idx if force_key_idx is not None else None)
            if force_key_idx is not None:
                cur_idx = (cur_idx + 1) % len(API_KEYS)
            time.sleep(5)

        except anthropic.APIStatusError as e:
            status = e.status_code
            last_error = f"HTTP {status}: {str(e.message)[:200]}"
            if status in (502, 503, 504):
                upstream_retries += 1
                key_manager.mark_upstream_error(status, idx=cur_idx if force_key_idx is not None else None)
                if upstream_retries > UPSTREAM_MAX_RETRY:
                    circuit_record_failure()
                    with _stats_lock:
                        request_stats["total"]["failed"] += 1
                    raise Exception(f"❌ Upstream {status} gagal {upstream_retries}x")
                wait_time = min(UPSTREAM_WAIT_BASE * (2 ** (upstream_retries - 1)), UPSTREAM_MAX_WAIT)
                time.sleep(wait_time)
                if force_key_idx is not None:
                    cur_idx = (cur_idx + 1) % len(API_KEYS)
                else:
                    key_manager.next_key()
            else:
                key_manager.mark_error(last_error, idx=cur_idx if force_key_idx is not None else None)
                time.sleep(2)

        except Exception as e:
            last_error = str(e)[:200]
            if "🔴" in last_error or "circuit" in last_error.lower():
                raise
            key_manager.mark_error(last_error, idx=cur_idx if force_key_idx is not None else None)
            time.sleep(2)

    with _stats_lock:
        request_stats["total"]["failed"] += 1
    raise Exception(f"❌ Semua percobaan gagal ({total_attempts}x). Error: {last_error}")

# ================================================
# UTILS
# ================================================
def bersihkan_json(text):
    text = text.strip()
    if "```" in text:
        lines = [l for l in text.splitlines() if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text

def bersihkan_code(text):
    if not text:
        return text
    text = text.strip()
    lines = text.split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(l for l in lines if not (l.strip().startswith("```") and len(l.strip()) <= 15)).strip()

def parse_json_toleran(text):
    if not text or not text.strip():
        raise Exception("Empty response")
    text = bersihkan_json(text)
    
    for attempt, parser in enumerate([
        lambda t: json.loads(t),
        lambda t: json.loads(re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', t, re.DOTALL).group(0)),
        lambda t: json.loads(t[t.find('{'):t.rfind('}')+1]),
        lambda t: json.loads(re.sub(r'\\(?![\\"/bfnrtu])', r'\\\\', t)),
    ], 1):
        try:
            return parser(text)
        except:
            pass
    
    # Regex fallback
    result = {"analysis": "", "new_files": {}, "modified_files": {}, "run_cmd": "", "notes": "",
              "description": "", "tech_stack": "", "files": [], "install_cmd": "", "test_cmd": "",
              "project_type": "script", "fixed_files": {}}
    for field in ["analysis", "run_cmd", "notes", "description", "tech_stack", "install_cmd", "test_cmd", "project_type"]:
        m = re.search(rf'"{field}"\s*:\s*"([^"]*(?:\\.[^"]*)*)"', text)
        if m:
            result[field] = m.group(1)
    m = re.search(r'"files"\s*:\s*\[(.*?)\]', text, re.DOTALL)
    if m:
        result["files"] = re.findall(r'"([^"]+)"', m.group(1))
    for match in re.finditer(r'"([a-zA-Z0-9_/\-\.]+\.(?:py|js|ts|html|css|json|txt|md|yaml|yml|toml))"\s*:\s*"((?:[^"\\]|\\.)*)"', text):
        fname, fcontent = match.group(1), match.group(2)
        fcontent = fcontent.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"').replace('\\\\', '\\')
        result["modified_files"][fname] = fcontent
    if not any([result["modified_files"], result["new_files"], result["fixed_files"], result["files"], result["description"], result["run_cmd"]]):
        raise Exception("Tidak bisa parse JSON dari response AI")
    return result

def buat_default_plan(nama, deskripsi):
    desc_lower = deskripsi.lower()
    if any(k in desc_lower for k in ["fastapi", "rest api", "endpoint", "web api"]):
        return {"description": deskripsi[:150], "tech_stack": "Python + FastAPI",
                "files": ["main.py", "models.py", "database.py", "requirements.txt", "README.md"],
                "install_cmd": "pip install -r requirements.txt", "run_cmd": "uvicorn main:app --host 0.0.0.0 --port 8000",
                "test_cmd": "pytest", "project_type": "fastapi"}
    elif any(k in desc_lower for k in ["flask"]):
        return {"description": deskripsi[:150], "tech_stack": "Python + Flask",
                "files": ["main.py", "requirements.txt", "README.md"],
                "install_cmd": "pip install -r requirements.txt", "run_cmd": "python main.py",
                "test_cmd": "pytest", "project_type": "flask"}
    else:
        return {"description": deskripsi[:150], "tech_stack": "Python",
                "files": ["main.py", "requirements.txt", "README.md"],
                "install_cmd": "pip install -r requirements.txt", "run_cmd": "python main.py",
                "test_cmd": "pytest", "project_type": "script"}

# ================================================
# LOG + WEBSOCKET
# ================================================
_log_lock = threading.Lock()

def log(project_id, pesan, level="info"):
    icon = {"info": "ℹ️", "success": "✅", "error": "❌", "warning": "⚠️", "loading": "⏳",
            "fix": "🔧", "file": "📄", "folder": "📁", "run": "🧪", "done": "🎉",
            "lanjut": "🔄", "cache": "✨", "figma": "🎨"}.get(level, "•")
    entry = f"{icon} {pesan}"
    with _log_lock:
        if project_id in projects:
            projects[project_id]["logs"].append(entry)
            save_state()
    broadcast_ws(project_id, {"type": "log", "message": entry, "level": level})

def broadcast_ws(project_id, data):
    with _ws_lock:
        conns = list(websocket_connections.get(project_id, set()))
    for ws in conns:
        try:
            asyncio.run_coroutine_threadsafe(ws.send_json(data), asyncio.get_event_loop())
        except:
            pass

# ================================================
# FILE & COMMAND UTILS
# ================================================
def jalankan_cmd(cmd, cwd, timeout=60):
    try:
        hasil = subprocess.run(cmd, cwd=cwd, shell=True, capture_output=True, text=True, timeout=timeout)
        return {"sukses": hasil.returncode == 0, "output": hasil.stdout, "error": hasil.stderr}
    except subprocess.TimeoutExpired:
        return {"sukses": False, "output": "", "error": f"TIMEOUT: '{cmd}'"}
    except Exception as e:
        return {"sukses": False, "output": "", "error": str(e)}

SKIP_DIRS = {"node_modules", "__pycache__", ".git", "venv", ".venv", ".mypy_cache", "uploads"}
READ_EXTS = {".py", ".js", ".ts", ".html", ".css", ".json", ".txt", ".md", ".env", ".yaml", ".yml", ".toml"}

def baca_semua_file(project_dir, max_chars=400):
    semua = {}
    for fp in sorted(project_dir.rglob("*")):
        if not fp.is_file(): continue
        if any(s in fp.parts for s in SKIP_DIRS): continue
        if fp.suffix not in READ_EXTS: continue
        if fp.name == ".ai_meta.json": continue
        try:
            semua[str(fp.relative_to(project_dir))] = fp.read_text(encoding="utf-8", errors="ignore")[:max_chars]
        except: pass
    return semua

def baca_file_full(project_dir):
    semua = {}
    for fp in sorted(project_dir.rglob("*")):
        if not fp.is_file(): continue
        if any(s in fp.parts for s in SKIP_DIRS): continue
        if fp.suffix not in READ_EXTS: continue
        if fp.name == ".ai_meta.json": continue
        try:
            semua[str(fp.relative_to(project_dir))] = fp.read_text(encoding="utf-8", errors="ignore")
        except: pass
    return semua

# ================================================
# BACKUP SYSTEM
# ================================================
def backup_project(project_dir, nama):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{nama}_{timestamp}.zip"
    backup_path = BACKUPS_DIR / backup_name
    try:
        with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for fp in project_dir.rglob("*"):
                if not fp.is_file(): continue
                if any(s in fp.parts for s in SKIP_DIRS): continue
                zf.write(fp, str(fp.relative_to(project_dir)))
        return backup_name
    except Exception as e:
        print(f"[BACKUP] ❌ {e}")
        return ""

def list_backups(nama=None):
    hasil = []
    if not BACKUPS_DIR.exists(): return hasil
    for fp in sorted(BACKUPS_DIR.iterdir(), reverse=True):
        if not fp.is_file() or fp.suffix != ".zip": continue
        if nama and not fp.name.startswith(nama + "_"): continue
        hasil.append({"name": fp.name, "size": fp.stat().st_size, "created": fp.stat().st_mtime,
                      "project": fp.name.rsplit("_", 2)[0]})
    return hasil

# ================================================
# CHUNKED FILE GENERATION
# ================================================
def dedup_consecutive_blocks(code):
    lines = code.split("\n")
    if len(lines) < 30: return code
    cleaned, i = [], 0
    while i < len(lines):
        if len(cleaned) >= 5 and i + 5 <= len(lines) and lines[i:i+5] == cleaned[-5:]:
            i += 5
            continue
        cleaned.append(lines[i])
        i += 1
    return "\n".join(cleaned)

def hitung_target_lines(filename, deskripsi):
    fname_lower = filename.lower()
    if filename in ["requirements.txt", "README.md", ".env.example", ".gitignore", "package.json", "Dockerfile"]:
        return 0
    if "test" in fname_lower or filename in ["config.py", "settings.py", "constants.py"]:
        return 100
    if filename in ["models.py", "schemas.py", "types.py"]:
        return 200
    if any(kw in fname_lower for kw in ["main", "app", "core", "engine", "manager", "handler", "controller", "service"]):
        return MAX_LINES_PER_FILE
    return 300

def generate_file_chunked(nama_project, deskripsi, filename, daftar_file, target_lines, force_key_idx=None):
    system = "Kamu programmer expert. Tulis code production-ready, tanpa placeholder."
    user_first = (f"Project: {nama_project}\n{deskripsi[:200]}\nFile: {filename}\n"
                  f"Target: ~{target_lines} baris.\nTulis BAGIAN AWAL (~{CHUNK_SIZE_LINES} baris).\n"
                  "Akhiri dengan: # ===CHUNK_END===\nJANGAN markdown.")
    
    raw = tanya_ai(system, user_first, max_tokens=CHUNK_MAX_TOKENS, use_cache=False, force_key_idx=force_key_idx)
    full_code = bersihkan_code(raw).replace("# ===CHUNK_END===", "").rstrip()
    total_lines, chunk_num = len(full_code.splitlines()), 1

    while total_lines < target_lines and chunk_num < MAX_CHUNKS_PER_FILE:
        chunk_num += 1
        tail = "\n".join(full_code.splitlines()[-CONTEXT_TAIL_LINES:])
        sisa = target_lines - total_lines
        user_next = (f"File: {filename} (lanjutan)\n\n{CONTEXT_TAIL_LINES} baris terakhir:\n```\n{tail}\n```\n\n"
                     f"LANJUTKAN (~{min(CHUNK_SIZE_LINES, sisa)} baris).\n"
                     "Jika SELESAI: # ===FILE_COMPLETE===\nJika BELUM: # ===CHUNK_END===\nJANGAN markdown.")
        try:
            raw = tanya_ai(system, user_next, max_tokens=CHUNK_MAX_TOKENS, use_cache=False, force_key_idx=force_key_idx)
        except:
            break
        is_complete = "===FILE_COMPLETE===" in raw
        chunk_text = bersihkan_code(raw).replace("# ===FILE_COMPLETE===", "").replace("# ===CHUNK_END===", "").rstrip()
        full_code += "\n\n" + chunk_text
        total_lines = len(full_code.splitlines())
        if is_complete: break

    return dedup_consecutive_blocks(full_code)

def generate_satu_file(nama_project, deskripsi, filename, daftar_file, force_key_idx=None):
    if filename.endswith("__init__.py"):
        return '"""Package init."""\n'
    
    ANTI = "\n\nPENTING: Balas HANYA code mentah TANPA ```python, TANPA penjelasan."
    PORT = "\n\nCATATAN: Baca port dari env: port = int(os.environ.get('PORT', 8000)). Bind 0.0.0.0."

    if filename == "requirements.txt":
        return bersihkan_code(tanya_ai("Tulis requirements.txt Python.", f"Project: {nama_project}\n{deskripsi[:200]}\nTulis requirements.txt:" + ANTI, max_tokens=350, force_key_idx=force_key_idx))
    elif filename == "README.md":
        return bersihkan_code(tanya_ai("Tulis README.md singkat.", f"Project: {nama_project}\n{deskripsi[:200]}\nTulis README.md: judul, deskripsi, install, cara pakai. Max 60 baris.", max_tokens=1000, force_key_idx=force_key_idx))
    elif "test" in filename.lower():
        return bersihkan_code(tanya_ai("Tulis unit test pytest.", f"Project: {nama_project}\n{deskripsi[:200]}\nTulis {filename}: 3-4 test dasar." + ANTI, max_tokens=1200, force_key_idx=force_key_idx))
    
    target_lines = hitung_target_lines(filename, deskripsi)
    if target_lines == 0:
        return bersihkan_code(tanya_ai("Programmer expert.", f"Project: {nama_project}\n{deskripsi[:200]}\nTulis {filename} lengkap." + PORT + ANTI, max_tokens=1500, force_key_idx=force_key_idx))
    if target_lines <= 150:
        return bersihkan_code(tanya_ai("Programmer expert.", f"Project: {nama_project}\n{deskripsi[:200]}\nTulis {filename} ~{target_lines} baris." + PORT + ANTI, max_tokens=3000, force_key_idx=force_key_idx))
    
    return generate_file_chunked(nama_project, deskripsi, filename, daftar_file, target_lines, force_key_idx)

def generate_satu_file_with_retry(nama_project, deskripsi, filename, daftar_file, force_key_idx=None):
    last_err = ""
    for attempt in range(1, MAX_FILE_RETRY + 1):
        try:
            return generate_satu_file(nama_project, deskripsi, filename, daftar_file, force_key_idx)
        except Exception as e:
            last_err = str(e)
            if any(k in last_err for k in ["502", "503", "504", "circuit", "🔴"]) and attempt < MAX_FILE_RETRY:
                time.sleep(20 * attempt)
                circuit_force_reset()
                key_manager.reset_errors()
                continue
            raise
    raise Exception(last_err)

# ================================================
# PARALLEL GENERATION
# ================================================
def generate_files_parallel(project_id, nama_project, deskripsi, daftar_file, project_dir):
    max_workers = MAX_PARALLEL_WORKERS or max(1, len(API_KEYS))
    file_berhasil, file_gagal, lock, done_count = [], [], threading.Lock(), [0]

    def worker(filename, key_idx):
        filename = str(filename).strip().lstrip("/")
        if not filename: return None
        t_start = time.time()
        try:
            isi = generate_satu_file_with_retry(nama_project, deskripsi, filename, daftar_file, force_key_idx=key_idx)
            elapsed = time.time() - t_start
            target = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())): return None
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(isi, encoding="utf-8")
            with lock:
                file_berhasil.append(filename)
                done_count[0] += 1
                log(project_id, f"[{done_count[0]}/{len(daftar_file)}] ✓ {filename} ({len(isi)} chars, {elapsed:.1f}s)", "success")
                projects[project_id]["files"] = file_berhasil.copy()
                save_state()
        except Exception as e:
            with lock:
                file_gagal.append(filename)
                done_count[0] += 1
                log(project_id, f"[{done_count[0]}/{len(daftar_file)}] ✗ {filename}: {str(e)[:150]}", "warning")
            target = (project_dir / filename).resolve()
            if str(target).startswith(str(project_dir.resolve())):
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(f"# {filename} — GAGAL GENERATE\n# Error: {str(e)[:150]}\n", encoding="utf-8")

    log(project_id, f"🚀 Generate {len(daftar_file)} file paralel ({max_workers} workers)...", "loading")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, f, i % len(API_KEYS)): f for i, f in enumerate(daftar_file)}
        for future in as_completed(futures):
            try: future.result()
            except: pass
    return file_berhasil, file_gagal

# ================================================
# UPLOAD & MERGE SYSTEM (NEW)
# ================================================
def process_uploaded_files(upload_dir: Path) -> list:
    """Baca semua file yang diupload dan return list info"""
    files_info = []
    for fp in sorted(upload_dir.rglob("*")):
        if not fp.is_file(): continue
        rel = str(fp.relative_to(upload_dir))
        try:
            if fp.suffix in {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.ico',
                             '.zip', '.tar', '.gz', '.rar', '.7z', '.pdf', '.pyc', '.pyo',
                             '.so', '.dll', '.exe', '.bin', '.mp3', '.mp4', '.wav', '.ogg'}:
                files_info.append({"path": rel, "size": fp.stat().st_size, "type": "binary", "content": "(binary file)"})
            else:
                content = fp.read_text(encoding="utf-8", errors="ignore")
                files_info.append({"path": rel, "size": fp.stat().st_size, "type": "text", "content": content})
        except:
            files_info.append({"path": rel, "size": fp.stat().st_size, "type": "error", "content": "(unreadable)"})
    return files_info

def upload_merge_background(project_id: str, uploaded_files: list, target_project: str,
                             strategy: str, instruksi: str):
    """Background task untuk merge file yang diupload dengan AI"""
    try:
        log(project_id, "Memulai proses Upload & Merge...", "loading")

        if circuit_is_open():
            log(project_id, "🔴 Provider AI down", "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = "Provider down"
            save_state()
            return

        # Siapkan context dari file yang diupload
        upload_context = {}
        binary_files = []
        for f in uploaded_files:
            if f["type"] == "text" and f.get("content") and f["content"] != "(binary file)":
                upload_context[f["path"]] = f["content"][:3000]  # limit per file
            elif f["type"] == "binary":
                binary_files.append(f["path"])

        log(project_id, f"📖 Membaca {len(upload_context)} file text, {len(binary_files)} binary", "info")

        # Tentukan target project
        if target_project:
            project_dir = BASE_DIR / target_project
            project_dir.mkdir(parents=True, exist_ok=True)
            existing_files = baca_file_full(project_dir)
            nama = target_project
            
            # Backup jika project existing
            if existing_files:
                backup_name = backup_project(project_dir, nama)
                if backup_name:
                    log(project_id, f"Backup: {backup_name}", "info")
        else:
            # Buat project baru dari upload
            nama = f"upload_{datetime.now().strftime('%H%M%S')}"
            project_dir = BASE_DIR / nama
            project_dir.mkdir(parents=True, exist_ok=True)
            existing_files = {}

        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            projects[project_id]["nama"] = nama
            save_state()

        # Strategi merge
        log(project_id, f"🔀 Strategi: {strategy}", "info")

        if strategy == "new":
            # Buat project baru langsung dari upload
            log(project_id, "Menulis file dari upload...", "loading")
            file_berhasil = []
            for f in uploaded_files:
                if f["type"] == "text" and f.get("content") and f["content"] != "(binary file)":
                    target = (project_dir / f["path"]).resolve()
                    if str(target).startswith(str(project_dir.resolve())):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_text(f["content"], encoding="utf-8")
                        file_berhasil.append(f["path"])
                        log(project_id, f"  ✓ {f['path']}", "file")
                elif f["type"] == "binary":
                    # Copy binary files
                    src = UPLOADS_DIR / f["path"]
                    if src.exists():
                        target = (project_dir / f["path"]).resolve()
                        if str(target).startswith(str(project_dir.resolve())):
                            target.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(src, target)
                            file_berhasil.append(f["path"])

            with _log_lock:
                projects[project_id]["files"] = file_berhasil
                projects[project_id]["status"] = "done"
                save_state()
            log(project_id, f"✅ {len(file_berhasil)} file ditulis!", "done")
            return

        # Smart Merge / Integrate / Overlay — pakai AI
        log(project_id, "🧠 AI menganalisis & merge...", "loading")

        system_merge = (
            "Kamu programmer expert yang menggabungkan kode dari berbagai sumber.\n\n"
            "TUGAS: Balas HANYA JSON valid.\n"
            "TIDAK ADA ```json, TIDAK ADA markdown.\n"
            "Mulai dengan { akhiri dengan }.\n\n"
            "FORMAT:\n"
            "{\n"
            '  "analysis": "penjelasan singkat",\n'
            '  "merged_files": {"path/file.py": "kode lengkap hasil merge"},\n'
            '  "new_files": {"path/new_file.py": "kode baru"},\n'
            '  "run_cmd": "uvicorn main:app --host 0.0.0.0 --port 8000",\n'
            '  "install_cmd": "pip install -r requirements.txt",\n'
            '  "tech_stack": "Python + FastAPI",\n'
            '  "project_type": "fastapi",\n'
            '  "notes": "catatan"\n'
            "}\n\n"
            "ESCAPE: \\ → \\\\, newline → \\n, \" → \\\"\n"
        )

        context_str = json.dumps(upload_context, ensure_ascii=False, indent=2)[:6000]
        existing_str = json.dumps(existing_files, ensure_ascii=False)[:3000] if existing_files else "{}"

        strategy_desc = {
            "smart": "Gabungkan dengan cerdas — sesuaikan import, dependencies, struktur",
            "integrate": "Integrasikan ke project existing — tambahkan file baru, update yang ada",
            "overlay": "Timpa file yang namanya sama, tambahkan yang baru"
        }.get(strategy, "Gabungkan dengan cerdas")

        user_merge = (
            f"Project: {nama}\n"
            f"Strategi: {strategy_desc}\n"
            f"Instruksi user: {instruksi or 'Auto-merge oleh AI'}\n\n"
            f"FILE YANG DIUPLOAD ({len(upload_context)} file):\n{context_str}\n\n"
        )
        if existing_files:
            user_merge += f"FILE EXISTING DI PROJECT:\n{existing_str}\n\n"
        if binary_files:
            user_merge += f"FILE BINARY (copy langsung): {', '.join(binary_files[:10])}\n\n"
        user_merge += "JSON:"

        try:
            raw = tanya_ai(system_merge, user_merge, max_tokens=6000, use_cache=False)
            hasil = parse_json_toleran(raw)
        except Exception as e:
            log(project_id, f"AI gagal parse: {str(e)[:100]} — pakai direct copy", "warning")
            hasil = None

        if hasil and isinstance(hasil, dict):
            log(project_id, f"Analisis: {hasil.get('analysis', '-')}", "info")

            merged = hasil.get("merged_files", {}) or {}
            new_files = hasil.get("new_files", {}) or {}

            file_berhasil = []
            for fname, content in {**merged, **new_files}.items():
                fname = str(fname).strip().lstrip("/")
                target = (project_dir / fname).resolve()
                if not str(target).startswith(str(project_dir.resolve())): continue
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(bersihkan_code(str(content)), encoding="utf-8")
                file_berhasil.append(fname)
                label = "Merge" if fname in merged else "Baru"
                log(project_id, f"  {label}: {fname}", "file")

            # Copy binary files
            for bf in binary_files:
                src = UPLOADS_DIR / bf
                if src.exists():
                    target = (project_dir / bf).resolve()
                    if str(target).startswith(str(project_dir.resolve())):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(src, target)
                        file_berhasil.append(bf)

            run_cmd = hasil.get("run_cmd", "")
            tech_stack = hasil.get("tech_stack", "Python")
            project_type = hasil.get("project_type", "script")
            install_cmd = hasil.get("install_cmd", "pip install -r requirements.txt")
        else:
            # Fallback: direct copy semua file
            log(project_id, "Direct copy semua file...", "loading")
            file_berhasil = []
            for f in uploaded_files:
                if f["type"] == "text" and f.get("content") and f["content"] != "(binary file)":
                    target = (project_dir / f["path"]).resolve()
                    if str(target).startswith(str(project_dir.resolve())):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_text(f["content"], encoding="utf-8")
                        file_berhasil.append(f["path"])
                        log(project_id, f"  ✓ {f['path']}", "file")
            
            run_cmd = "python main.py"
            tech_stack = "Python"
            project_type = "script"
            install_cmd = "pip install -r requirements.txt"

        # Auto-add libs ke requirements.txt
        req_file = project_dir / "requirements.txt"
        if req_file.exists():
            current_req = req_file.read_text(encoding="utf-8")
            libs_add = []
            if project_type == "fastapi":
                if "fastapi" not in current_req.lower(): libs_add.append("fastapi")
                if "uvicorn" not in current_req.lower(): libs_add.append("uvicorn")
            if project_type == "flask" and "flask" not in current_req.lower():
                libs_add.append("flask")
            if libs_add:
                current_req = current_req.rstrip() + "\n" + "\n".join(libs_add) + "\n"
                req_file.write_text(current_req, encoding="utf-8")
                log(project_id, f"+ libs: {', '.join(libs_add)}", "info")

        # Save meta
        meta = {
            "nama": nama, "deskripsi": instruksi or "Upload & Merge", "tech_stack": tech_stack,
            "run_cmd": run_cmd, "install_cmd": install_cmd, "files": file_berhasil,
            "project_type": project_type, "created_at": datetime.now().isoformat(),
            "merge_strategy": strategy, "upload_count": len(uploaded_files),
        }
        (project_dir / ".ai_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        with _log_lock:
            projects[project_id]["files"] = file_berhasil
            projects[project_id]["run_cmd"] = run_cmd
            projects[project_id]["tech"] = tech_stack
            projects[project_id]["project_type"] = project_type
            projects[project_id]["status"] = "done"
            save_state()

        # Install dependencies
        if req_file.exists():
            log(project_id, "Install dependencies...", "loading")
            hasil_install = jalankan_cmd(install_cmd, str(project_dir), timeout=180)
            if hasil_install["sukses"]:
                log(project_id, "Dependencies OK!", "success")
            else:
                log(project_id, f"Warning install: {hasil_install['error'][:150]}", "warning")

        log(project_id, "=" * 40, "info")
        log(project_id, f"✅ Project '{nama}' selesai! ({len(file_berhasil)} file)", "done")
        log(project_id, "Klik ▶️ RUN untuk jalankan!", "info")

        broadcast_ws(project_id, {"type": "status", "status": "done"})

    except Exception as e:
        import traceback
        msg = f"{str(e)}\n\n{traceback.format_exc()[:800]}"
        log(project_id, f"Error: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = msg
            save_state()
        broadcast_ws(project_id, {"type": "status", "status": "error"})

# ================================================
# FIGMA MCP INTEGRATION (NEW)
# ================================================
figma_connected = False

async def check_figma_mcp():
    """Check if Figma MCP server is available"""
    global figma_connected
    try:
        # This would check actual MCP connection
        # For now, we'll simulate based on env/config
        figma_token = os.getenv("FIGMA_ACCESS_TOKEN", "")
        figma_connected = bool(figma_token)
        return figma_connected
    except:
        figma_connected = False
        return False

def figma_merge_background(project_id: str, design_url: str, target_project: str,
                            merge_mode: str, figma_url: str):
    """Background task untuk merge Figma design ke code"""
    try:
        log(project_id, "🎨 Memulai Figma Design Merge...", "figma")

        if circuit_is_open():
            log(project_id, "🔴 Provider AI down", "error")
            projects[project_id]["status"] = "error"
            save_state()
            return

        # Siapkan context
        if target_project:
            project_dir = BASE_DIR / target_project
            existing_files = baca_file_full(project_dir) if project_dir.exists() else {}
            nama = target_project
            if existing_files:
                backup_project(project_dir, nama)
        else:
            nama = f"figma_{datetime.now().strftime('%H%M%S')}"
            project_dir = BASE_DIR / nama
            project_dir.mkdir(parents=True, exist_ok=True)
            existing_files = {}

        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            projects[project_id]["nama"] = nama
            save_state()

        log(project_id, f"📸 Design: {design_url[:80]}...", "figma")
        log(project_id, f"📁 Target: {nama}", "info")
        log(project_id, f"🔀 Mode: {merge_mode}", "info")

        # AI generate code dari desain
        system_figma = (
            "Kamu programmer expert yang mengkonversi desain UI ke kode.\n\n"
            "TUGAS: Buat kode HTML/CSS/JS yang menyerupai desain yang diberikan.\n"
            "Balas HANYA JSON valid.\n\n"
            "FORMAT:\n"
            "{\n"
            '  "analysis": "deskripsi desain",\n'
            '  "files": {"index.html": "...", "style.css": "...", "script.js": "..."},\n'
            '  "run_cmd": "python -m http.server 8000",\n'
            '  "tech_stack": "HTML + CSS + JS",\n'
            '  "notes": "..."\n'
            "}\n\n"
            "ESCAPE: \\ → \\\\, newline → \\n, \" → \\\"\n"
        )

        existing_str = json.dumps(existing_files, ensure_ascii=False)[:2000] if existing_files else "{}"

        user_figma = (
            f"Project: {nama}\n"
            f"Merge mode: {merge_mode}\n"
            f"URL Figma: {figma_url or 'N/A'}\n\n"
            f"DESIGN INFO:\n"
            f"- Design URL/data tersedia: {'Ya' if design_url else 'Tidak'}\n"
            f"- Buat UI modern, responsive, dark theme\n\n"
        )
        if existing_files:
            user_figma += f"EXISTING CODE:\n{existing_str}\n\n"
        user_figma += "JSON:"

        try:
            raw = tanya_ai(system_figma, user_figma, max_tokens=6000, use_cache=False)
            hasil = parse_json_toleran(raw)
        except Exception as e:
            log(project_id, f"AI gagal: {str(e)[:100]}", "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = str(e)[:200]
            save_state()
            return

        log(project_id, f"Analisis: {hasil.get('analysis', '-')}", "figma")

        files_dict = hasil.get("files", {})
        file_berhasil = []

        for fname, content in files_dict.items():
            fname = str(fname).strip().lstrip("/")
            target = (project_dir / fname).resolve()
            if not str(target).startswith(str(project_dir.resolve())): continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(bersihkan_code(str(content)), encoding="utf-8")
            file_berhasil.append(fname)
            log(project_id, f"  ✓ {fname}", "file")

        run_cmd = hasil.get("run_cmd", "python -m http.server 8000")
        tech_stack = hasil.get("tech_stack", "HTML + CSS + JS")

        meta = {
            "nama": nama, "deskripsi": f"Figma Design: {figma_url or design_url[:50]}",
            "tech_stack": tech_stack, "run_cmd": run_cmd, "files": file_berhasil,
            "project_type": "fastapi", "created_at": datetime.now().isoformat(),
            "figma_url": figma_url, "merge_mode": merge_mode,
        }
        (project_dir / ".ai_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        with _log_lock:
            projects[project_id]["files"] = file_berhasil
            projects[project_id]["run_cmd"] = run_cmd
            projects[project_id]["tech"] = tech_stack
            projects[project_id]["status"] = "done"
            save_state()

        log(project_id, "=" * 40, "info")
        log(project_id, f"✅ Figma merge '{nama}' selesai! ({len(file_berhasil)} file)", "done")
        broadcast_ws(project_id, {"type": "status", "status": "done"})

    except Exception as e:
        import traceback
        log(project_id, f"Error: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = str(e)[:200]
            save_state()
        broadcast_ws(project_id, {"type": "status", "status": "error"})

# ================================================
# BUAT PROJECT BACKGROUND
# ================================================
def buat_project_background(project_id, deskripsi, nama):
    try:
        log(project_id, "Memulai pembuatan project...", "loading")
        if circuit_is_open():
            log(project_id, "🔴 Provider AI down", "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = "Provider down"
            save_state()
            return

        log(project_id, f"Model: {key_manager.get_info()['model']} | {len(API_KEYS)} API key", "info")
        log(project_id, "Tahap 1: AI merancang struktur...", "loading")

        system_plan = (
            "Kamu arsitek software. Balas HANYA JSON valid.\n"
            "TIDAK ADA ```json, TIDAK ADA markdown.\n"
            "Mulai dengan { akhiri dengan }.\n\n"
            "FORMAT:\n"
            '{"description":"...","tech_stack":"...","files":["file1.py","file2.py"],'
            '"install_cmd":"...","run_cmd":"...","test_cmd":"...","project_type":"script"}\n\n'
            f"MAKSIMAL {MAX_FILES_PER_PROJ} file. Balas HANYA JSON."
        )

        plan = None
        try:
            raw_plan = tanya_ai(system_plan, f"Nama: {nama}\nDeskripsi: {deskripsi[:300]}\nJSON:", max_tokens=1500, use_cache=False)
            plan = parse_json_toleran(raw_plan)
            log(project_id, "✓ Planning JSON valid", "success")
        except Exception as e:
            if any(kw in str(e) for kw in ["🔴", "circuit", "502", "503", "504"]):
                plan = buat_default_plan(nama, deskripsi)
            else:
                try:
                    raw_retry = tanya_ai("JSON only. Start { end }.", f"Project: {nama} - {deskripsi[:150]}\nJSON:", max_tokens=1000, use_cache=False)
                    plan = parse_json_toleran(raw_retry)
                except:
                    plan = buat_default_plan(nama, deskripsi)

        if not isinstance(plan, dict):
            plan = buat_default_plan(nama, deskripsi)

        daftar_file = (plan.get("files") or ["main.py", "requirements.txt", "README.md"])[:MAX_FILES_PER_PROJ]
        run_cmd = plan.get("run_cmd") or "python main.py"
        tech_stack = plan.get("tech_stack") or "Python"
        project_type = (plan.get("project_type") or "script").lower()
        if project_type not in ["cli", "fastapi", "flask", "script", "node"]:
            project_type = "script"

        log(project_id, f"Struktur: {len(daftar_file)} file | Tipe: {project_type}", "info")

        project_dir = BASE_DIR / nama
        project_dir.mkdir(parents=True, exist_ok=True)
        for f in daftar_file:
            (project_dir / f).parent.mkdir(parents=True, exist_ok=True)

        with _log_lock:
            projects[project_id].update({
                "folder": str(project_dir.resolve()), "tech": tech_stack,
                "desc": plan.get("description", deskripsi), "project_type": project_type
            })
            save_state()

        log(project_id, "Tahap 2: Generate file...", "loading")

        use_parallel = ENABLE_PARALLEL_GENERATION and len(daftar_file) >= PARALLEL_MIN_FILES
        if use_parallel:
            file_berhasil, file_gagal = generate_files_parallel(project_id, nama, deskripsi, daftar_file, project_dir)
        else:
            file_berhasil, file_gagal = [], []
            upstream_fail_count = 0
            max_fail = max(3, int(len(daftar_file) * MAX_UPSTREAM_FAIL_PERCENT))

            for idx, filename in enumerate(daftar_file, 1):
                filename = str(filename).strip().lstrip("/")
                if not filename: continue
                if upstream_fail_count >= max_fail:
                    for remaining in daftar_file[idx-1:]:
                        remaining = str(remaining).strip().lstrip("/")
                        if remaining and remaining not in file_berhasil:
                            file_gagal.append(remaining)
                    break

                log(project_id, f"[{idx}/{len(daftar_file)}] {filename}...", "loading")
                t_start = time.time()
                try:
                    isi = generate_satu_file_with_retry(nama, deskripsi, filename, daftar_file)
                    elapsed = time.time() - t_start
                    log(project_id, f"  ✓ {len(isi)} chars ({elapsed:.1f}s)", "success")
                    upstream_fail_count = 0
                except Exception as e:
                    err_msg = str(e)[:200]
                    if any(kw in err_msg for kw in ["🔴", "circuit", "502", "503", "504"]):
                        upstream_fail_count += 1
                    file_gagal.append(filename)
                    isi = f"# {filename} — GAGAL GENERATE\n# Error: {err_msg}\n"

                target = (project_dir / filename).resolve()
                if str(target).startswith(str(project_dir.resolve())):
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(isi, encoding="utf-8")
                file_berhasil.append(filename)
                with _log_lock:
                    projects[project_id]["files"] = file_berhasil.copy()
                    save_state()

        # Auto-add libs
        req_file = project_dir / "requirements.txt"
        if req_file.exists():
            current_req = req_file.read_text(encoding="utf-8")
            libs_add = []
            if project_type == "fastapi":
                for lib in ["fastapi", "uvicorn", "jinja2"]:
                    if lib not in current_req.lower(): libs_add.append(lib)
            if project_type == "flask" and "flask" not in current_req.lower():
                libs_add.append("flask")
            if libs_add:
                req_file.write_text(current_req.rstrip() + "\n" + "\n".join(libs_add) + "\n", encoding="utf-8")

        with _log_lock:
            projects[project_id]["files"] = file_berhasil
            projects[project_id]["run_cmd"] = run_cmd
            save_state()

        meta = {"nama": nama, "deskripsi": deskripsi, "tech_stack": tech_stack, "run_cmd": run_cmd,
                "install_cmd": plan.get("install_cmd", "pip install -r requirements.txt"),
                "test_cmd": plan.get("test_cmd", "pytest"), "files": file_berhasil,
                "project_type": project_type, "created_at": datetime.now().isoformat(), "failed_files": file_gagal}
        (project_dir / ".ai_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        if len(file_gagal) < len(daftar_file) / 2 and req_file.exists():
            log(project_id, "Install dependencies...", "loading")
            hasil_install = jalankan_cmd(meta["install_cmd"], str(project_dir), timeout=180)
            log(project_id, "Dependencies OK!" if hasil_install["sukses"] else f"Warning: {hasil_install['error'][:150]}", "success" if hasil_install["sukses"] else "warning")

        log(project_id, "=" * 40, "info")
        if file_gagal:
            log(project_id, f"Project '{nama}' selesai (partial: {len(file_gagal)} gagal)!", "warning")
        else:
            log(project_id, f"Project '{nama}' selesai!", "done")
        log(project_id, "Klik ▶️ RUN untuk jalankan!", "info")

        with _log_lock:
            projects[project_id]["status"] = "done" if not file_gagal else "partial"
            save_state()
        broadcast_ws(project_id, {"type": "status", "status": "done"})

    except Exception as e:
        import traceback
        log(project_id, f"Error fatal: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = f"{str(e)}\n\n{traceback.format_exc()[:800]}"
            save_state()
        broadcast_ws(project_id, {"type": "status", "status": "error"})

# ================================================
# LANJUT PROJECT
# ================================================
def lanjut_project_background(project_id, nama, permintaan):
    try:
        project_dir = BASE_DIR / nama
        if not project_dir.exists():
            log(project_id, f"Folder '{nama}' tidak ada", "error")
            projects[project_id]["status"] = "error"
            save_state()
            return

        if circuit_is_open():
            log(project_id, "🔴 Provider AI down", "error")
            projects[project_id]["status"] = "error"
            save_state()
            return

        log(project_id, "Backup project...", "loading")
        backup_name = backup_project(project_dir, nama)
        if backup_name:
            log(project_id, f"Backup: {backup_name}", "info")

        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            save_state()

        meta = {}
        meta_file = project_dir / ".ai_meta.json"
        if meta_file.exists():
            try: meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except: pass

        semua_file = baca_semua_file(project_dir, max_chars=400)
        failed_files = meta.get("failed_files", [])

        if failed_files:
            log(project_id, f"Regenerate {len(failed_files)} file gagal...", "lanjut")
            daftar_semua = meta.get("files", []) or list(semua_file.keys())
            regenerated, regen_failed = [], []
            for fname in failed_files:
                fname = str(fname).strip().lstrip("/")
                log(project_id, f"Regenerate: {fname}...", "loading")
                try:
                    isi = generate_satu_file_with_retry(nama, meta.get("deskripsi", permintaan), fname, daftar_semua)
                    target = (project_dir / fname).resolve()
                    if str(target).startswith(str(project_dir.resolve())):
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_text(isi, encoding="utf-8")
                        regenerated.append(fname)
                        log(project_id, f"  ✓ {fname}", "success")
                except Exception as e:
                    regen_failed.append(fname)
                    log(project_id, f"  ✗ {fname}: {str(e)[:100]}", "warning")

            meta["failed_files"] = regen_failed
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            if not permintaan or permintaan.strip() == "":
                with _log_lock:
                    projects[project_id]["status"] = "done" if not regen_failed else "partial"
                    save_state()
                return

        system_lanjut = (
            "Kamu programmer expert. Balas HANYA JSON valid.\n"
            "TIDAK ADA ```json, TIDAK ADA markdown.\n"
            "Mulai dengan { akhiri dengan }.\n\n"
            "ESCAPE: \\ → \\\\, newline → \\n, \" → \\\"\n\n"
            '{"analysis":"...","new_files":{"file.py":"code"},"modified_files":{"main.py":"code"},'
            '"run_cmd":"python main.py","notes":"..."}\n\n'
            "HANYA JSON."
        )

        context = {}
        total_ch = 0
        for fname, fcontent in semua_file.items():
            if total_ch > 3000: break
            context[fname] = fcontent
            total_ch += len(fcontent)

        log(project_id, "AI mengembangkan...", "loading")
        try:
            raw = tanya_ai(system_lanjut, f"Project: {nama} | Tech: {meta.get('tech_stack', 'Python')}\nPermintaan: {permintaan}\nFile:\n{json.dumps(context, ensure_ascii=False)}\nJSON:", max_tokens=5000, use_cache=False)
            hasil = parse_json_toleran(raw)
        except Exception as e:
            if any(kw in str(e) for kw in ["🔴", "circuit", "502", "503", "504"]):
                log(project_id, f"Provider down: {str(e)[:200]}", "error")
                projects[project_id]["status"] = "error"
                save_state()
                return
            try:
                raw_retry = tanya_ai("JSON ONLY. Start { end }.", f"Project: {nama}\nPermintaan: {permintaan[:200]}\nJSON:", max_tokens=4000, use_cache=False)
                hasil = parse_json_toleran(raw_retry)
            except Exception as e2:
                log(project_id, f"AI gagal 2x: {str(e2)[:200]}", "error")
                projects[project_id]["status"] = "error"
                save_state()
                return

        log(project_id, f"Analisis: {hasil.get('analysis', '-')}", "info")

        new_files = hasil.get("new_files", {}) or {}
        mod_files = hasil.get("modified_files", {}) or {}

        for filename, content in {**new_files, **mod_files}.items():
            filename = str(filename).strip().lstrip("/")
            target = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())): continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(bersihkan_code(str(content)), encoding="utf-8")
            log(project_id, f"{'Baru' if filename in new_files else 'Update'}: {filename}", "file")

        all_files = list(semua_file.keys())
        for f in list(new_files.keys()) + list(mod_files.keys()):
            if f not in all_files: all_files.append(f)

        run_cmd = hasil.get("run_cmd") or meta.get("run_cmd", "")
        meta.update({"files": all_files, "run_cmd": run_cmd, "last_modified": datetime.now().isoformat()})
        meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        with _log_lock:
            projects[project_id].update({"files": all_files, "run_cmd": run_cmd, "tech": meta.get("tech_stack", ""), "nama": nama, "project_type": meta.get("project_type", "script")})
            save_state()

        log(project_id, "Pengembangan selesai!", "done")
        with _log_lock:
            projects[project_id]["status"] = "done"
            save_state()

    except Exception as e:
        import traceback
        log(project_id, f"Error: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"] = f"{str(e)}\n\n{traceback.format_exc()[:800]}"
            save_state()

# ================================================
# RUN PROJECT
# ================================================
def cari_port_kosong():
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
            except: return port
    return PORT_START

def stop_project(project_id):
    with _proc_lock:
        info = running_processes.get(project_id)
        if not info: return False
        proc = info.get("process")
        if proc and proc.poll() is None:
            try:
                proc.terminate() if sys.platform == "win32" else proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=5)
            except: 
                try: proc.kill()
                except: pass
        del running_processes[project_id]
        return True

def stream_output(project_id, proc):
    try:
        for line in iter(proc.stdout.readline, ''):
            if not line: break
            with _proc_lock:
                if project_id in running_processes:
                    running_processes[project_id]["logs"].append(line.rstrip())
                    if len(running_processes[project_id]["logs"]) > 500:
                        running_processes[project_id]["logs"] = running_processes[project_id]["logs"][-500:]
            broadcast_ws(project_id, {"type": "runlog", "line": line.rstrip()})
    except: pass

def run_project(project_id, project_dir, run_cmd, project_type):
    stop_project(project_id)
    port = cari_port_kosong()
    env = os.environ.copy()
    env["PORT"] = str(port)
    env["HOST"] = "127.0.0.1"

    if project_type == "fastapi":
        if "uvicorn" not in run_cmd.lower():
            for candidate in ["main:app", "app:app"]:
                if (project_dir / f"{candidate.split(':')[0]}.py").exists():
                    run_cmd = f"uvicorn {candidate} --host 127.0.0.1 --port {port}"
                    break
        else:
            run_cmd = re.sub(r"--port\s+\d+", f"--port {port}", run_cmd)
            run_cmd = re.sub(r"--host\s+\S+", "--host 127.0.0.1", run_cmd)
            if "--port" not in run_cmd: run_cmd += f" --port {port}"
    elif project_type == "flask":
        env["FLASK_RUN_PORT"] = str(port)
        env["FLASK_RUN_HOST"] = "127.0.0.1"

    try:
        proc = subprocess.Popen(run_cmd, cwd=str(project_dir), shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
        with _proc_lock:
            running_processes[project_id] = {
                "process": proc, "port": port,
                "logs": [f"[SYSTEM] {run_cmd}", f"[SYSTEM] Port: {port}"],
                "started_at": time.time(), "run_cmd": run_cmd, "type": project_type, "last_access": time.time()
            }
        threading.Thread(target=stream_output, args=(project_id, proc), daemon=True).start()
        time.sleep(2)
        if proc.poll() is not None and proc.poll() != 0:
            return {"sukses": False, "error": f"Process berhenti (exit: {proc.poll()})", "logs": running_processes.get(project_id, {}).get("logs", [])[-20:]}
        is_web = project_type in ["fastapi", "flask"]
        return {"sukses": True, "port": port, "run_cmd": run_cmd, "type": project_type, "url": f"/proxy/{project_id}/" if is_web else None}
    except Exception as e:
        return {"sukses": False, "error": str(e)}

# ================================================
# AUTO CLEANUP
# ================================================
def cleanup_idle_processes():
    while True:
        try:
            time.sleep(300)
            now = time.time()
            with _proc_lock:
                idle_pids = [pid for pid, info in running_processes.items() if (now - info.get("last_access", now)) / 60 > AUTO_CLEANUP_MIN]
            for pid in idle_pids:
                stop_project(pid)
        except: pass

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
            resp = client.messages.create(model=k["model"], max_tokens=64, messages=[{"role": "user", "content": "Balas: OK"}])
            hasil.append({"index": i, "status": "✅ sukses", "model": k["model"], "reply": ambil_text(resp)})
        except Exception as e:
            hasil.append({"index": i, "status": "❌ gagal", "model": k["model"], "error": str(e)})
    return JSONResponse({"keys": hasil})

@app.get("/health-upstream")
async def health_upstream():
    hasil = []
    for i, k in enumerate(API_KEYS):
        try:
            buat_client(k["base_url"], k["api_key"]).messages.create(model=k["model"], max_tokens=10, messages=[{"role": "user", "content": "hi"}])
            hasil.append({"index": i, "model": k["model"], "status": "✅ healthy"})
        except anthropic.APIStatusError as e:
            hasil.append({"index": i, "model": k["model"], "status": f"⚠️ {e.status_code}", "error": str(e.message)[:150]})
        except Exception as e:
            hasil.append({"index": i, "model": k["model"], "status": "❌ error", "error": str(e)[:150]})
    return JSONResponse({"keys": hasil})

@app.get("/circuit-status")
async def circuit_status():
    with _circuit_lock:
        state = dict(_circuit_state)
    remaining = max(0, int(CIRCUIT_BREAKER_TIMEOUT - (time.time() - state["opened_at"]))) if state["is_open"] else 0
    return JSONResponse({"is_open": state["is_open"], "fail_count": state["fail_count"], "total_opens": state["total_opens"],
                         "reset_in_sec": remaining, "threshold": CIRCUIT_BREAKER_THRESHOLD, "timeout_sec": CIRCUIT_BREAKER_TIMEOUT})

@app.post("/circuit-reset")
async def circuit_reset():
    circuit_force_reset()
    key_manager.reset_errors()
    return JSONResponse({"success": True, "message": "✅ Circuit & key errors direset."})

@app.get("/stats")
async def get_stats():
    with _stats_lock: stats = dict(request_stats)
    with _cache_lock: cache_size = len(ai_cache)
    with _proc_lock: running_count = len(running_processes)
    total_projects = len(list(BASE_DIR.iterdir())) if BASE_DIR.exists() else 0
    total_backups = len(list(BACKUPS_DIR.iterdir())) if BACKUPS_DIR.exists() else 0
    avg_time = stats["total"]["total_time"] / stats["total"]["success"] if stats.get("total", {}).get("success", 0) > 0 else 0
    return JSONResponse({
        "requests": {"total": stats.get("total", {}).get("count", 0), "success": stats.get("total", {}).get("success", 0),
                     "failed": stats.get("total", {}).get("failed", 0), "avg_time": round(avg_time, 2),
                     "cache_hits": stats.get("cache_hit", {}).get("count", 0)},
        "cache": {"size": cache_size, "max": 200},
        "projects": {"total": total_projects, "running": running_count, "backups": total_backups},
        "system": {"port_range": f"{PORT_START}-{PORT_END}", "rate_limit": f"{RATE_LIMIT_PER_MIN}/min",
                   "cache_ttl": f"{CACHE_TTL_SECONDS}s", "parallel_enabled": ENABLE_PARALLEL_GENERATION,
                   "api_key_count": len(API_KEYS)}
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
        target_dir = BASE_DIR / project_name
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
    backup_path.unlink()
    return JSONResponse({"success": True})

@app.post("/cache/clear")
async def clear_cache():
    with _cache_lock:
        count = len(ai_cache)
        ai_cache.clear()
    return JSONResponse({"cleared": count})

# ================================================
# PROJECT ROUTES
# ================================================
@app.post("/buat-project")
async def buat_project_route(request: Request, deskripsi: str = Form(...), nama: str = Form(...)):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse({"error": f"Rate limit! Max {RATE_LIMIT_PER_MIN}/menit"}, status_code=429)
    if circuit_is_open():
        with _circuit_lock:
            remaining = max(0, int(CIRCUIT_BREAKER_TIMEOUT - (time.time() - _circuit_state["opened_at"])))
        return JSONResponse({"error": f"🔴 Provider AI down. Coba dalam {remaining}s"}, status_code=503)

    nama = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project_kuliah"
    project_id = str(uuid.uuid4())[:8]
    with _log_lock:
        projects[project_id] = {"status": "loading", "logs": [], "files": [], "folder": "", "error": "",
                                "run_cmd": "", "tech": "", "desc": "", "nama": nama, "project_type": "script",
                                "created_at": datetime.now().isoformat()}
        save_state()
    threading.Thread(target=buat_project_background, args=(project_id, deskripsi, nama), daemon=True).start()
    return JSONResponse({"project_id": project_id, "nama": nama})

@app.post("/lanjut-project")
async def lanjut_project_route(request: Request, nama: str = Form(...), permintaan: str = Form(...)):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse({"error": f"Rate limit! Max {RATE_LIMIT_PER_MIN}/menit"}, status_code=429)
    if circuit_is_open():
        return JSONResponse({"error": "🔴 Provider AI down"}, status_code=503)

    nama = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project"
    project_id = str(uuid.uuid4())[:8]
    with _log_lock:
        projects[project_id] = {"status": "loading", "logs": [], "files": [], "folder": "", "error": "",
                                "run_cmd": "", "tech": "", "desc": "", "nama": nama, "project_type": "script",
                                "created_at": datetime.now().isoformat()}
        save_state()
    threading.Thread(target=lanjut_project_background, args=(project_id, nama, permintaan), daemon=True).start()
    return JSONResponse({"project_id": project_id, "nama": nama})

# ================================================
# UPLOAD & MERGE ROUTES (NEW)
# ================================================
@app.post("/upload-merge")
async def upload_merge_route(
    request: Request,
    files: List[UploadFile] = File(...),
    files_info: str = Form("[]"),
    strategy: str = Form("smart"),
    target_project: str = Form(""),
    instruksi: str = Form(""),
    file_count: int = Form(0),
):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse({"error": f"Rate limit! Max {RATE_LIMIT_PER_MIN}/menit"}, status_code=429)

    project_id = str(uuid.uuid4())[:8]
    nama = target_project or f"upload_{datetime.now().strftime('%H%M%S')}"

    with _log_lock:
        projects[project_id] = {"status": "loading", "logs": [], "files": [], "folder": "", "error": "",
                                "run_cmd": "", "tech": "", "desc": instruksi or "Upload & Merge",
                                "nama": nama, "project_type": "script", "created_at": datetime.now().isoformat()}
        save_state()

    # Simpan file yang diupload ke uploads/
    upload_session_dir = UPLOADS_DIR / project_id
    upload_session_dir.mkdir(parents=True, exist_ok=True)

    uploaded_files_info = []
    for file in files:
        try:
            # Simpan file
            file_path = upload_session_dir / file.filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            content = await file.read()
            file_path.write_bytes(content)

            # Detect type
            ext = Path(file.filename).suffix.lower()
            binary_exts = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.ico', '.svg',
                          '.zip', '.tar', '.gz', '.rar', '.7z', '.pdf', '.pyc', '.pyo',
                          '.so', '.dll', '.exe', '.bin', '.mp3', '.mp4', '.wav', '.ogg',
                          '.woff', '.woff2', '.ttf', '.eot'}

            if ext in binary_exts:
                uploaded_files_info.append({
                    "path": file.filename, "size": len(content), "type": "binary",
                    "content": "(binary file)"
                })
            else:
                try:
                    text_content = content.decode("utf-8", errors="ignore")
                    uploaded_files_info.append({
                        "path": file.filename, "size": len(content), "type": "text",
                        "content": text_content
                    })
                except:
                    uploaded_files_info.append({
                        "path": file.filename, "size": len(content), "type": "error",
                        "content": "(unreadable)"
                    })
        except Exception as e:
            print(f"[UPLOAD] Error processing {file.filename}: {e}")

    log(project_id, f"📤 {len(uploaded_files_info)} file diterima", "info")

    threading.Thread(
        target=upload_merge_background,
        args=(project_id, uploaded_files_info, target_project, strategy, instruksi),
        daemon=True
    ).start()

    return JSONResponse({"project_id": project_id, "nama": nama, "file_count": len(uploaded_files_info)})

# ================================================
# FIGMA ROUTES (NEW)
# ================================================
@app.get("/figma/status")
async def figma_status():
    await check_figma_mcp()
    return JSONResponse({"connected": figma_connected, "mcp_url": FIGMA_MCP_URL})

@app.post("/figma/screenshot")
async def figma_screenshot(request: Request):
    """Get screenshot from Figma URL via MCP"""
    data = await request.json()
    figma_url = data.get("url", "")
    
    if not figma_url:
        return JSONResponse({"error": "URL required"}, status_code=400)

    # In real implementation, this would call Figma MCP API
    # For now, return a placeholder
    return JSONResponse({
        "success": True,
        "message": "Figma screenshot akan tersedia dengan MCP server yang terhubung",
        "image_url": None,
        "figma_url": figma_url
    })

@app.post("/figma-merge")
async def figma_merge_route(
    request: Request,
    design_file: Optional[UploadFile] = File(None),
    design_url: str = Form(""),
    design_type: str = Form(""),
    merge_mode: str = Form("replace"),
    target_project: str = Form(""),
    figma_url: str = Form(""),
):
    client_ip = request.client.host
    if not check_rate_limit(client_ip):
        return JSONResponse({"error": f"Rate limit! Max {RATE_LIMIT_PER_MIN}/menit"}, status_code=429)

    project_id = str(uuid.uuid4())[:8]
    nama = target_project or f"figma_{datetime.now().strftime('%H%M%S')}"

    with _log_lock:
        projects[project_id] = {"status": "loading", "logs": [], "files": [], "folder": "", "error": "",
                                "run_cmd": "", "tech": "", "desc": f"Figma Design Merge",
                                "nama": nama, "project_type": "script", "created_at": datetime.now().isoformat()}
        save_state()

    # Simpan design file jika ada
    design_data_url = design_url
    if design_file:
        upload_dir = UPLOADS_DIR / project_id
        upload_dir.mkdir(parents=True, exist_ok=True)
        content = await design_file.read()
        design_path = upload_dir / design_file.filename
        design_path.write_bytes(content)
        import base64
        mime = design_file.content_type or "image/png"
        design_data_url = f"data:{mime};base64,{base64.b64encode(content).decode()}"

    threading.Thread(
        target=figma_merge_background,
        args=(project_id, design_data_url, target_project, merge_mode, figma_url),
        daemon=True
    ).start()

    return JSONResponse({"project_id": project_id, "nama": nama})

@app.post("/figma-sync-design")
async def figma_sync_design(request: Request):
    data = await request.json()
    project_id = data.get("project_id", "")
    design_url = data.get("design_url", "")
    
    if project_id not in projects:
        return JSONResponse({"error": "Project not found"}, status_code=404)
    
    # In real implementation, this would sync with Figma MCP
    return JSONResponse({"success": True, "message": "Design sync initiated"})

# ================================================
# FILE MANAGEMENT ROUTES
# ================================================
@app.get("/status/{project_id}")
async def cek_status(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Tidak ditemukan"}, status_code=404)
    data = dict(projects[project_id])
    with _proc_lock:
        info = running_processes.get(project_id)
        if info:
            proc = info["process"]
            is_web = info.get("type") in ["fastapi", "flask"]
            data["running"] = {"alive": proc.poll() is None, "port": info["port"],
                              "url": f"/proxy/{project_id}/" if is_web else None,
                              "started_at": info["started_at"], "uptime": int(time.time() - info["started_at"]),
                              "type": info.get("type")}
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
        try: isi = fp.read_text(encoding="utf-8", errors="ignore")
        except: isi = "(tidak bisa dibaca)"
        hasil.append({"path": rel, "content": isi, "size": fp.stat().st_size})
    return JSONResponse({"files": hasil})

@app.post("/files/{project_id}/save")
async def save_file_route(project_id: str, path: str = Form(...), content: str = Form(...)):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder = projects[project_id].get("folder", "")
    if not folder: return JSONResponse({"error": "No folder"}, status_code=400)
    project_dir = Path(folder)
    target = (project_dir / path.lstrip("/")).resolve()
    if not str(target).startswith(str(project_dir.resolve())):
        return JSONResponse({"error": "Path unsafe"}, status_code=400)
    backup_project(project_dir, projects[project_id]["nama"])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return JSONResponse({"success": True})

@app.delete("/files/{project_id}/delete")
async def delete_file_route(project_id: str, path: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder = projects[project_id].get("folder", "")
    target = (Path(folder) / path.lstrip("/")).resolve()
    if not str(target).startswith(str(Path(folder).resolve())):
        return JSONResponse({"error": "Path unsafe"}, status_code=400)
    try:
        target.unlink()
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/list-projects")
async def list_projects_route():
    hasil = []
    if not BASE_DIR.exists(): return JSONResponse({"projects": []})
    for folder in sorted(BASE_DIR.iterdir()):
        if not folder.is_dir(): continue
        meta = {}
        meta_file = folder / ".ai_meta.json"
        if meta_file.exists():
            try: meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except: pass
        file_count = sum(1 for f in folder.rglob("*") if f.is_file() and f.name != ".ai_meta.json" and not any(s in f.parts for s in SKIP_DIRS))
        is_running = False
        with _proc_lock:
            for pid in running_processes:
                if projects.get(pid, {}).get("nama") == folder.name:
                    is_running = True
                    break
        hasil.append({"nama": folder.name, "tech_stack": meta.get("tech_stack", "-"), "deskripsi": meta.get("deskripsi", "-"),
                      "run_cmd": meta.get("run_cmd", "-"), "project_type": meta.get("project_type", "script"),
                      "file_count": file_count, "created_at": meta.get("created_at", ""), "is_running": is_running,
                      "failed_files": meta.get("failed_files", [])})
    return JSONResponse({"projects": hasil})

@app.delete("/project/{nama}")
async def delete_project(nama: str):
    project_dir = BASE_DIR / nama
    if not project_dir.exists():
        return JSONResponse({"error": "Not found"}, status_code=404)
    with _proc_lock:
        pids_to_stop = [pid for pid in running_processes if projects.get(pid, {}).get("nama") == nama]
    for pid in pids_to_stop:
        stop_project(pid)
    backup_project(project_dir, nama + "_before_delete")
    shutil.rmtree(project_dir)
    return JSONResponse({"success": True, "backup": True})

@app.get("/key-status")
async def key_status_route():
    return JSONResponse({"current_index": key_manager.current_idx, "keys": key_manager.status()})

@app.post("/key-reset")
async def key_reset_route():
    key_manager.reset_errors()
    return JSONResponse({"message": "Reset OK"})

# ================================================
# RUN/STOP/RESTART ROUTES
# ================================================
@app.post("/run/{project_id}")
async def run_project_route(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    pd = projects[project_id]
    folder = pd.get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"error": "Folder tidak ada"}, status_code=404)
    run_cmd = pd.get("run_cmd", "")
    if not run_cmd:
        meta_file = Path(folder) / ".ai_meta.json"
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                run_cmd = meta.get("run_cmd", "python main.py")
            except: run_cmd = "python main.py"
    return JSONResponse(run_project(project_id, Path(folder), run_cmd, pd.get("project_type", "script")))

@app.post("/stop/{project_id}")
async def stop_project_route(project_id: str):
    stopped = stop_project(project_id)
    return JSONResponse({"success": stopped, "message": "Dihentikan" if stopped else "No process"})

@app.post("/restart/{project_id}")
async def restart_project_route(project_id: str):
    stop_project(project_id)
    time.sleep(1)
    return await run_project_route(project_id)

@app.get("/run-logs/{project_id}")
async def run_logs_route(project_id: str):
    with _proc_lock:
        info = running_processes.get(project_id)
        if not info: return JSONResponse({"logs": [], "alive": False})
        info["last_access"] = time.time()
        proc = info["process"]
        is_web = info.get("type") in ["fastapi", "flask"]
        return JSONResponse({"logs": info["logs"][-200:], "alive": proc.poll() is None, "port": info["port"],
                            "url": f"/proxy/{project_id}/" if is_web else None, "exit_code": proc.poll(),
                            "run_cmd": info.get("run_cmd", ""), "uptime": int(time.time() - info["started_at"]),
                            "type": info.get("type")})

@app.get("/download/{project_id}")
async def download_project(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Not found"}, status_code=404)
    folder = projects[project_id].get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"error": "No folder"}, status_code=404)
    project_dir = Path(folder)
    nama = projects[project_id].get("nama", "project")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in project_dir.rglob("*"):
            if not fp.is_file(): continue
            if any(s in fp.parts for s in SKIP_DIRS): continue
            zf.write(fp, str(fp.relative_to(project_dir)))
    zip_buffer.seek(0)
    return StreamingResponse(zip_buffer, media_type="application/zip",
                            headers={"Content-Disposition": f'attachment; filename="{nama}.zip"'})

@app.get("/running-list")
async def running_list_route():
    hasil = []
    with _proc_lock:
        for pid, info in running_processes.items():
            proc = info["process"]
            is_web = info.get("type") in ["fastapi", "flask"]
            hasil.append({"project_id": pid, "port": info["port"],
                         "url": f"/proxy/{pid}/" if is_web else None,
                         "alive": proc.poll() is None, "uptime": int(time.time() - info["started_at"]),
                         "type": info.get("type"), "run_cmd": info.get("run_cmd", "")})
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
    except (WebSocketDisconnect, Exception):
        with _ws_lock:
            websocket_connections[project_id].discard(websocket)

# ================================================
# PROXY
# ================================================
@app.api_route("/proxy/{project_id}", methods=["GET","POST","PUT","DELETE","PATCH","OPTIONS","HEAD"])
@app.api_route("/proxy/{project_id}/{path:path}", methods=["GET","POST","PUT","DELETE","PATCH","OPTIONS","HEAD"])
async def proxy_to_project(project_id: str, request: Request, path: str = ""):
    with _proc_lock:
        info = running_processes.get(project_id)
        if info: info["last_access"] = time.time()

    if not info:
        return HTMLResponse(f"""<html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
            <h2>⚠️ Project belum di-RUN</h2><p>Klik ▶️ RUN untuk menjalankan.</p></body></html>""", status_code=503)

    port = info.get("port")
    proc = info.get("process")
    if proc and proc.poll() is not None:
        return HTMLResponse(f"""<html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
            <h2>❌ Project berhenti (exit: {proc.poll()})</h2></body></html>""", status_code=503)

    target_url = f"http://127.0.0.1:{port}/{path}"
    if request.url.query: target_url += "?" + request.url.query
    body = await request.body()
    headers = dict(request.headers)
    for h in ["host", "content-length", "connection", "accept-encoding"]:
        headers.pop(h, None)

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False, follow_redirects=False) as client:
            resp = await client.request(method=request.method, url=target_url, headers=headers, content=body)
        
        resp_headers = {k: v for k, v in resp.headers.items()
                       if k.lower() not in ["content-encoding", "transfer-encoding", "connection", "x-frame-options", "content-security-policy"]}
        
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location", "")
            if location.startswith("/"):
                resp_headers["location"] = f"/proxy/{project_id}{location}"
            elif location.startswith(f"http://127.0.0.1:{port}"):
                resp_headers["location"] = f"/proxy/{project_id}{location.replace(f'http://127.0.0.1:{port}', '')}"

        return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers,
                       media_type=resp.headers.get("content-type"))
    except httpx.ConnectError:
        return HTMLResponse(f"""<html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
            <h2>⏳ Loading...</h2><p>Server belum siap di port {port}.</p>
            <button onclick="location.reload()" style="padding:10px 20px;background:#6366f1;color:#fff;border:none;border-radius:6px;cursor:pointer">🔄 Reload</button>
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
            except: pass
        running_processes.clear()

# ================================================
# STARTUP
# ================================================
print("=" * 60)
print("🚀 AI Project Maker ULTIMATE Edition v3.0")
print("=" * 60)
print(f"✨ Cache TTL: {CACHE_TTL_SECONDS}s")
print(f"🛡️  Rate limit: {RATE_LIMIT_PER_MIN}/menit per IP")
print(f"💾 Auto backup: sebelum modifikasi")
print(f"🔴 Circuit breaker: threshold={CIRCUIT_BREAKER_THRESHOLD} | timeout={CIRCUIT_BREAKER_TIMEOUT}s")
print(f"⚡ Upstream retry: {UPSTREAM_MAX_RETRY}x | backoff cap {UPSTREAM_MAX_WAIT}s")
print(f"✂️  Max lines/file: {MAX_LINES_PER_FILE} | chunking otomatis")
print(f"🚀 Parallel gen: {'ON' if ENABLE_PARALLEL_GENERATION else 'OFF'} ({len(API_KEYS)} API key)")
print(f"🎨 Figma MCP: {FIGMA_MCP_URL}")
print(f"📤 Upload & Merge: ON (uploads/{UPLOADS_DIR})")
print("=" * 60)
