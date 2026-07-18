# ================================================
# LOAD ENV — HARUS DI PALING ATAS
# ================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv belum terinstall, jalankan: pip install python-dotenv")

try:
    from cryptography.fernet import Fernet
except ImportError:
    print("⚠️  cryptography belum terinstall, jalankan: pip install cryptography")
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
import httpx
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
import anthropic

# ================================================
# DECRYPT API KEY
# ================================================
def decrypt_key(encrypted: str) -> str:
    if not encrypted:
        return ""
    if Fernet is None:
        print("[DECRYPT] ❌ cryptography tidak terinstall")
        return ""
    try:
        enc_key = os.getenv("ENCRYPTION_KEY", "")
        if not enc_key:
            return encrypted
        f = Fernet(enc_key.encode())
        return f.decrypt(encrypted.encode()).decode()
    except Exception as e:
        print(f"[DECRYPT] ❌ Gagal decrypt: {e}")
        return ""

# ================================================
# SETTING — BACA DARI .env
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
        _raw_keys.append({
            "base_url" : BASE_URL,
            "api_key"  : api_key,
            "model"    : model,
        })
    i += 1

if not _raw_keys:
    _raw_keys = [{
        "base_url" : BASE_URL,
        "api_key"  : os.getenv("API_KEY_1", ""),
        "model"    : "claude-sonnet-4-5",
    }]

API_KEYS = [k for k in _raw_keys if k["api_key"]]

if not API_KEYS:
    raise ValueError("❌ Tidak ada API Key yang valid! Cek .env")

print(f"✅ {len(API_KEYS)} API Key berhasil dimuat")
for idx, k in enumerate(API_KEYS):
    hint = k["api_key"][:8] + "..." if len(k["api_key"]) > 8 else k["api_key"]
    print(f"   [{idx}] model={k['model']} | key={hint}")

# ================================================
# KONSTANTA
# ================================================
MAX_FIX             = 2
BASE_DIR            = Path("workspace")
BASE_DIR.mkdir(exist_ok=True)
Path("templates").mkdir(exist_ok=True)

REQUEST_TIMEOUT     = 180
MAX_FILES_PER_PROJ  = 8
MAX_LINES_PER_FILE  = 250

PORT_START          = 9001
PORT_END            = 9100

app = FastAPI(title="AI Project Maker — Claude Code")

projects: dict = {}
STATE_FILE = Path("projects_state.json")

running_processes: dict = {}
_proc_lock = threading.Lock()

# ================================================
# STATE
# ================================================
def load_state():
    global projects
    if STATE_FILE.exists():
        try:
            projects = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            print(f"[STATE] Loaded {len(projects)} project.")
        except Exception as e:
            print(f"[STATE] Gagal load: {e}")
            projects = {}

def save_state():
    try:
        STATE_FILE.write_text(
            json.dumps(projects, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"[STATE] Gagal simpan: {e}")

load_state()

# ================================================
# MULTI KEY MANAGER
# ================================================
class MultiKeyManager:
    def __init__(self, keys: list):
        self.keys         = keys
        self.current_idx  = 0
        self.error_counts = [0] * len(keys)
        self._lock        = threading.Lock()

    def get_info(self) -> dict:
        with self._lock:
            return self.keys[self.current_idx]

    def next_key(self):
        with self._lock:
            self.current_idx = (self.current_idx + 1) % len(self.keys)

    def mark_error(self):
        with self._lock:
            self.error_counts[self.current_idx] += 1
        self.next_key()

    def reset_errors(self):
        with self._lock:
            self.error_counts = [0] * len(self.keys)

    def status(self) -> list:
        with self._lock:
            result = []
            for i, key_info in enumerate(self.keys):
                api_key = key_info["api_key"]
                result.append({
                    "index"       : i,
                    "base_url"    : key_info["base_url"],
                    "model"       : key_info["model"],
                    "api_key_hint": api_key[:8] + "..." if len(api_key) > 8 else api_key,
                    "error_count" : self.error_counts[i],
                    "active"      : i == self.current_idx,
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
            timeout = httpx.Timeout(
                connect = 15.0,
                read    = float(REQUEST_TIMEOUT),
                write   = 15.0,
                pool    = 15.0,
            ),
            verify = False,
        )
    )

def ambil_text(resp) -> str:
    hasil = ""
    for block in resp.content:
        block_type = getattr(block, "type", "")
        if block_type == "text":
            hasil += getattr(block, "text", "")
        elif block_type == "thinking":
            thinking_len = len(getattr(block, "thinking", ""))
            print(f"[AI] 💭 ThinkingBlock ({thinking_len} chars), diskip")
    return hasil.strip()

def tanya_ai(system_prompt: str, user_prompt: str, max_tokens: int = 4096) -> str:
    last_error  = "tidak ada error"
    max_retries = len(API_KEYS) * 2

    for attempt in range(max_retries):
        key_info = key_manager.get_info()
        model    = key_info["model"]

        print(f"[AI] Attempt {attempt+1}/{max_retries} | model={model}")
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

            print(f"[AI] ✅ Sukses {elapsed:.1f}s | {len(hasil)} chars")
            return hasil

        except anthropic.AuthenticationError as e:
            last_error = f"401: {str(e)[:200]}"
            key_manager.mark_error()
            time.sleep(1)
        except anthropic.RateLimitError as e:
            last_error = f"429: {str(e)[:200]}"
            key_manager.mark_error()
            time.sleep(5)
        except anthropic.APIStatusError as e:
            last_error = f"Status {e.status_code}: {str(e.message)[:200]}"
            key_manager.mark_error()
            time.sleep(2)
        except anthropic.APIConnectionError as e:
            last_error = f"Connection: {str(e)[:200]}"
            key_manager.mark_error()
            time.sleep(3)
        except Exception as e:
            last_error = f"{str(e)[:200]}"
            key_manager.mark_error()
            time.sleep(2)

    raise Exception(f"Semua key gagal. Error terakhir: {last_error}")

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
# ✅ PARSE JSON TOLERAN — handle invalid escape sequences
# ================================================
def parse_json_toleran(text: str) -> dict:
    """
    Parse JSON yang mungkin berisi backslash aneh dari code Python.
    4 strategi bertingkat.
    """
    text = bersihkan_json(text)

    # ── Strategi 1: Parse langsung ──
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 1 failed: {e}")

    # ── Strategi 2: Fix invalid escape sequences ──
    # JSON hanya boleh: \" \\ \/ \b \f \n \r \t \uXXXX
    try:
        fixed = re.sub(r'\\(?![\\"/bfnrtu])', r'\\\\', text)
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 2 failed: {e}")

    # ── Strategi 3: Fix + hapus control characters ──
    try:
        fixed = re.sub(r'\\(?![\\"/bfnrtu])', r'\\\\', text)
        fixed = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', fixed)
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        print(f"[JSON] Attempt 3 failed: {e}")

    # ── Strategi 4: Ekstrak field manual pakai regex ──
    print("[JSON] Fallback: ekstrak field manual pakai regex")
    result = {
        "analysis"      : "",
        "new_files"     : {},
        "modified_files": {},
        "run_cmd"       : "",
        "notes"         : "",
        "description"   : "",
        "tech_stack"    : "",
        "files"         : [],
        "install_cmd"   : "",
        "test_cmd"      : "",
        "project_type"  : "script",
        "fixed_files"   : {},
    }

    # Ekstrak field string sederhana
    for field in ["analysis", "run_cmd", "notes", "description",
                  "tech_stack", "install_cmd", "test_cmd", "project_type"]:
        m = re.search(rf'"{field}"\s*:\s*"([^"]*(?:\\.[^"]*)*)"', text)
        if m:
            result[field] = m.group(1)

    # Ekstrak array files
    m = re.search(r'"files"\s*:\s*\[(.*?)\]', text, re.DOTALL)
    if m:
        files_str = m.group(1)
        result["files"] = re.findall(r'"([^"]+)"', files_str)

    # Ekstrak isi file dari new_files/modified_files/fixed_files
    for match in re.finditer(
        r'"([a-zA-Z0-9_/\-\.]+\.(?:py|js|ts|html|css|json|txt|md|yaml|yml|toml))"\s*:\s*"((?:[^"\\]|\\.)*)"',
        text
    ):
        fname, fcontent = match.group(1), match.group(2)
        # Unescape manually
        fcontent = fcontent.replace('\\n', '\n').replace('\\t', '\t')
        fcontent = fcontent.replace('\\"', '"').replace('\\\\', '\\')
        result["modified_files"][fname] = fcontent

    if (not result["modified_files"] and not result["new_files"]
        and not result["fixed_files"] and not result["files"]):
        raise Exception("Tidak bisa parse JSON sama sekali")

    return result

# ================================================
# LOG
# ================================================
_log_lock = threading.Lock()

def log(project_id: str, pesan: str, level: str = "info"):
    emoji_map = {
        "info"   : "ℹ️", "success": "✅", "error": "❌",
        "warning": "⚠️", "loading": "⏳", "fix"  : "🔧",
        "file"   : "📄", "folder" : "📁", "run"  : "🧪",
        "done"   : "🎉", "lanjut" : "🔄",
    }
    icon  = emoji_map.get(level, "•")
    entry = f"{icon} {pesan}"
    with _log_lock:
        if project_id in projects:
            projects[project_id]["logs"].append(entry)
            save_state()
    print(f"[{project_id}] {entry}")

# ================================================
# JALANKAN COMMAND
# ================================================
def jalankan_cmd(cmd: str, cwd: str, timeout: int = 60) -> dict:
    try:
        hasil = subprocess.run(
            cmd, cwd=cwd, shell=True,
            capture_output=True, text=True, timeout=timeout,
        )
        return {
            "sukses": hasil.returncode == 0,
            "output": hasil.stdout,
            "error" : hasil.stderr,
        }
    except subprocess.TimeoutExpired:
        return {"sukses": False, "output": "", "error": f"TIMEOUT: '{cmd}'"}
    except Exception as e:
        return {"sukses": False, "output": "", "error": str(e)}

# ================================================
# BACA FILE
# ================================================
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
            rel = str(fp.relative_to(project_dir))
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
            rel = str(fp.relative_to(project_dir))
            semua[rel] = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            pass
    return semua

# ================================================
# GENERATE SATU FILE
# ================================================
def generate_satu_file(nama_project: str, deskripsi: str,
                        filename: str, daftar_file: list) -> str:

    if filename.endswith("__init__.py"):
        return '"""Package init."""\n'

    desc_short = deskripsi[:200]
    files_hint = ", ".join(daftar_file[:6])

    ANTI_MARKDOWN = (
        "\n\nPENTING: Balas HANYA isi file mentah TANPA ```python, TANPA ```, "
        "TANPA penjelasan. Langsung code saja dari baris pertama."
    )

    PORT_HINT = (
        "\n\nCATATAN: Jika project pakai FastAPI/Flask/HTTP server, "
        "WAJIB baca port dari env variable PORT (default 8000). Contoh: "
        "port = int(os.environ.get('PORT', 8000)). "
        "Bind ke host 0.0.0.0."
    )

    if filename == "requirements.txt":
        system = "Programmer Python. Tulis requirements.txt saja."
        user   = (
            f"Project: {nama_project} - {desc_short}\n"
            "Tulis requirements.txt (satu library per baris, no komentar, no versi):"
            + ANTI_MARKDOWN
        )
        raw = tanya_ai(system, user, max_tokens=400)

    elif filename == "README.md":
        system = "Tulis README.md singkat dan padat."
        user   = (
            f"Project: {nama_project}\nDeskripsi: {desc_short}\n"
            "Tulis README.md: judul, deskripsi 2 kalimat, install, cara pakai. RINGKAS."
            "\n\nPENTING: Langsung markdown, JANGAN dibungkus ```markdown."
        )
        raw = tanya_ai(system, user, max_tokens=1200)

    elif "test" in filename.lower():
        system = "Programmer Python. Tulis unit test pytest SEDERHANA."
        user   = (
            f"Project: {nama_project}\nDeskripsi: {desc_short}\n\n"
            f"Tulis {filename} berisi 2-3 unit test SEDERHANA. Gunakan mock."
            + ANTI_MARKDOWN
        )
        raw = tanya_ai(system, user, max_tokens=1500)

    elif filename in ["config.py", "database.py"]:
        system = "Programmer Python. Tulis file konfigurasi RINGKAS."
        user   = (
            f"Project: {nama_project}\nDeskripsi: {desc_short}\n"
            f"Tulis {filename} RINGKAS (max 60 baris)."
            + ANTI_MARKDOWN
        )
        raw = tanya_ai(system, user, max_tokens=1200)

    elif filename in ["models.py", "schemas.py", "crud.py"]:
        system = "Programmer Python expert FastAPI + SQLAlchemy."
        user   = (
            f"Project: {nama_project}\nDeskripsi: {desc_short}\n"
            f"File lain: {files_hint}\n\n"
            f"Tulis {filename} LENGKAP tapi RINGKAS (max {MAX_LINES_PER_FILE} baris)."
            + ANTI_MARKDOWN
        )
        raw = tanya_ai(system, user, max_tokens=2500)

    else:
        system = "Programmer Python expert. Tulis code RINGKAS dan langsung jalan."
        user   = (
            f"Project: {nama_project}\nDeskripsi: {desc_short}\n"
            f"File di project: {files_hint}\n\n"
            f"Tulis {filename} LENGKAP bisa langsung dijalankan. "
            f"MAKSIMAL {MAX_LINES_PER_FILE} baris."
            + PORT_HINT + ANTI_MARKDOWN
        )
        raw = tanya_ai(system, user, max_tokens=3500)

    return bersihkan_code(raw)

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
        "PENTING escape: backslash \\ jadi \\\\, newline jadi \\n, kutip jadi \\\".\n"
        '{"analysis":"penjelasan","fixed_files":{"path/file.py":"code LENGKAP"}}'
    )
    context = {}
    total   = 0
    for k, v in semua_file.items():
        if total > 4000: break
        context[k] = v
        total += len(v)

    user = f"ERROR:\n{error_msg[:600]}\n\nFILE:\n{json.dumps(context, ensure_ascii=False)}"

    try:
        raw  = tanya_ai(system, user, max_tokens=4000)
        # ✅ FIX: pakai parse_json_toleran
        data = parse_json_toleran(raw)
        log(project_id, f"Analisis: {data.get('analysis', '-')}", "fix")
        fixed = data.get("fixed_files", {})
        for filename, new_code in fixed.items():
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
# BUAT PROJECT
# ================================================
def buat_project_background(project_id: str, deskripsi: str, nama: str):
    try:
        log(project_id, "Memulai pembuatan project...", "loading")
        key_info = key_manager.get_info()
        log(project_id, f"Model: {key_info['model']}", "info")

        log(project_id, "Tahap 1: AI merancang struktur...", "loading")

        system_plan = (
            "Kamu arsitek software Python. Rancang struktur project SEDERHANA.\n\n"
            "Balas HANYA JSON valid tanpa markdown:\n"
            "{\n"
            '  "description": "deskripsi singkat",\n'
            '  "tech_stack": "teknologi",\n'
            '  "files": ["main.py","requirements.txt","README.md","tests/test_main.py"],\n'
            '  "install_cmd": "pip install -r requirements.txt",\n'
            '  "run_cmd": "python main.py",\n'
            '  "test_cmd": "python -m pytest tests/ -v",\n'
            '  "project_type": "cli|fastapi|flask|script"\n'
            "}"
        )
        user_plan = (
            f"Project: {nama}\nDeskripsi: {deskripsi[:300]}\n"
            f"MAKSIMAL {MAX_FILES_PER_PROJ} file. project_type harus dipilih: cli, fastapi, flask, atau script."
        )

        raw_plan = tanya_ai(system_plan, user_plan, max_tokens=800)

        # ✅ FIX: pakai parse_json_toleran
        try:
            plan = parse_json_toleran(raw_plan)
        except Exception as e:
            msg = f"Gagal parse planning: {e}\nRaw (500 char): {raw_plan[:500]}"
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        daftar_file = plan.get("files", ["main.py","requirements.txt","README.md","tests/test_main.py"])
        if not daftar_file:
            daftar_file = ["main.py","requirements.txt","README.md","tests/test_main.py"]
        if len(daftar_file) > MAX_FILES_PER_PROJ:
            daftar_file = daftar_file[:MAX_FILES_PER_PROJ]

        install_cmd  = plan.get("install_cmd", "pip install -r requirements.txt")
        run_cmd      = plan.get("run_cmd",     "python main.py")
        test_cmd     = plan.get("test_cmd",    "python -m pytest tests/ -v")
        tech_stack   = plan.get("tech_stack",  "Python")
        description  = plan.get("description", deskripsi)
        project_type = (plan.get("project_type") or "script").lower()

        log(project_id, f"Struktur: {len(daftar_file)} file akan dibuat", "info")
        log(project_id, f"Tipe project: {project_type}", "info")

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
        log(project_id, "Tahap 2: Generate file satu per satu...", "loading")

        file_berhasil = []
        file_gagal    = []

        for idx, filename in enumerate(daftar_file, 1):
            filename = str(filename).strip().lstrip("/")
            if not filename: continue

            log(project_id, f"[{idx}/{len(daftar_file)}] Generating: {filename}...", "loading")
            t_start = time.time()

            try:
                isi = generate_satu_file(nama, deskripsi, filename, daftar_file)
                elapsed = time.time() - t_start
                log(project_id, f"  ✓ Selesai ({len(isi)} chars, {elapsed:.1f}s)", "success")
            except Exception as e:
                err_msg = str(e)[:200]
                log(project_id, f"  ✗ Gagal: {err_msg}", "warning")
                file_gagal.append(filename)
                isi = f'# File {filename} - GAGAL DIGENERATE\n# Error: {err_msg}\n'

            target = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(isi, encoding="utf-8")
            file_berhasil.append(filename)
            log(project_id, f"  💾 Disimpan: {filename}", "file")

            with _log_lock:
                projects[project_id]["files"] = file_berhasil.copy()
                save_state()

        # Auto-add pytest & framework
        req_file = project_dir / "requirements.txt"
        if req_file.exists():
            current_req = req_file.read_text(encoding="utf-8")
            libs_add = []
            if any("test" in f.lower() for f in file_berhasil) and "pytest" not in current_req.lower():
                libs_add.append("pytest")
            if project_type == "fastapi":
                if "fastapi" not in current_req.lower(): libs_add.append("fastapi")
                if "uvicorn" not in current_req.lower(): libs_add.append("uvicorn")
            if project_type == "flask" and "flask" not in current_req.lower():
                libs_add.append("flask")

            if libs_add:
                current_req = current_req.rstrip() + "\n" + "\n".join(libs_add) + "\n"
                req_file.write_text(current_req, encoding="utf-8")
                log(project_id, f"Ditambahkan: {', '.join(libs_add)}", "info")

        with _log_lock:
            projects[project_id]["files"]   = file_berhasil
            projects[project_id]["run_cmd"] = run_cmd
            save_state()

        meta = {
            "nama"        : nama,
            "deskripsi"   : deskripsi,
            "tech_stack"  : tech_stack,
            "run_cmd"     : run_cmd,
            "install_cmd" : install_cmd,
            "test_cmd"    : test_cmd,
            "files"       : file_berhasil,
            "project_type": project_type,
        }
        (project_dir / ".ai_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        if len(file_gagal) < len(daftar_file) / 2:
            if req_file.exists():
                log(project_id, "Menginstall dependencies...", "loading")
                hasil_install = jalankan_cmd(install_cmd, str(project_dir), timeout=180)
                if hasil_install["sukses"]:
                    log(project_id, "Dependencies berhasil diinstall!", "success")
                else:
                    log(project_id, f"Warning install: {hasil_install['error'][:200]}", "warning")

        log(project_id, "=" * 40, "info")
        log(project_id, f"Project '{nama}' selesai!", "done")
        log(project_id, f"Tipe: {project_type} | Files: {len(file_berhasil)}", "info")
        log(project_id, f"Klik tombol ▶️ RUN untuk menjalankan!", "info")

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
# LANJUT PROJECT
# ================================================
def lanjut_project_background(project_id: str, nama: str, permintaan: str):
    try:
        project_dir = BASE_DIR / nama
        if not project_dir.exists():
            log(project_id, f"Folder '{nama}' tidak ditemukan.", "error")
            projects[project_id]["status"] = "error"
            save_state()
            return

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

        # ✅ Prompt dengan reminder escape yang benar
        system_lanjut = (
            "Programmer Python expert. Kembangkan project.\n\n"
            "Balas HANYA JSON valid tanpa markdown.\n"
            "PENTING escape untuk isi code di dalam JSON:\n"
            "- Backslash \\ HARUS jadi \\\\\n"
            "- Newline HARUS jadi \\n\n"
            "- Double quote \" HARUS jadi \\\"\n"
            "- Tab HARUS jadi \\t\n"
            "- JANGAN pakai raw string atau karakter aneh\n\n"
            "Format:\n"
            "{\n"
            '  "analysis": "singkat",\n'
            '  "new_files": {"path/file.py": "code dengan escape benar"},\n'
            '  "modified_files": {"path/file.py": "code dengan escape benar"},\n'
            '  "run_cmd": "perintah",\n'
            '  "notes": "ringkasan"\n'
            "}"
        )

        context  = {}
        total_ch = 0
        for fname, fcontent in semua_file.items():
            if total_ch > 3000: break
            context[fname] = fcontent
            total_ch += len(fcontent)

        user_lanjut = (
            f"Project: {nama} | Tech: {meta.get('tech_stack', 'Python')}\n"
            f"Permintaan: {permintaan}\n\n"
            f"File ada:\n{json.dumps(context, ensure_ascii=False)}"
        )

        log(project_id, "AI mengembangkan...", "loading")
        raw_lanjut = tanya_ai(system_lanjut, user_lanjut, max_tokens=5000)

        # ✅ FIX: pakai parse_json_toleran
        try:
            hasil = parse_json_toleran(raw_lanjut)
        except Exception as e:
            msg = f"JSON invalid setelah 4x retry: {e}\nRaw (500 char): {raw_lanjut[:500]}"
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        log(project_id, f"Analisis: {hasil.get('analysis', '-')}", "info")

        new_files = hasil.get("new_files", {}) or {}
        mod_files = hasil.get("modified_files", {}) or {}

        if not new_files and not mod_files:
            log(project_id, "AI tidak mengusulkan perubahan file.", "warning")

        for filename, content in {**new_files, **mod_files}.items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(bersihkan_code(str(content)), encoding="utf-8")
            label = "File baru" if filename in new_files else "Diupdate"
            log(project_id, f"{label}: {filename}", "file")

        all_files = list(semua_file.keys())
        for f in list(new_files.keys()) + list(mod_files.keys()):
            if f not in all_files:
                all_files.append(f)

        run_cmd = hasil.get("run_cmd") or meta.get("run_cmd", "")
        meta["files"]   = all_files
        meta["run_cmd"] = run_cmd
        meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        with _log_lock:
            projects[project_id]["files"]   = all_files
            projects[project_id]["run_cmd"] = run_cmd
            projects[project_id]["tech"]    = meta.get("tech_stack", "")
            projects[project_id]["nama"]    = nama
            projects[project_id]["project_type"] = meta.get("project_type", "script")
            save_state()

        if "requirements.txt" in mod_files or "requirements.txt" in new_files:
            jalankan_cmd(
                meta.get("install_cmd", "pip install -r requirements.txt"),
                str(project_dir), timeout=180,
            )

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
            return port
    return PORT_START

def stop_project(project_id: str) -> bool:
    with _proc_lock:
        info = running_processes.get(project_id)
        if not info:
            return False

        proc = info.get("process")
        if proc and proc.poll() is None:
            try:
                if sys.platform == "win32":
                    proc.terminate()
                else:
                    proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

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
    except Exception as e:
        with _proc_lock:
            if project_id in running_processes:
                running_processes[project_id]["logs"].append(f"[STREAM ERROR] {e}")

def run_project(project_id: str, project_dir: Path, run_cmd: str, project_type: str) -> dict:
    stop_project(project_id)
    port = cari_port_kosong()

    env = os.environ.copy()
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
            run_cmd = re.sub(r"--port\s+\d+", f"--port {port}", run_cmd)
            run_cmd = re.sub(r"--host\s+\S+", "--host 127.0.0.1", run_cmd)
            if "--port" not in run_cmd:
                run_cmd = run_cmd + f" --port {port}"
            if "--host" not in run_cmd:
                run_cmd = run_cmd + " --host 127.0.0.1"

    elif project_type == "flask":
        env["FLASK_RUN_PORT"] = str(port)
        env["FLASK_RUN_HOST"] = "127.0.0.1"

    print(f"[RUN] Project {project_id} → {run_cmd} @ port {port}")

    try:
        proc = subprocess.Popen(
            run_cmd,
            cwd    = str(project_dir),
            shell  = True,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT,
            text   = True,
            bufsize= 1,
            env    = env,
        )

        with _proc_lock:
            running_processes[project_id] = {
                "process"   : proc,
                "port"      : port,
                "logs"      : [f"[SYSTEM] Menjalankan: {run_cmd}", f"[SYSTEM] Port: {port}"],
                "started_at": time.time(),
                "run_cmd"   : run_cmd,
                "type"      : project_type,
            }

        threading.Thread(
            target=stream_output,
            args=(project_id, proc),
            daemon=True,
        ).start()

        time.sleep(2)
        if proc.poll() is not None and proc.poll() != 0:
            with _proc_lock:
                logs = running_processes.get(project_id, {}).get("logs", [])
            return {
                "sukses": False,
                "error" : f"Process langsung berhenti (exit code: {proc.poll()})",
                "logs"  : logs[-20:],
            }

        is_web = project_type in ["fastapi", "flask"]
        url = f"/proxy/{project_id}/" if is_web else None

        return {
            "sukses" : True,
            "port"   : port,
            "run_cmd": run_cmd,
            "type"   : project_type,
            "url"    : url,
        }

    except Exception as e:
        return {"sukses": False, "error": str(e)}

# ================================================
# ROUTES
# ================================================
@app.get("/", response_class=HTMLResponse)
async def halaman_utama():
    html_file = Path("templates/index.html")
    if not html_file.exists():
        return HTMLResponse(content="<h1>templates/index.html tidak ada</h1>", status_code=404)
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

@app.get("/test-ai")
async def test_ai():
    hasil_cek = []
    for i, key_info in enumerate(API_KEYS):
        try:
            client = buat_client(key_info["base_url"], key_info["api_key"])
            resp   = client.messages.create(
                model      = key_info["model"],
                max_tokens = 64,
                messages   = [{"role": "user", "content": "Balas: OK"}]
            )
            reply = ambil_text(resp) or "(kosong)"
            hasil_cek.append({"index": i, "status": "✅ sukses",
                              "model": key_info["model"], "reply": reply})
        except Exception as e:
            hasil_cek.append({"index": i, "status": "❌ gagal",
                              "model": key_info["model"], "error": str(e)})
    return JSONResponse({"keys": hasil_cek})

@app.post("/buat-project")
async def buat_project_route(deskripsi: str = Form(...), nama: str = Form(...)):
    nama       = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project_kuliah"
    project_id = str(uuid.uuid4())[:8]

    with _log_lock:
        projects[project_id] = {
            "status": "loading", "logs": [], "files": [],
            "folder": "", "error": "", "run_cmd": "",
            "tech": "", "desc": "", "nama": nama,
            "project_type": "script",
        }
        save_state()

    threading.Thread(
        target=buat_project_background,
        args=(project_id, deskripsi, nama),
        daemon=True,
    ).start()

    return JSONResponse({"project_id": project_id, "nama": nama})

@app.post("/lanjut-project")
async def lanjut_project_route(nama: str = Form(...), permintaan: str = Form(...)):
    nama       = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project"
    project_id = str(uuid.uuid4())[:8]

    with _log_lock:
        projects[project_id] = {
            "status": "loading", "logs": [], "files": [],
            "folder": "", "error": "", "run_cmd": "",
            "tech": "", "desc": "", "nama": nama,
            "project_type": "script",
        }
        save_state()

    threading.Thread(
        target=lanjut_project_background,
        args=(project_id, nama, permintaan),
        daemon=True,
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
            proc     = info["process"]
            is_alive = proc.poll() is None
            is_web   = info.get("type") in ["fastapi", "flask"]
            data["running"] = {
                "alive"     : is_alive,
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
        return JSONResponse({"error": "Tidak ditemukan"}, status_code=404)

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
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        file_count = sum(
            1 for f in folder.rglob("*")
            if f.is_file() and f.name != ".ai_meta.json"
            and not any(s in f.parts for s in SKIP_DIRS)
        )
        hasil.append({
            "nama"        : folder.name,
            "tech_stack"  : meta.get("tech_stack", "-"),
            "deskripsi"   : meta.get("deskripsi",  "-"),
            "run_cmd"     : meta.get("run_cmd",    "-"),
            "project_type": meta.get("project_type", "script"),
            "file_count"  : file_count,
        })

    return JSONResponse({"projects": hasil})

@app.get("/key-status")
async def key_status_route():
    return JSONResponse({
        "current_index": key_manager.current_idx,
        "keys"         : key_manager.status(),
    })

@app.post("/key-reset")
async def key_reset_route():
    key_manager.reset_errors()
    return JSONResponse({"message": "Reset OK"})

# ================================================
# RUN / STOP / LOGS API
# ================================================
@app.post("/run/{project_id}")
async def run_project_route(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Project tidak ditemukan"}, status_code=404)

    project_data = projects[project_id]
    folder = project_data.get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"error": "Folder project tidak ada"}, status_code=404)

    run_cmd = project_data.get("run_cmd", "")
    if not run_cmd:
        meta_file = Path(folder) / ".ai_meta.json"
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                run_cmd = meta.get("run_cmd", "python main.py")
            except Exception:
                run_cmd = "python main.py"

    project_type = project_data.get("project_type", "script")
    hasil = run_project(project_id, Path(folder), run_cmd, project_type)
    return JSONResponse(hasil)

@app.post("/stop/{project_id}")
async def stop_project_route(project_id: str):
    stopped = stop_project(project_id)
    return JSONResponse({
        "success": stopped,
        "message": "Project dihentikan" if stopped else "Tidak ada process running"
    })

@app.get("/run-logs/{project_id}")
async def run_logs_route(project_id: str):
    with _proc_lock:
        info = running_processes.get(project_id)
        if not info:
            return JSONResponse({"logs": [], "alive": False})

        proc      = info["process"]
        is_alive  = proc.poll() is None
        exit_code = proc.poll()
        is_web    = info.get("type") in ["fastapi", "flask"]

        return JSONResponse({
            "logs"     : info["logs"][-200:],
            "alive"    : is_alive,
            "port"     : info["port"],
            "url"      : f"/proxy/{project_id}/" if is_web else None,
            "exit_code": exit_code,
            "run_cmd"  : info.get("run_cmd", ""),
            "uptime"   : int(time.time() - info["started_at"]),
            "type"     : info.get("type"),
        })

@app.get("/download/{project_id}")
async def download_project(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Tidak ditemukan"}, status_code=404)

    folder = projects[project_id].get("folder", "")
    if not folder or not Path(folder).exists():
        return JSONResponse({"error": "Folder tidak ada"}, status_code=404)

    project_dir = Path(folder)
    nama = projects[project_id].get("nama", "project")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for fp in project_dir.rglob("*"):
            if not fp.is_file(): continue
            if any(s in fp.parts for s in SKIP_DIRS): continue
            arcname = str(fp.relative_to(project_dir))
            zf.write(fp, arcname)

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{nama}.zip"'}
    )

@app.get("/running-list")
async def running_list_route():
    hasil = []
    with _proc_lock:
        for pid, info in running_processes.items():
            proc     = info["process"]
            is_alive = proc.poll() is None
            is_web   = info.get("type") in ["fastapi", "flask"]
            hasil.append({
                "project_id": pid,
                "port"      : info["port"],
                "url"       : f"/proxy/{pid}/" if is_web else None,
                "alive"     : is_alive,
                "uptime"    : int(time.time() - info["started_at"]),
                "type"      : info.get("type"),
                "run_cmd"   : info.get("run_cmd", ""),
            })
    return JSONResponse({"running": hasil})

# ================================================
# PROXY UNTUK IFRAME PREVIEW
# ================================================
@app.api_route(
    "/proxy/{project_id}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]
)
@app.api_route(
    "/proxy/{project_id}/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]
)
async def proxy_to_project(project_id: str, request: Request, path: str = ""):
    with _proc_lock:
        info = running_processes.get(project_id)

    if not info:
        return HTMLResponse(
            content=f"""
            <html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
              <h2>⚠️ Project belum di-RUN</h2>
              <p>Klik tombol ▶️ RUN di tab Info untuk menjalankan project ini.</p>
              <p style="color:#64748b;font-size:.9rem">Project ID: {project_id}</p>
            </body></html>
            """,
            status_code=503
        )

    port = info.get("port")
    proc = info.get("process")

    if proc and proc.poll() is not None:
        return HTMLResponse(
            content=f"""
            <html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
              <h2>❌ Project berhenti</h2>
              <p>Exit code: {proc.poll()}</p>
              <p>Cek tab ▶️ Run untuk melihat error log.</p>
            </body></html>
            """,
            status_code=503
        )

    target_url = f"http://127.0.0.1:{port}/{path}"
    if request.url.query:
        target_url += "?" + request.url.query

    body = await request.body()

    headers = dict(request.headers)
    for h in ["host", "content-length", "connection", "accept-encoding"]:
        headers.pop(h, None)

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False, follow_redirects=False) as client:
            resp = await client.request(
                method  = request.method,
                url     = target_url,
                headers = headers,
                content = body,
            )

        resp_headers = {}
        for k, v in resp.headers.items():
            lk = k.lower()
            if lk in ["content-encoding", "transfer-encoding", "connection",
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
            content     = resp.content,
            status_code = resp.status_code,
            headers     = resp_headers,
            media_type  = resp.headers.get("content-type"),
        )

    except httpx.ConnectError:
        return HTMLResponse(
            content=f"""
            <html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#e2e8f0">
              <h2>⏳ Project sedang loading...</h2>
              <p>Server belum siap di port {port}. Tunggu beberapa detik lalu reload.</p>
              <button onclick="location.reload()" style="padding:10px 20px;background:#6366f1;color:#fff;border:none;border-radius:6px;cursor:pointer">🔄 Reload</button>
            </body></html>
            """,
            status_code=503
        )
    except httpx.TimeoutException:
        return HTMLResponse(
            content="<h2>Timeout — project terlalu lambat merespon</h2>",
            status_code=504
        )
    except Exception as e:
        return HTMLResponse(
            content=f"<h2>Proxy error</h2><pre>{str(e)[:500]}</pre>",
            status_code=500
        )

# ================================================
# CLEANUP on shutdown
# ================================================
@app.on_event("shutdown")
def cleanup_on_shutdown():
    print("[SHUTDOWN] Menghentikan semua running process...")
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
