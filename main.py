import os
import json
import subprocess
import threading
import re
import uuid
import time
from pathlib import Path

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from openai import OpenAI

# ================================================
# SETTING — MULTI API KEY
# ================================================
API_KEYS = [
    (
        "https://freetokenfaucet.com/v1",
        "tf_deb549c018ab46ee9128e3a6d42449f6",
        "gpt-5.6-terra",
    ),
    (
        "https://freetokenfaucet.com/v1",
        "tf_c908113f2e9c45f091efd1f39a803a24",
        "gpt-5.6-terra",
    ),
]

MAX_FIX  = 3
BASE_DIR = Path("workspace")
BASE_DIR.mkdir(exist_ok=True)
Path("templates").mkdir(exist_ok=True)

app = FastAPI(title="AI Project Maker")

projects: dict = {}
STATE_FILE = Path("projects_state.json")

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

    def get_client(self) -> tuple:
        with self._lock:
            base_url, api_key, model = self.keys[self.current_idx]
        client = OpenAI(base_url=base_url, api_key=api_key)
        return client, model

    def next_key(self):
        with self._lock:
            self.current_idx = (self.current_idx + 1) % len(self.keys)
            print(f"[KEY] Pindah ke index: {self.current_idx}")

    def mark_error(self):
        with self._lock:
            self.error_counts[self.current_idx] += 1
            idx   = self.current_idx
            count = self.error_counts[self.current_idx]
        print(f"[KEY] Key index {idx} error ke-{count}")
        self.next_key()

    def reset_errors(self):
        with self._lock:
            self.error_counts = [0] * len(self.keys)

    def status(self) -> list:
        with self._lock:
            result = []
            for i, (base_url, api_key, model) in enumerate(self.keys):
                result.append({
                    "index"       : i,
                    "base_url"    : base_url,
                    "model"       : model,
                    "api_key_hint": api_key[:10] + "..." if len(api_key) > 10 else api_key,
                    "error_count" : self.error_counts[i],
                    "active"      : i == self.current_idx,
                })
        return result

key_manager = MultiKeyManager(API_KEYS)

# ================================================
# TANYA AI
# ================================================
def tanya_ai(system_prompt: str, user_prompt: str) -> str:
    max_retries = len(API_KEYS) * 2
    for attempt in range(max_retries):
        try:
            client, model = key_manager.get_client()
            response = client.chat.completions.create(
                model=model,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ]
            )
            return (response.choices[0].message.content or "").strip()
        except Exception as e:
            err_str = str(e).lower()
            print(f"[AI] Attempt {attempt+1} error: {e}")
            if any(k in err_str for k in [
                "rate_limit","rate limit","quota","exceeded",
                "unauthorized","invalid_api_key","insufficient",
                "429","401","403",
            ]):
                key_manager.mark_error()
                time.sleep(1)
            else:
                time.sleep(2)
    raise Exception("Semua API key gagal. Cek koneksi atau tambah key baru.")

def bersihkan_json(text: str) -> str:
    text = text.strip()
    if "```" in text:
        lines = text.splitlines()
        lines = [l for l in lines if not l.strip().startswith("```")]
        text  = "\n".join(lines).strip()
    return text

# ================================================
# LOG
# ================================================
_log_lock = threading.Lock()

def log(project_id: str, pesan: str, level: str = "info"):
    emoji_map = {
        "info"   : "ℹ️",
        "success": "✅",
        "error"  : "❌",
        "warning": "⚠️",
        "loading": "⏳",
        "fix"    : "🔧",
        "file"   : "📄",
        "folder" : "📁",
        "run"    : "🧪",
        "done"   : "🎉",
        "lanjut" : "🔄",
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
SKIP_DIRS = {"node_modules","__pycache__",".git","venv",".venv",".mypy_cache"}
READ_EXTS  = {".py",".js",".ts",".html",".css",".json",".txt",
              ".md",".env",".yaml",".yml",".toml"}

def baca_semua_file(project_dir: Path, max_chars: int = 500) -> dict:
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
# PERBAIKI ERROR
# ================================================
def perbaiki_error(project_id: str, project_dir: Path, error_msg: str):
    log(project_id, "Menganalisis error...", "fix")
    semua_file = baca_file_full(project_dir)
    if not semua_file:
        log(project_id, "Tidak ada file untuk diperbaiki.", "warning")
        return

    system = """
Kamu adalah expert debugger Python/Web.
Perbaiki error berdasarkan pesan error dan isi file yang ada.

Balas HANYA JSON valid tanpa markdown:
{
  "analysis": "penjelasan singkat apa yang salah",
  "fixed_files": {
    "path/file.py": "isi code LENGKAP yang sudah diperbaiki"
  }
}

ATURAN:
- Tulis isi file LENGKAP, bukan parsial
- Hanya perbaiki file yang memang bermasalah
- Jangan hapus fungsi yang tidak berkaitan dengan error
"""
    context = {}
    total   = 0
    for k, v in semua_file.items():
        if total > 8000: break
        context[k] = v
        total += len(v)

    user = f"""
ERROR MESSAGE:
{error_msg[:2000]}

FILE PROJECT:
{json.dumps(context, ensure_ascii=False, indent=2)}

Perbaiki semua file yang bermasalah.
"""
    try:
        raw  = tanya_ai(system, user)
        data = json.loads(bersihkan_json(raw))
        log(project_id, f"Analisis: {data.get('analysis','-')}", "fix")
        fixed = data.get("fixed_files", {})
        if not fixed:
            log(project_id, "AI tidak mengusulkan perubahan.", "warning")
            return
        for filename, new_code in fixed.items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                log(project_id, f"Path tidak aman: {filename}", "warning")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(new_code, encoding="utf-8")
            log(project_id, f"Diperbaiki: {filename}", "fix")
    except json.JSONDecodeError as e:
        log(project_id, f"Gagal parse JSON perbaikan: {e}", "error")
    except Exception as e:
        log(project_id, f"Error saat perbaiki: {e}", "error")

# ================================================
# TEST OTOMATIS + AUTO FIX
# ================================================
def jalankan_test(project_id: str, project_dir: Path,
                  test_cmd: str, run_cmd: str) -> bool:
    cmds = []
    if test_cmd: cmds.append(("Test", test_cmd))
    if run_cmd and run_cmd != test_cmd: cmds.append(("Run", run_cmd))
    if not cmds:
        log(project_id, "Tidak ada command test/run.", "warning")
        return True

    for label, cmd in cmds:
        log(project_id, f"{label}: $ {cmd}", "run")
        for attempt in range(1, MAX_FIX + 1):
            log(project_id, f"Percobaan ke-{attempt}/{MAX_FIX}...", "run")
            hasil = jalankan_cmd(cmd, str(project_dir), timeout=30)
            if hasil["sukses"]:
                output = (hasil["output"] or "").strip()
                log(project_id, "Berhasil dijalankan!", "success")
                if output:
                    log(project_id, f"Output:\n{output[:600]}", "success")
                return True
            else:
                err = (hasil["error"] or hasil["output"] or "Unknown error")
                log(project_id, f"Gagal:\n{err[:400]}", "error")
                if attempt < MAX_FIX:
                    log(project_id,
                        f"AI mencoba perbaiki... (sisa {MAX_FIX-attempt}x)", "fix")
                    perbaiki_error(project_id, project_dir, err)
                else:
                    log(project_id, "Batas auto-fix tercapai.", "warning")
    return False

# ================================================
# BUAT PROJECT — BACKGROUND
# ================================================
def buat_project_background(project_id: str, deskripsi: str, nama: str):
    try:
        log(project_id, "Memulai pembuatan project baru...", "loading")
        log(project_id, f"API key index: {key_manager.current_idx}", "info")
        log(project_id, "AI sedang merancang struktur project...", "loading")

        system_plan = """
Kamu adalah Senior Software Engineer berpengalaman.
Buat project kuliah tingkat tinggi (S1) yang profesional, lengkap, dan siap dijalankan.

Balas HANYA dengan JSON valid tanpa markdown:
{
  "description": "deskripsi singkat project",
  "tech_stack": "teknologi yang digunakan",
  "folders": ["folder1", "folder2/subfolder"],
  "files": {
    "main.py": "isi code lengkap",
    "models.py": "isi code lengkap",
    "requirements.txt": "daftar library",
    "README.md": "dokumentasi lengkap",
    "tests/test_main.py": "isi code test"
  },
  "install_cmd": "pip install -r requirements.txt",
  "run_cmd": "python main.py",
  "test_cmd": "python -m pytest tests/ -v"
}

ATURAN WAJIB:
- Minimal 5 file yang relevan
- Setiap file harus berisi code LENGKAP dan bisa langsung dijalankan
- Gunakan best practice Python
- Komentar dalam bahasa Indonesia
- Sertakan README.md dengan cara instalasi dan cara pakai
- Sertakan requirements.txt dengan versi library yang stabil
- Sertakan minimal 1 file test dengan pytest
- Jangan gunakan library yang tidak umum atau tidak stabil
"""
        user_plan = f"""
Buatkan project kuliah lengkap berikut:

DESKRIPSI:
{deskripsi}

NAMA PROJECT:
{nama}

SYARAT:
- Layak untuk tugas akhir / skripsi level S1
- Minimal 5 file terstruktur rapi
- Menggunakan Python
- Bisa langsung dijalankan tanpa konfigurasi tambahan
"""
        raw_plan = tanya_ai(system_plan, user_plan)

        try:
            data = json.loads(bersihkan_json(raw_plan))
        except json.JSONDecodeError as e:
            msg = f"AI tidak mengembalikan JSON valid: {e}"
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        if not data.get("files"):
            msg = "AI tidak menghasilkan file apapun."
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        project_dir = BASE_DIR / nama
        project_dir.mkdir(parents=True, exist_ok=True)
        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            save_state()

        log(project_id, f"Folder: workspace/{nama}/", "folder")

        for folder in data.get("folders", []):
            folder = str(folder).strip().lstrip("/")
            if folder:
                target = (project_dir / folder).resolve()
                if str(target).startswith(str(project_dir.resolve())):
                    target.mkdir(parents=True, exist_ok=True)
                    log(project_id, f"Folder: {folder}/", "folder")

        log(project_id, "Menulis semua file...", "loading")
        daftar_file = []

        for filename, content in data.get("files", {}).items():
            filename = str(filename).strip().lstrip("/")
            if not filename: continue
            target = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                log(project_id, f"Path tidak aman: {filename}", "warning")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(str(content), encoding="utf-8")
            daftar_file.append(filename)
            log(project_id, f"File dibuat: {filename}", "file")

        with _log_lock:
            projects[project_id]["files"]   = daftar_file
            projects[project_id]["run_cmd"] = data.get("run_cmd", "")
            projects[project_id]["tech"]    = data.get("tech_stack", "")
            projects[project_id]["desc"]    = data.get("description", "")
            save_state()

        meta = {
            "nama"       : nama,
            "deskripsi"  : deskripsi,
            "tech_stack" : data.get("tech_stack", ""),
            "run_cmd"    : data.get("run_cmd", ""),
            "install_cmd": data.get("install_cmd", ""),
            "test_cmd"   : data.get("test_cmd", ""),
            "files"      : daftar_file,
        }
        (project_dir / ".ai_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        install_cmd = data.get("install_cmd", "").strip()
        req_file    = project_dir / "requirements.txt"
        if install_cmd and req_file.exists():
            log(project_id, "Menginstall dependencies...", "loading")
            hasil_install = jalankan_cmd(install_cmd, str(project_dir), timeout=120)
            if hasil_install["sukses"]:
                log(project_id, "Dependencies berhasil diinstall!", "success")
            else:
                err_i = hasil_install["error"] or hasil_install["output"]
                log(project_id, f"Warning install: {err_i[:300]}", "warning")

        jalankan_test(
            project_id, project_dir,
            data.get("test_cmd", ""),
            data.get("run_cmd", ""),
        )

        log(project_id, "=" * 45, "info")
        log(project_id, f"Project '{nama}' berhasil dibuat!", "done")
        log(project_id, f"Tech Stack : {data.get('tech_stack','-')}", "info")
        log(project_id, f"Total file : {len(daftar_file)} file", "info")
        log(project_id, f"Lokasi     : workspace/{nama}/", "info")
        log(project_id, f"Jalankan   : {data.get('run_cmd','-')}", "info")

        with _log_lock:
            projects[project_id]["status"] = "done"
            save_state()

    except Exception as e:
        import traceback
        tb  = traceback.format_exc()
        msg = f"{str(e)}\n\n{tb[:1000]}"
        log(project_id, f"Error tidak terduga: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()

# ================================================
# LANJUT PROJECT — BACKGROUND
# ================================================
def lanjut_project_background(project_id: str, nama: str, permintaan: str):
    try:
        project_dir = BASE_DIR / nama
        if not project_dir.exists():
            msg = f"Folder '{nama}' tidak ditemukan."
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        log(project_id, f"Membaca project: {nama}", "lanjut")
        log(project_id, f"API key index: {key_manager.current_idx}", "info")

        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            save_state()

        meta      = {}
        meta_file = project_dir / ".ai_meta.json"
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                log(project_id, f"Tech Stack: {meta.get('tech_stack','-')}", "info")
            except Exception:
                pass

        semua_file = baca_semua_file(project_dir, max_chars=600)
        log(project_id, f"Total file: {len(semua_file)}", "info")
        for f in semua_file:
            log(project_id, f"File ada: {f}", "file")

        system_lanjut = """
Kamu adalah Senior Software Engineer yang melanjutkan project yang sudah ada.
Tugasmu: tambahkan atau modifikasi fitur sesuai permintaan,
sambil mempertahankan kode dan struktur yang sudah ada.

Balas HANYA JSON valid tanpa markdown:
{
  "analysis": "analisis singkat project dan apa yang akan ditambahkan",
  "new_files": {
    "path/file_baru.py": "isi code LENGKAP"
  },
  "modified_files": {
    "path/file_lama.py": "isi code LENGKAP yang sudah dimodifikasi"
  },
  "run_cmd": "perintah menjalankan project",
  "notes": "penjelasan singkat apa yang sudah ditambahkan"
}

ATURAN PENTING:
- Jangan hapus fungsi yang sudah ada kecuali diminta
- Tulis isi file LENGKAP, bukan parsial atau snippet
- Pertahankan style dan struktur kode yang ada
- Kalau modifikasi requirements.txt, sertakan SEMUA library (lama + baru)
- Minimal ada 1 perubahan nyata
"""
        context  = {}
        total_ch = 0
        for fname, fcontent in semua_file.items():
            if total_ch > 7000: break
            context[fname] = fcontent
            total_ch += len(fcontent)

        user_lanjut = f"""
PROJECT YANG ADA:
Nama      : {nama}
Tech Stack: {meta.get('tech_stack','Python')}
Deskripsi : {meta.get('deskripsi','-')}

FILE-FILE YANG SUDAH ADA:
{json.dumps(context, ensure_ascii=False, indent=2)}

PERMINTAAN PENGEMBANGAN:
{permintaan}

Kembangkan project sesuai permintaan di atas.
"""
        log(project_id, "AI sedang mengembangkan project...", "loading")
        raw_lanjut = tanya_ai(system_lanjut, user_lanjut)

        try:
            hasil = json.loads(bersihkan_json(raw_lanjut))
        except json.JSONDecodeError as e:
            msg = f"AI tidak mengembalikan JSON valid: {e}"
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        log(project_id, f"Analisis AI: {hasil.get('analysis','-')}", "info")

        new_files = hasil.get("new_files",      {}) or {}
        mod_files = hasil.get("modified_files", {}) or {}

        if not new_files and not mod_files:
            log(project_id, "AI tidak mengusulkan perubahan.", "warning")

        for filename, content in new_files.items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                log(project_id, f"Path tidak aman: {filename}", "warning")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(str(content), encoding="utf-8")
            log(project_id, f"File baru: {filename}", "file")

        for filename, content in mod_files.items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                log(project_id, f"Path tidak aman: {filename}", "warning")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(str(content), encoding="utf-8")
            log(project_id, f"File diupdate: {filename}", "file")

        all_files = list(semua_file.keys())
        for f in list(new_files.keys()) + list(mod_files.keys()):
            if f not in all_files:
                all_files.append(f)

        run_cmd = hasil.get("run_cmd") or meta.get("run_cmd", "")
        meta["files"]   = all_files
        meta["run_cmd"] = run_cmd
        meta_file.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        with _log_lock:
            projects[project_id]["files"]   = all_files
            projects[project_id]["run_cmd"] = run_cmd
            projects[project_id]["tech"]    = meta.get("tech_stack", "")
            projects[project_id]["nama"]    = nama
            save_state()

        req_berubah = (
            "requirements.txt" in mod_files or
            "requirements.txt" in new_files
        )
        if req_berubah:
            log(project_id, "requirements.txt berubah, reinstall...", "loading")
            jalankan_cmd(
                meta.get("install_cmd", "pip install -r requirements.txt"),
                str(project_dir), timeout=120,
            )

        jalankan_test(project_id, project_dir, meta.get("test_cmd",""), run_cmd)

        log(project_id, "=" * 45, "info")
        log(project_id, "Pengembangan project selesai!", "done")
        log(project_id, f"Notes    : {hasil.get('notes','-')}", "info")
        log(project_id, f"File baru: {len(new_files)}", "info")
        log(project_id, f"Diupdate : {len(mod_files)}", "info")
        log(project_id, f"Jalankan : {run_cmd}", "info")

        with _log_lock:
            projects[project_id]["status"] = "done"
            projects[project_id]["desc"]   = hasil.get("notes", "")
            save_state()

    except Exception as e:
        import traceback
        tb  = traceback.format_exc()
        msg = f"{str(e)}\n\n{tb[:1000]}"
        log(project_id, f"Error: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()

# ================================================
# ROUTES
# ================================================

# ✅ FIX UTAMA — tidak pakai Jinja2, baca HTML langsung
@app.get("/", response_class=HTMLResponse)
async def halaman_utama():
    html_file = Path("templates/index.html")
    if not html_file.exists():
        return HTMLResponse(
            content="<h1>Error: templates/index.html tidak ditemukan</h1>",
            status_code=404
        )
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.post("/buat-project")
async def buat_project_route(
    deskripsi: str = Form(...),
    nama: str      = Form(...),
):
    nama       = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project_kuliah"
    project_id = str(uuid.uuid4())[:8]

    with _log_lock:
        projects[project_id] = {
            "status" : "loading",
            "logs"   : [],
            "files"  : [],
            "folder" : "",
            "error"  : "",
            "run_cmd": "",
            "tech"   : "",
            "desc"   : "",
            "nama"   : nama,
        }
        save_state()

    threading.Thread(
        target=buat_project_background,
        args=(project_id, deskripsi, nama),
        daemon=True,
    ).start()

    return JSONResponse({"project_id": project_id, "nama": nama})


@app.post("/lanjut-project")
async def lanjut_project_route(
    nama      : str = Form(...),
    permintaan: str = Form(...),
):
    nama       = re.sub(r"[^a-zA-Z0-9_\-]", "_", nama.strip())[:50] or "project"
    project_id = str(uuid.uuid4())[:8]

    with _log_lock:
        projects[project_id] = {
            "status" : "loading",
            "logs"   : [],
            "files"  : [],
            "folder" : "",
            "error"  : "",
            "run_cmd": "",
            "tech"   : "",
            "desc"   : "",
            "nama"   : nama,
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
        return JSONResponse(
            {"error": f"Project ID '{project_id}' tidak ditemukan."},
            status_code=404
        )
    return JSONResponse(projects[project_id])


@app.get("/files/{project_id}")
async def lihat_files(project_id: str):
    if project_id not in projects:
        return JSONResponse({"error": "Tidak ditemukan."}, status_code=404)

    folder = projects[project_id].get("folder", "")
    if not folder:
        return JSONResponse({"files": []})

    project_dir = Path(folder)
    if not project_dir.exists():
        return JSONResponse({"files": []})

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
        hasil.append({
            "path"   : rel,
            "content": isi,
            "size"   : fp.stat().st_size,
        })

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
            if f.is_file()
            and f.name != ".ai_meta.json"
            and not any(s in f.parts for s in SKIP_DIRS)
        )
        hasil.append({
            "nama"      : folder.name,
            "tech_stack": meta.get("tech_stack", "-"),
            "deskripsi" : meta.get("deskripsi",  "-"),
            "run_cmd"   : meta.get("run_cmd",     "-"),
            "file_count": file_count,
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
    return JSONResponse({"message": "Error counter semua key direset."})
