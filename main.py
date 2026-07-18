import os
import json
import subprocess
import threading
import re
import uuid
import time
import httpx
from pathlib import Path

from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse
from openai import OpenAI

# ================================================
# SETTING — MULTI API KEY
# ================================================
API_KEYS = [
    (
        "https://freetokenfaucet.com/v1",
        "tf_deb549c018ab46ee9128e3a6d42449f6",
        "gpt-5.6-luna",
    ),
    (
        "https://freetokenfaucet.com/v1",
        "tf_c908113f2e9c45f091efd1f39a803a24",
        "gpt-5.6-luna",
    ),
    (
        "https://freetokenfaucet.com/v1",
        "tf_deb549c018ab46ee9128e3a6d42449f6",
        "qwen3.6-flash",
    ),
    (
        "https://freetokenfaucet.com/v1",
        "tf_c908113f2e9c45f091efd1f39a803a24",
        "mimo-v2.5",
    ),
]

MAX_FIX  = 2
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

    def get_info(self) -> tuple:
        with self._lock:
            return self.keys[self.current_idx]

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
# BUAT CLIENT
# ================================================
def buat_client(base_url: str, api_key: str) -> OpenAI:
    return OpenAI(
        base_url=base_url,
        api_key=api_key,
        timeout=90.0,
        http_client=httpx.Client(timeout=90.0, verify=False)
    )

# ================================================
# TANYA AI — PROMPT PENDEK, ROTATE OTOMATIS
# ================================================
def tanya_ai(system_prompt: str, user_prompt: str) -> str:
    last_error  = "tidak ada error"
    max_retries = len(API_KEYS) * 3

    for attempt in range(max_retries):
        base_url, api_key, model = key_manager.get_info()
        print(f"[AI] Attempt {attempt+1}/{max_retries} | model={model}")

        try:
            client   = buat_client(base_url, api_key)
            response = client.chat.completions.create(
                model=model,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ]
            )
            hasil = (response.choices[0].message.content or "").strip()
            print(f"[AI] ✅ Sukses! {len(hasil)} karakter | model={model}")
            return hasil

        except Exception as e:
            last_error = str(e)
            print(f"[AI] ❌ Error: {last_error}")
            key_manager.mark_error()

            err_str = last_error.lower()
            if any(k in err_str for k in ["connection","timeout","ssl","refused"]):
                time.sleep(3)
            else:
                time.sleep(1)

    raise Exception(f"Semua key/model gagal. Error terakhir: {last_error}")

def bersihkan_json(text: str) -> str:
    text = text.strip()
    if "```" in text:
        lines = [l for l in text.splitlines() if not l.strip().startswith("```")]
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
# GENERATE SATU FILE — PROMPT KECIL
# ================================================
def generate_satu_file(nama_project: str, deskripsi: str,
                        filename: str, daftar_file: list) -> str:
    """Generate isi satu file saja — prompt kecil agar tidak timeout"""

    ext = Path(filename).suffix.lower()

    if filename == "requirements.txt":
        system = "Kamu adalah programmer Python. Tulis requirements.txt saja."
        user   = (
            f"Project: {nama_project}\n"
            f"Deskripsi: {deskripsi}\n"
            f"File lain di project: {', '.join(daftar_file)}\n\n"
            "Tulis HANYA isi requirements.txt (satu library per baris, tanpa komentar)."
        )
    elif filename == "README.md":
        system = "Kamu adalah technical writer. Tulis README.md saja."
        user   = (
            f"Project: {nama_project}\n"
            f"Deskripsi: {deskripsi}\n\n"
            "Tulis README.md dengan: deskripsi, cara install, cara pakai, struktur file."
        )
    elif "test" in filename:
        system = "Kamu adalah programmer Python expert. Tulis file unit test saja."
        user   = (
            f"Project: {nama_project}\n"
            f"Deskripsi: {deskripsi}\n\n"
            f"Tulis HANYA isi file {filename} berisi unit test pytest yang relevan. "
            "Jangan import modul yang belum tentu ada, gunakan mock jika perlu."
        )
    else:
        system = "Kamu adalah programmer Python expert. Tulis satu file Python saja."
        user   = (
            f"Project: {nama_project}\n"
            f"Deskripsi: {deskripsi}\n\n"
            f"Tulis HANYA isi file {filename} secara LENGKAP dan bisa langsung dijalankan. "
            "Gunakan best practice Python. Komentar dalam bahasa Indonesia."
        )

    return tanya_ai(system, user)

# ================================================
# PERBAIKI ERROR
# ================================================
def perbaiki_error(project_id: str, project_dir: Path, error_msg: str):
    log(project_id, "Menganalisis error...", "fix")
    semua_file = baca_file_full(project_dir)
    if not semua_file:
        log(project_id, "Tidak ada file untuk diperbaiki.", "warning")
        return

    system = (
        "Kamu adalah debugger Python expert.\n"
        "Perbaiki error. Balas HANYA JSON valid tanpa markdown:\n"
        '{"analysis":"penjelasan","fixed_files":{"path/file.py":"code LENGKAP"}}'
    )

    context = {}
    total   = 0
    for k, v in semua_file.items():
        if total > 4000: break
        context[k] = v
        total += len(v)

    user = (
        f"ERROR:\n{error_msg[:600]}\n\n"
        f"FILE:\n{json.dumps(context, ensure_ascii=False)}"
    )

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
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(new_code, encoding="utf-8")
            log(project_id, f"Diperbaiki: {filename}", "fix")
    except Exception as e:
        log(project_id, f"Error perbaiki: {e}", "error")

# ================================================
# TEST OTOMATIS
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
                    log(project_id, f"Output:\n{output[:400]}", "success")
                return True
            else:
                err = (hasil["error"] or hasil["output"] or "Unknown error")
                log(project_id, f"Gagal:\n{err[:300]}", "error")
                if attempt < MAX_FIX:
                    log(project_id, f"AI mencoba perbaiki...", "fix")
                    perbaiki_error(project_id, project_dir, err)
                else:
                    log(project_id, "Batas auto-fix tercapai.", "warning")
    return False

# ================================================
# BUAT PROJECT — BACKGROUND
# TAHAP 1: Planning (JSON kecil)
# TAHAP 2: Generate tiap file satu per satu
# ================================================
def buat_project_background(project_id: str, deskripsi: str, nama: str):
    try:
        log(project_id, "Memulai pembuatan project...", "loading")
        log(project_id, f"Model: {key_manager.get_info()[2]}", "info")

        # ── TAHAP 1: PLANNING — hanya minta struktur, bukan isi file ──
        log(project_id, "Tahap 1: AI merancang struktur...", "loading")

        system_plan = (
            "Kamu adalah arsitek software Python.\n"
            "Rancang struktur project kuliah S1.\n\n"
            "Balas HANYA JSON valid tanpa markdown:\n"
            "{\n"
            '  "description": "deskripsi singkat",\n'
            '  "tech_stack": "teknologi",\n'
            '  "files": ["main.py","models.py","requirements.txt","README.md","tests/test_main.py"],\n'
            '  "install_cmd": "pip install -r requirements.txt",\n'
            '  "run_cmd": "python main.py",\n'
            '  "test_cmd": "python -m pytest tests/ -v"\n'
            "}"
        )

        user_plan = (
            f"Project: {nama}\n"
            f"Deskripsi: {deskripsi}\n"
            "Tentukan daftar file yang dibutuhkan (minimal 4 file)."
        )

        raw_plan = tanya_ai(system_plan, user_plan)

        try:
            plan = json.loads(bersihkan_json(raw_plan))
        except json.JSONDecodeError as e:
            msg = f"Gagal parse planning: {e}\nRaw: {raw_plan[:400]}"
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        daftar_file  = plan.get("files", [
            "main.py", "requirements.txt", "README.md", "tests/test_main.py"
        ])
        install_cmd  = plan.get("install_cmd", "pip install -r requirements.txt")
        run_cmd      = plan.get("run_cmd", "python main.py")
        test_cmd     = plan.get("test_cmd", "python -m pytest tests/ -v")
        tech_stack   = plan.get("tech_stack", "Python")
        description  = plan.get("description", deskripsi)

        log(project_id, f"Struktur: {len(daftar_file)} file akan dibuat", "info")
        for f in daftar_file:
            log(project_id, f"  → {f}", "info")

        # Buat folder project
        project_dir = BASE_DIR / nama
        project_dir.mkdir(parents=True, exist_ok=True)

        # Buat subfolder
        for f in daftar_file:
            subfolder = (project_dir / f).parent
            subfolder.mkdir(parents=True, exist_ok=True)

        with _log_lock:
            projects[project_id]["folder"] = str(project_dir.resolve())
            projects[project_id]["tech"]   = tech_stack
            projects[project_id]["desc"]   = description
            save_state()

        log(project_id, f"Folder: workspace/{nama}/", "folder")

        # ── TAHAP 2: GENERATE TIAP FILE — satu per satu ──
        log(project_id, "Tahap 2: Generate file satu per satu...", "loading")
        file_berhasil = []

        for filename in daftar_file:
            filename = str(filename).strip().lstrip("/")
            if not filename:
                continue

            log(project_id, f"Generating: {filename}...", "loading")

            try:
                isi = generate_satu_file(nama, deskripsi, filename, daftar_file)
            except Exception as e:
                log(project_id, f"Gagal generate {filename}: {e}", "warning")
                isi = f"# File {filename} - gagal digenerate\n# Error: {e}\n"

            target = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                log(project_id, f"Path tidak aman: {filename}", "warning")
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(isi, encoding="utf-8")
            file_berhasil.append(filename)
            log(project_id, f"File dibuat: {filename}", "file")

        with _log_lock:
            projects[project_id]["files"]   = file_berhasil
            projects[project_id]["run_cmd"] = run_cmd
            save_state()

        # Simpan metadata
        meta = {
            "nama"       : nama,
            "deskripsi"  : deskripsi,
            "tech_stack" : tech_stack,
            "run_cmd"    : run_cmd,
            "install_cmd": install_cmd,
            "test_cmd"   : test_cmd,
            "files"      : file_berhasil,
        }
        (project_dir / ".ai_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # ── TAHAP 3: INSTALL ──
        req_file = project_dir / "requirements.txt"
        if req_file.exists():
            log(project_id, "Menginstall dependencies...", "loading")
            hasil_install = jalankan_cmd(install_cmd, str(project_dir), timeout=120)
            if hasil_install["sukses"]:
                log(project_id, "Dependencies berhasil diinstall!", "success")
            else:
                err_i = hasil_install["error"] or hasil_install["output"]
                log(project_id, f"Warning install: {err_i[:200]}", "warning")

        # ── TAHAP 4: TEST ──
        jalankan_test(project_id, project_dir, test_cmd, run_cmd)

        # ── SELESAI ──
        log(project_id, "=" * 40, "info")
        log(project_id, f"Project '{nama}' berhasil dibuat!", "done")
        log(project_id, f"Tech Stack : {tech_stack}", "info")
        log(project_id, f"Total file : {len(file_berhasil)} file", "info")
        log(project_id, f"Lokasi     : workspace/{nama}/", "info")
        log(project_id, f"Jalankan   : {run_cmd}", "info")

        with _log_lock:
            projects[project_id]["status"] = "done"
            save_state()

    except Exception as e:
        import traceback
        tb  = traceback.format_exc()
        msg = f"{str(e)}\n\n{tb[:800]}"
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
        log(project_id, f"Model: {key_manager.get_info()[2]}", "info")

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

        semua_file = baca_semua_file(project_dir, max_chars=400)
        log(project_id, f"Total file: {len(semua_file)}", "info")
        for f in semua_file:
            log(project_id, f"File ada: {f}", "file")

        system_lanjut = (
            "Kamu adalah programmer Python expert.\n"
            "Kembangkan project sesuai permintaan.\n\n"
            "Balas HANYA JSON valid tanpa markdown:\n"
            "{\n"
            '  "analysis": "apa yang ditambahkan",\n'
            '  "new_files": {"path/file.py": "code LENGKAP"},\n'
            '  "modified_files": {"path/file.py": "code LENGKAP"},\n'
            '  "run_cmd": "perintah jalankan",\n'
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
            f"Project: {nama} | Tech: {meta.get('tech_stack','Python')}\n"
            f"Permintaan: {permintaan}\n\n"
            f"File ada:\n{json.dumps(context, ensure_ascii=False)}"
        )

        log(project_id, "AI sedang mengembangkan...", "loading")
        raw_lanjut = tanya_ai(system_lanjut, user_lanjut)

        try:
            hasil = json.loads(bersihkan_json(raw_lanjut))
        except json.JSONDecodeError as e:
            msg = f"AI tidak mengembalikan JSON valid: {e}\nRaw: {raw_lanjut[:300]}"
            log(project_id, msg, "error")
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()
            return

        log(project_id, f"Analisis: {hasil.get('analysis','-')}", "info")

        new_files = hasil.get("new_files",      {}) or {}
        mod_files = hasil.get("modified_files", {}) or {}

        if not new_files and not mod_files:
            log(project_id, "AI tidak mengusulkan perubahan.", "warning")

        for filename, content in {**new_files, **mod_files}.items():
            filename = str(filename).strip().lstrip("/")
            target   = (project_dir / filename).resolve()
            if not str(target).startswith(str(project_dir.resolve())):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(str(content), encoding="utf-8")
            label = "File baru" if filename in new_files else "Diupdate"
            log(project_id, f"{label}: {filename}", "file")

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

        if "requirements.txt" in mod_files or "requirements.txt" in new_files:
            log(project_id, "Reinstall dependencies...", "loading")
            jalankan_cmd(
                meta.get("install_cmd", "pip install -r requirements.txt"),
                str(project_dir), timeout=120,
            )

        jalankan_test(project_id, project_dir, meta.get("test_cmd",""), run_cmd)

        log(project_id, "=" * 40, "info")
        log(project_id, "Pengembangan selesai!", "done")
        log(project_id, f"Notes: {hasil.get('notes','-')}", "info")
        log(project_id, f"Baru: {len(new_files)} | Update: {len(mod_files)}", "info")

        with _log_lock:
            projects[project_id]["status"] = "done"
            projects[project_id]["desc"]   = hasil.get("notes", "")
            save_state()

    except Exception as e:
        import traceback
        tb  = traceback.format_exc()
        msg = f"{str(e)}\n\n{tb[:800]}"
        log(project_id, f"Error: {str(e)}", "error")
        with _log_lock:
            projects[project_id]["status"] = "error"
            projects[project_id]["error"]  = msg
            save_state()

# ================================================
# ROUTES
# ================================================

@app.get("/", response_class=HTMLResponse)
async def halaman_utama():
    html_file = Path("templates/index.html")
    if not html_file.exists():
        return HTMLResponse(
            content="<h1>Error: templates/index.html tidak ditemukan</h1>",
            status_code=404
        )
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))


@app.get("/test-ai")
async def test_ai():
    hasil_cek = []
    for i, (base_url, api_key, model) in enumerate(API_KEYS):
        try:
            client = buat_client(base_url, api_key)
            resp   = client.chat.completions.create(
                model=model,
                temperature=0.1,
                messages=[{"role": "user", "content": "Balas: OK"}]
            )
            hasil_cek.append({
                "index" : i, "status": "✅ sukses",
                "model" : model, "reply": resp.choices[0].message.content,
            })
        except Exception as e:
            hasil_cek.append({
                "index" : i, "status": "❌ gagal",
                "model" : model, "error": str(e),
            })
    return JSONResponse({"keys": hasil_cek})


@app.get("/list-models")
async def list_models():
    try:
        base_url, api_key, _ = API_KEYS[0]
        client = buat_client(base_url, api_key)
        models = client.models.list()
        return JSONResponse({
            "total" : len(models.data),
            "models": sorted([m.id for m in models.data])
        })
    except Exception as e:
        return JSONResponse({"error": str(e)})


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
