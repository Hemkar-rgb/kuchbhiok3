# bot.py
import os
import re
import asyncio
import logging
import hashlib
import json
import time
from pathlib import Path
from typing import Optional, Tuple
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatAction,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ytbot")

# -------------------------
# Env & config
# -------------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "2000"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "2"))
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing in environment variables")

# -------------------------
# Paths (Render-safe)
# -------------------------
TMP_DIR = Path("/tmp/ytbot")
TMP_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR = TMP_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
CACHE_DIR = TMP_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
PROGRESS_DIR = TMP_DIR / "progress"
PROGRESS_DIR.mkdir(exist_ok=True)

# -------------------------
# Regex for youtube including shorts
# -------------------------
YOUTUBE_URL_RE = re.compile(
    r"(https?://)?(www\.)?(youtube\.com|youtu\.be)/(?:(shorts)/)?\S+", re.IGNORECASE
)

# -------------------------
# Utility helpers
# -------------------------
def short_id_from(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]

def human_size(bytes_num: float) -> str:
    for unit in ("B","KB","MB","GB","TB"):
        if bytes_num < 1024.0 or unit == "TB":
            return f"{bytes_num:.1f} {unit}"
        bytes_num /= 1024.0
    return f"{bytes_num:.1f} B"

def _ffmpeg_ok() -> bool:
    return os.system("ffmpeg -version > /dev/null 2>&1") == 0

# -------------------------
# Concurrency control stored in app.bot_data on startup
# -------------------------
# We'll create a semaphore in main() and store it as app.bot_data["dl_sem"]

# -------------------------
# Download logic (blocking): uses yt_dlp and writes progress updates to a JSON file
# -------------------------
def _download_sync(url: str, mode: str, outdir: str, progress_path: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Downloads with yt_dlp. Communicates progress via a JSON file at progress_path.
    Returns (filepath, title, error_message)
    """
    try:
        import yt_dlp
    except Exception as e:
        return None, None, f"yt-dlp missing: {e}"

    # prepare outtmpl: use safe short title-length
    outtmpl = str(Path(outdir) / "%(title).150s.%(ext)s")

    ydl_opts = {
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [],
    }

    # progress hook writes to the progress_path
    def _hook(d):
        # keep a compact progress dict
        progress = {
            "status": d.get("status"),
            "filename": d.get("filename"),
            "downloaded_bytes": d.get("downloaded_bytes"),
            "total_bytes": d.get("total_bytes") or d.get("total_bytes_estimate"),
            "speed": d.get("speed"),
            "eta": d.get("eta"),
            "elapsed": d.get("elapsed"),
            "reason": d.get("reason"),
        }
        try:
            with open(progress_path, "w") as pf:
                json.dump(progress, pf)
        except Exception:
            pass

    ydl_opts["progress_hooks"].append(_hook)

    if mode == "audio":
        if not _ffmpeg_ok():
            return None, None, "ffmpeg is required for MP3 conversion (not found)."
        ydl_opts.update({
            "format": "bestaudio/best",
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
        })
    else:  # video
        ydl_opts.update({
            "format": (
                "bv*[ext=mp4][height<=720]+ba[ext=m4a]/"
                "b[ext=mp4][height<=720]/best[ext=mp4]/best"
            ),
            "merge_output_format": "mp4",
        })

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get("title") or "video"
            req = info.get("requested_downloads")
            if req:
                filepath = Path(req[0]["filepath"])
            else:
                filepath = Path(ydl.prepare_filename(info))
            if mode == "audio":
                filepath = filepath.with_suffix(".mp3")
            # final progress write
            final = {"status": "finished", "filename": str(filepath), "downloaded_bytes": filepath.stat().st_size}
            try:
                with open(progress_path, "w") as pf:
                    json.dump(final, pf)
            except Exception:
                pass
            return str(filepath), title, None
    except Exception as e:
        # write error to progress for the UI
        try:
            with open(progress_path, "w") as pf:
                json.dump({"status": "error", "reason": str(e)}, pf)
        except Exception:
            pass
        return None, None, f"Error: {e}"

# -------------------------
# Async wrapper around blocking download. We'll call it in a background Task.
# -------------------------
async def _download_background(app, uid: str, url: str, mode: str):
    """
    Acquire semaphore, perform download in thread; on completion clean semaphore.
    Writes progress to a file located at PROGRESS_DIR/uid.json
    Stores result metadata in app.bot_data["dl_results"][uid] when done.
    """
    sem: asyncio.Semaphore = app.bot_data["dl_sem"]
    dl_results = app.bot_data.setdefault("dl_results", {})
    await sem.acquire()
    try:
        progress_path = str(PROGRESS_DIR / f"{uid}.json")
        outdir = str(DOWNLOAD_DIR)
        # check cache
        cache_key = hashlib.sha1((url + "|" + mode).encode("utf-8")).hexdigest()
        cached = next((p for p in CACHE_DIR.iterdir() if p.name.startswith(cache_key)), None)
        if cached and cached.exists():
            # populate results from cache
            dl_results[uid] = {"status": "cached", "filepath": str(cached), "title": cached.name}
            # write progress file as finished
            with open(progress_path, "w") as pf:
                json.dump({"status": "finished", "filename": str(cached), "downloaded_bytes": cached.stat().st_size}, pf)
            return

        # perform blocking download in thread; use asyncio.to_thread
        filepath, title, err = await asyncio.to_thread(_download_sync, url, mode, outdir, progress_path)

        if err:
            dl_results[uid] = {"status": "error", "error": err}
        elif not filepath:
            dl_results[uid] = {"status": "error", "error": "unknown download failure"}
        else:
            # move to cache (prefix with cache_key)
            src = Path(filepath)
            # name cached file: <cachekey>__<originalname>
            cached_name = f"{cache_key}__{src.name}"
            dest = CACHE_DIR / cached_name
            try:
                src.rename(dest)
            except Exception:
                # fallback to copy
                import shutil
                shutil.copy2(src, dest)
                try:
                    src.unlink(missing_ok=True)
                except Exception:
                    pass
            dl_results[uid] = {"status": "finished", "filepath": str(dest), "title": title}
    finally:
        sem.release()

# -------------------------
# UI messages & handlers
# -------------------------
WELCOME = (
    "👋 <b>Welcome</b>!\n\n"
    "Send a YouTube link (regular or Shorts). I will let you pick <b>Video (MP4)</b> or <b>Audio (MP3)</b>, "
    "show live progress, and upload the file if it is under the size limit."
)

HELP = (
    "Usage:\n"
    "/start - welcome\n"
    "/help - this help\n"
    "/stats - bot stats (admin only)\n"
    "/cleancache - remove cached downloads (admin only)\n\n"
    "Notes:\n"
    "- MP3 requires ffmpeg installed (add ffmpeg to apt.txt on Render).\n"
    "- Files are cached to avoid re-downloading the same URL+format.\n"
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(WELCOME)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP)

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ You are not authorized to use this command.")
    app = context.application
    dl_results = app.bot_data.get("dl_results", {})
    cache_items = list(CACHE_DIR.iterdir())
    msg = (
        f"🔧 Bot stats\n\n"
        f"Concurrent limit: {MAX_CONCURRENT}\n"
        f"Active download slots: {MAX_CONCURRENT - app.bot_data['dl_sem']._value}\n"
        f"Total cache files: {len(cache_items)}\n"
        f"Tracked downloads: {len(dl_results)}"
    )
    await update.message.reply_text(msg)

async def cleancache_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ You are not authorized to use this command.")
    removed = 0
    for p in CACHE_DIR.iterdir():
        try:
            p.unlink()
            removed += 1
        except Exception:
            pass
    await update.message.reply_text(f"✅ Cleared cache. Removed {removed} files.")

def _make_choice_kb(uid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Video MP4", callback_data=f"v|{uid}")],
        [InlineKeyboardButton("🎵 Audio MP3", callback_data=f"a|{uid}")],
    ])

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    match = YOUTUBE_URL_RE.search(text)
    if not match:
        return  # ignore non-links

    url = match.group(0)
    # normalize some common short forms (ensure scheme)
    if not url.startswith("http"):
        url = "https://" + url

    # store in user_data using short id
    uid = short_id_from(url + str(time.time()))
    context.user_data[uid] = {"url": url, "created_at": time.time()}

    await update.message.reply_html("Choose format:", reply_markup=_make_choice_kb(uid))

# -------------------------
# Helper to read progress file
# -------------------------
def read_progress(uid: str):
    p = PROGRESS_DIR / f"{uid}.json"
    if not p.exists():
        return None
    try:
        with p.open("r") as pf:
            return json.load(pf)
    except Exception:
        return None

# -------------------------
# Handle the user's choice
# -------------------------
async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|", 1)
    if len(parts) != 2:
        return await query.edit_message_text("Invalid selection.")

    mode_key, uid = parts
    udata = context.user_data.get(uid)
    if not udata:
        return await query.edit_message_text("⚠️ Session expired. Please send the link again.")

    url = udata["url"]
    mode = "audio" if mode_key == "a" else "video"

    # Message to user showing initial status
    status_msg = await query.edit_message_text(f"⏳ Queued for download — preparing...")

    # create bot_data structures
    app = context.application
    app.bot_data.setdefault("dl_results", {})
    # schedule background download task
    bg_task = asyncio.create_task(_download_background(app, uid, url, mode))

    # Poll progress while background task runs
    last_text = None
    try:
        while not bg_task.done():
            prog = read_progress(uid)
            if prog:
                st = prog.get("status")
                if st == "downloading":
                    dl = prog.get("downloaded_bytes") or 0
                    total = prog.get("total_bytes") or 0
                    eta = prog.get("eta")
                    speed = prog.get("speed") or 0
                    perc = (dl / total * 100) if total else 0.0
                    text = (
                        f"⬇️ Downloading: <b>{prog.get('filename') or 'file'}</b>\n"
                        f"{perc:.1f}% — {human_size(dl)} / {human_size(total) if total else 'Unknown'}\n"
                        f"Speed: {human_size(speed)}/s ETA: {eta}s"
                    )
                elif st == "finished":
                    fn = prog.get("filename")
                    sz = prog.get("downloaded_bytes") or 0
                    text = f"✅ Download finished: <b>{Path(fn).name}</b> — {human_size(sz)}\nProcessing..."
                elif st == "error":
                    text = f"❌ Download error: {prog.get('reason')}"
                else:
                    text = "⏳ Preparing..."
            else:
                text = "⏳ Preparing..."
            if text != last_text:
                try:
                    await status_msg.edit_text(text, parse_mode="HTML")
                except Exception:
                    pass
                last_text = text
            await asyncio.sleep(1.0)

        # wait for background to finish and get result
        await bg_task
        dl_results = app.bot_data.get("dl_results", {})
        result = dl_results.get(uid)
        if not result:
            # fallback to reading progress file
            prog = read_progress(uid) or {}
            if prog.get("status") == "error":
                return await status_msg.edit_text(f"❌ {prog.get('reason')}")
            return await status_msg.edit_text("❌ Download failed (unknown).")

        if result["status"] == "error":
            return await status_msg.edit_text(f"❌ {result.get('error')}")
        if result["status"] == "cached":
            # send cached file
            filepath = Path(result["filepath"])
            title = filepath.name
            if filepath.exists():
                size_mb = filepath.stat().st_size / (1024*1024)
                if size_mb > MAX_UPLOAD_MB:
                    return await status_msg.edit_text(f"⚠️ Cached file too large: {size_mb:.1f} MB > {MAX_UPLOAD_MB} MB")
                await status_msg.edit_text(f"📦 Using cached file: {title}")
                await _send_file(context, query.message.chat.id, filepath, mode, title)
                return await status_msg.delete()
            else:
                # fallthrough to error
                return await status_msg.edit_text("❌ Cached file missing. Try again.")
        if result["status"] == "finished":
            filepath = Path(result["filepath"])
            title = result.get("title") or filepath.name
            if not filepath.exists():
                return await status_msg.edit_text("❌ File missing after download.")
            size_mb = filepath.stat().st_size / (1024*1024)
            if size_mb > MAX_UPLOAD_MB:
                return await status_msg.edit_text(f"⚠️ File too large: {size_mb:.1f} MB > {MAX_UPLOAD_MB} MB")
            await status_msg.edit_text(f"📤 Uploading: {title}")
            await _send_file(context, query.message.chat.id, filepath, mode, title)
            # optional: don't delete cache; keep it to reduce re-downloads
            await status_msg.delete()
            return
    except Exception as e:
        log.exception("Error in on_choice loop")
        try:
            await status_msg.edit_text(f"❌ Internal error: {e}")
        except Exception:
            pass

async def _send_file(context: ContextTypes.DEFAULT_TYPE, chat_id: int, filepath: Path, mode: str, title: str):
    # Send typing action for better UX
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
    except Exception:
        pass

    # open file in context manager to ensure closure
    try:
        if mode == "audio":
            with filepath.open("rb") as f:
                await context.bot.send_audio(chat_id=chat_id, audio=f, title=title, caption=f"🎵 {title}")
        else:
            with filepath.open("rb") as f:
                await context.bot.send_document(chat_id=chat_id, document=f, filename=filepath.name, caption=f"🎬 {title}")
    except Exception as e:
        log.exception("Failed to upload file")
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Upload failed: {e}")
        except Exception:
            pass

# -------------------------
# Main & startup
# -------------------------
def main():
    log.info("Starting bot...")
    app = Application.builder().token(BOT_TOKEN).build()

    # store semaphore for concurrency
    app.bot_data["dl_sem"] = asyncio.Semaphore(MAX_CONCURRENT)
    app.bot_data["dl_results"] = {}

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("cleancache", cleancache_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(CallbackQueryHandler(on_choice))

    # start polling
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
