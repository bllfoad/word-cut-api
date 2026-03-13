#!/usr/bin/env python3
"""
Word Cut API — Microservice for zlabz.io
Searches YouTube for a word/phrase, extracts precise clips, streams them individually.

Endpoints:
  POST /api/word-cut/generate → start job, returns job_id
  GET  /api/word-cut/status/<id> → poll job status + clip list
  GET  /api/word-cut/clip/<job_id>/<clip_idx> → download individual clip
  GET  /api/word-cut/video/<id> → download compiled video (optional)
  GET  /health → health check

All endpoints (except /health) require header: X-API-Key: <secret>
"""
import subprocess, json, os, re, tempfile, shutil, glob, uuid, time, threading, base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from flask import Flask, request, jsonify, Response, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=os.environ.get('CORS_ORIGINS', '*').split(','))

OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/tmp/wordcut-output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

API_SECRET = os.environ.get('API_SECRET', 'wc_zlabz_2026_secret')

SKIP_WORDS = ['music video', 'official video', 'official audio',
              'شيلة', 'أغنية', 'كليب', 'lyrics', 'song', 'remix', 'cover']


# ── Auth middleware ──────────────────────────────────────────────────────────

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-Key') or request.args.get('key')
        if key != API_SECRET:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


# ── Cleanup ──────────────────────────────────────────────────────────────────

def cleanup_old():
    try:
        now = time.time()
        for f in os.listdir(OUTPUT_DIR):
            fp = os.path.join(OUTPUT_DIR, f)
            if os.path.isdir(fp) and now - os.path.getmtime(fp) > 3600:
                shutil.rmtree(fp, ignore_errors=True)
            elif os.path.isfile(fp) and now - os.path.getmtime(fp) > 3600:
                os.remove(fp)
    except:
        pass


# ── YouTube search ───────────────────────────────────────────────────────────

def search_youtube(word, max_videos=60):
    ascii_ratio = sum(1 for c in word if ord(c) < 128) / max(len(word), 1)
    if ascii_ratio > 0.7:
        queries = [f'"{word}"', f'"{word}" interview', f'"{word}" speech', f'"{word}" movie scene']
    else:
        queries = [f'"{word}" news', f'"{word}" أخبار', f'"{word}" تقرير', f'"{word}" خطاب']

    video_ids = []
    seen = set()
    for q in queries:
        if len(video_ids) >= max_videos:
            break
        try:
            r = subprocess.run(
                ['yt-dlp', '--flat-playlist', '--print', 'id', '--print', 'title',
                 f'ytsearch25:{q}', '--no-warnings'],
                capture_output=True, text=True, timeout=30
            )
            lines = r.stdout.strip().split('\n')
            for i in range(0, len(lines) - 1, 2):
                vid_id = lines[i].strip()
                title = lines[i + 1].strip() if i + 1 < len(lines) else ''
                if vid_id and vid_id not in seen and len(vid_id) == 11:
                    if any(sw in title.lower() for sw in SKIP_WORDS):
                        continue
                    seen.add(vid_id)
                    video_ids.append({'id': vid_id, 'title': title})
        except:
            pass
    return video_ids


# ── Subtitle word/phrase search ──────────────────────────────────────────────

def get_word_timestamps(word, vid_id, title, workdir):
    vid_dir = os.path.join(workdir, f"v_{vid_id}")
    os.makedirs(vid_dir, exist_ok=True)
    url = f"https://www.youtube.com/watch?v={vid_id}"

    try:
        subprocess.run(
            ['yt-dlp', '--write-auto-sub', '--sub-lang', 'en,ar', '--skip-download',
             '--sub-format', 'json3', '-o', os.path.join(vid_dir, 'subs'),
             '--no-warnings', '--socket-timeout', '10', url],
            capture_output=True, text=True, timeout=20
        )
    except:
        return None

    sub_files = glob.glob(os.path.join(vid_dir, 'subs*.json3'))
    if not sub_files:
        return None

    best_match = None
    word_lower = word.lower().strip()
    word_tokens = word_lower.split()
    is_phrase = len(word_tokens) > 1

    for sf in sub_files:
        try:
            with open(sf) as f:
                data = json.load(f)

            all_segs = []
            for ev in data.get('events', []):
                tStart = ev.get('tStartMs', 0)
                for seg in ev.get('segs', []):
                    text = seg.get('utf8', '').strip()
                    if not text or text == '\n':
                        continue
                    offset = seg.get('tOffsetMs', 0)
                    all_segs.append({'text': text, 'start_ms': tStart + offset})

            if is_phrase:
                for i in range(len(all_segs)):
                    combined = ''
                    for j in range(i, min(i + len(word_tokens) + 5, len(all_segs))):
                        if combined:
                            combined += ' '
                        combined += all_segs[j]['text']
                        if word_lower in combined.lower():
                            phrase_start_ms = all_segs[i]['start_ms']
                            phrase_end_ms = all_segs[j]['start_ms'] + max(200, len(all_segs[j]['text']) * 80)
                            if j + 1 < len(all_segs):
                                phrase_end_ms = all_segs[j + 1]['start_ms']
                            is_exact = combined.lower().strip() == word_lower
                            if best_match is None or (is_exact and not best_match.get('exact')):
                                best_match = {
                                    'word_start': phrase_start_ms / 1000.0,
                                    'word_end': phrase_end_ms / 1000.0,
                                    'word_text': combined.strip(),
                                    'exact': is_exact,
                                    'vid_id': vid_id, 'title': title,
                                    'vid_dir': vid_dir, 'url': url
                                }
                            break
                    if best_match and best_match.get('exact'):
                        break
            else:
                for si, seg in enumerate(all_segs):
                    text = seg['text']
                    if word in text or word_lower in text.lower():
                        word_start_ms = seg['start_ms']
                        word_end_ms = all_segs[si + 1]['start_ms'] if si + 1 < len(all_segs) else word_start_ms + max(200, len(text) * 80)
                        is_exact = text.lower().strip() == word_lower or text.strip() == word
                        if best_match is None or (is_exact and not best_match.get('exact')):
                            best_match = {
                                'word_start': word_start_ms / 1000.0,
                                'word_end': word_end_ms / 1000.0,
                                'word_text': text, 'exact': is_exact,
                                'vid_id': vid_id, 'title': title,
                                'vid_dir': vid_dir, 'url': url
                            }
                            if is_exact:
                                break
                if best_match and best_match.get('exact'):
                    break
        except:
            continue

    return best_match


# ── Download clip ────────────────────────────────────────────────────────────

def download_clip(info):
    window_start = max(0, info['word_start'] - 0.3)
    window_end = info['word_end'] + 0.3
    clip_path = os.path.join(info['vid_dir'], 'clip.mp4')

    try:
        subprocess.run(
            ['yt-dlp', '-f', 'best[height>=720]/best',
             '--download-sections', f'*{window_start}-{window_end}',
             '-o', clip_path, '--no-warnings', '--socket-timeout', '15',
             '--force-keyframes-at-cuts', info['url']],
            capture_output=True, text=True, timeout=60
        )
        if os.path.exists(clip_path) and os.path.getsize(clip_path) > 5000:
            local_start = max(0, info['word_start'] - window_start)
            word_dur = max(info['word_end'] - info['word_start'], 0.15)
            precise_path = os.path.join(info['vid_dir'], 'precise.mp4')
            subprocess.run(
                ['ffmpeg', '-y', '-i', clip_path,
                 '-ss', str(local_start), '-t', str(word_dur),
                 '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=30',
                 '-c:v', 'libx264', '-preset', 'fast', '-crf', '18',
                 '-profile:v', 'high', '-pix_fmt', 'yuv420p',
                 '-c:a', 'aac', '-ar', '44100', '-ac', '2', '-b:a', '192k',
                 '-movflags', '+faststart', precise_path],
                capture_output=True, timeout=15
            )
            if os.path.exists(precise_path) and os.path.getsize(precise_path) > 3000:
                return precise_path
    except:
        pass
    return None


# ── Jobs store ───────────────────────────────────────────────────────────────

jobs = {}  # job_id → {status, progress, clips: [{path, source, duration_ms, word_text}], ...}


def run_pipeline(job_id, word, target_clips, lang):
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    workdir = tempfile.mkdtemp(prefix="wordcut_")

    try:
        jobs[job_id]['status'] = 'searching'
        videos = search_youtube(word, max_videos=target_clips * 3)
        jobs[job_id]['progress'] = {'step': 'search', 'found': len(videos)}

        if len(videos) < 2:
            jobs[job_id] = {'status': 'failed', 'error': 'Not enough videos found'}
            return

        # Get timestamps
        jobs[job_id]['status'] = 'analyzing'
        results = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(get_word_timestamps, word, v['id'], v['title'], workdir): v
                       for v in videos}
            for f in as_completed(futures):
                try:
                    r = f.result()
                    if r:
                        results.append(r)
                        jobs[job_id]['progress'] = {
                            'step': 'analyze', 'found': len(results), 'target': target_clips
                        }
                        if len(results) >= target_clips + 5:
                            break
                except:
                    pass

        if len(results) < 2:
            jobs[job_id] = {'status': 'failed', 'error': 'Word not found in subtitles'}
            return

        results.sort(key=lambda x: (not x['exact'], x['word_start']))
        results = results[:target_clips + 5]

        # Download clips — save individually
        jobs[job_id]['status'] = 'downloading'
        clips_meta = []
        clip_idx = 0

        with ThreadPoolExecutor(max_workers=5) as pool:
            future_map = {pool.submit(download_clip, info): info for info in results}
            for f in as_completed(future_map):
                info = future_map[f]
                try:
                    path = f.result()
                    if path:
                        # Copy to job dir with index name
                        dest = os.path.join(job_dir, f"clip_{clip_idx:03d}.mp4")
                        shutil.copy2(path, dest)

                        # Get duration
                        probe = subprocess.run(
                            ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'json', dest],
                            capture_output=True, text=True
                        )
                        dur = float(json.loads(probe.stdout).get('format', {}).get('duration', 0))
                        size = os.path.getsize(dest)

                        clips_meta.append({
                            'index': clip_idx,
                            'source': info.get('title', '')[:60],
                            'vid_id': info.get('vid_id', ''),
                            'word_text': info.get('word_text', ''),
                            'duration': round(dur, 3),
                            'size': size,
                        })
                        clip_idx += 1

                        jobs[job_id]['progress'] = {
                            'step': 'download', 'done': clip_idx, 'target': target_clips
                        }
                        if clip_idx >= target_clips:
                            break
                except:
                    pass

        if clip_idx < 2:
            jobs[job_id] = {'status': 'failed', 'error': 'Failed to download clips'}
            return

        # Done — clips are stored individually, no compilation needed
        total_dur = sum(c['duration'] for c in clips_meta)
        total_size = sum(c['size'] for c in clips_meta)

        jobs[job_id] = {
            'status': 'done',
            'result': {
                'job_id': job_id,
                'word': word,
                'clip_count': len(clips_meta),
                'total_duration': round(total_dur, 2),
                'total_size_mb': round(total_size / (1024 * 1024), 1),
                'clips': clips_meta,
            }
        }

    except Exception as e:
        jobs[job_id] = {'status': 'failed', 'error': str(e)}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'word-cut-api'})


@app.route('/api/word-cut/generate', methods=['POST'])
@require_api_key
def generate():
    """Start word cut generation. Returns job_id for polling."""
    cleanup_old()
    data = request.json or {}
    word = data.get('word', '').strip()
    if not word:
        return jsonify({'error': 'word is required'}), 400

    target_clips = min(max(int(data.get('count', 20)), 2), 40)
    lang = data.get('lang', 'ar')

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {'status': 'queued', 'progress': {}}

    t = threading.Thread(target=run_pipeline, args=(job_id, word, target_clips, lang), daemon=True)
    t.start()

    return jsonify({'job_id': job_id, 'status': 'queued'})


@app.route('/api/word-cut/status/<job_id>', methods=['GET'])
@require_api_key
def job_status(job_id):
    """Poll job status. When done, includes clips array with metadata."""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(jobs[job_id])


@app.route('/api/word-cut/clip/<job_id>/<int:clip_idx>', methods=['GET'])
@require_api_key
def get_clip(job_id, clip_idx):
    """Download individual clip by index."""
    clip_path = os.path.join(OUTPUT_DIR, job_id, f"clip_{clip_idx:03d}.mp4")
    if not os.path.exists(clip_path):
        return jsonify({'error': 'Clip not found'}), 404

    # Range request support
    range_header = request.headers.get('Range')
    file_size = os.path.getsize(clip_path)

    if range_header:
        match = re.search(r'bytes=(\d+)-(\d*)', range_header)
        if match:
            byte1 = int(match.group(1))
            byte2 = int(match.group(2)) if match.group(2) else file_size - 1
            length = byte2 - byte1 + 1
            with open(clip_path, 'rb') as f:
                f.seek(byte1)
                data = f.read(length)
            resp = Response(data, 206, mimetype='video/mp4')
            resp.headers.add('Content-Range', f'bytes {byte1}-{byte2}/{file_size}')
            resp.headers.add('Accept-Ranges', 'bytes')
            resp.headers.add('Content-Length', str(length))
            return resp

    return send_file(clip_path, mimetype='video/mp4',
                     download_name=f'clip-{job_id}-{clip_idx}.mp4')


@app.route('/api/word-cut/video/<job_id>', methods=['GET'])
@require_api_key
def get_compiled_video(job_id):
    """Compile and download all clips as one video (on-demand)."""
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    if not os.path.isdir(job_dir):
        return jsonify({'error': 'Job not found'}), 404

    compiled = os.path.join(job_dir, 'compiled.mp4')
    if not os.path.exists(compiled):
        clips = sorted(glob.glob(os.path.join(job_dir, 'clip_*.mp4')))
        if len(clips) < 2:
            return jsonify({'error': 'Not enough clips'}), 404

        inputs = []
        filter_parts = []
        for i, p in enumerate(clips):
            inputs.extend(['-i', p])
            filter_parts.append(f'[{i}:v:0][{i}:a:0]')
        filter_str = ''.join(filter_parts) + f'concat=n={len(clips)}:v=1:a=1[outv][outa]'

        subprocess.run(
            ['ffmpeg', '-y'] + inputs + [
                '-filter_complex', filter_str,
                '-map', '[outv]', '-map', '[outa]',
                '-c:v', 'libx264', '-preset', 'medium', '-crf', '18',
                '-profile:v', 'high', '-pix_fmt', 'yuv420p', '-r', '30',
                '-c:a', 'aac', '-b:a', '192k', '-ar', '44100',
                '-movflags', '+faststart', compiled],
            capture_output=True, timeout=120
        )

    if not os.path.exists(compiled):
        return jsonify({'error': 'Compilation failed'}), 500

    return send_file(compiled, mimetype='video/mp4',
                     download_name=f'wordcut-{job_id}.mp4')


# ── Production entry point ───────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
