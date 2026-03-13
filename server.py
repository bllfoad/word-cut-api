#!/usr/bin/env python3
"""
Word Cut API — Microservice for zlabz.io
Searches YouTube for a word, extracts precise clips, returns compiled video or NDJSON stream.

Endpoints:
  POST /api/word-cut/search   → search YouTube for word occurrences (returns metadata)
  POST /api/word-cut/generate → full pipeline: search + download + compile → returns video
  GET  /api/word-cut/video/<id> → download generated video
  GET  /health                → health check
"""
import subprocess, json, os, sys, re, tempfile, shutil, glob, uuid, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, Response, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=os.environ.get('CORS_ORIGINS', '*').split(','))

OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/tmp/wordcut-output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Cleanup old files (>1h)
def cleanup_old():
    try:
        now = time.time()
        for f in os.listdir(OUTPUT_DIR):
            fp = os.path.join(OUTPUT_DIR, f)
            if os.path.isfile(fp) and now - os.path.getmtime(fp) > 3600:
                os.remove(fp)
    except: pass

SKIP_WORDS = ['music video', 'official video', 'official audio',
              'شيلة', 'أغنية', 'كليب', 'lyrics', 'song', 'remix', 'cover']


def search_youtube(word, max_videos=60):
    """Search YouTube for videos containing the word."""
    # Detect language: if mostly ASCII, use English queries
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


def get_word_timestamps(word, vid_id, title, workdir):
    """Get precise word timestamps from YouTube json3 subtitles."""
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

            # Build a flat list of all segments with absolute timestamps
            all_segs = []
            for ev in data.get('events', []):
                tStart = ev.get('tStartMs', 0)
                for seg in ev.get('segs', []):
                    text = seg.get('utf8', '').strip()
                    if not text or text == '\n':
                        continue
                    offset = seg.get('tOffsetMs', 0)
                    abs_start = tStart + offset
                    all_segs.append({'text': text, 'start_ms': abs_start})

            if is_phrase:
                # Phrase search: slide a window over consecutive segments
                full_text_parts = [(s['text'], s['start_ms']) for s in all_segs]
                for i in range(len(full_text_parts)):
                    # Build phrase from segments starting at i
                    combined = ''
                    for j in range(i, min(i + len(word_tokens) + 5, len(full_text_parts))):
                        if combined:
                            combined += ' '
                        combined += full_text_parts[j][0]
                        if word_lower in combined.lower():
                            phrase_start_ms = full_text_parts[i][1]
                            # End: estimate from last matched segment
                            phrase_end_ms = full_text_parts[j][1] + max(200, len(full_text_parts[j][0]) * 80)
                            if j + 1 < len(full_text_parts):
                                phrase_end_ms = full_text_parts[j + 1][1]
                            
                            is_exact = combined.lower().strip() == word_lower
                            if best_match is None or (is_exact and not best_match.get('exact')):
                                best_match = {
                                    'word_start': phrase_start_ms / 1000.0,
                                    'word_end': phrase_end_ms / 1000.0,
                                    'word_text': combined.strip(),
                                    'exact': is_exact,
                                    'vid_id': vid_id,
                                    'title': title,
                                    'vid_dir': vid_dir,
                                    'url': url
                                }
                            break
                    if best_match and best_match.get('exact'):
                        break
            else:
                # Single word search
                for si, seg in enumerate(all_segs):
                    text = seg['text']
                    if word in text or word_lower in text.lower():
                        word_start_ms = seg['start_ms']

                        word_end_ms = None
                        if si + 1 < len(all_segs):
                            word_end_ms = all_segs[si + 1]['start_ms']
                        if word_end_ms is None:
                            char_count = len(text)
                            word_end_ms = word_start_ms + max(200, min(char_count * 80, 600))

                        is_exact = text.lower().strip() == word_lower or text.strip() == word
                        if best_match is None or (is_exact and not best_match.get('exact')):
                            best_match = {
                                'word_start': word_start_ms / 1000.0,
                                'word_end': word_end_ms / 1000.0,
                                'word_text': text,
                                'exact': is_exact,
                                'vid_id': vid_id,
                                'title': title,
                                'vid_dir': vid_dir,
                                'url': url
                            }
                            if is_exact:
                                break
                if best_match and best_match.get('exact'):
                    break
        except:
            continue

    return best_match


def download_clip(info):
    """Download just the word segment from YouTube."""
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
                 '-movflags', '+faststart',
                 precise_path],
                capture_output=True, timeout=15
            )

            if os.path.exists(precise_path) and os.path.getsize(precise_path) > 3000:
                return precise_path
    except:
        pass
    return None


def compile_clips(clips, output_path):
    """Compile clips into one video using concat filter."""
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
            '-movflags', '+faststart', output_path],
        capture_output=True, timeout=120
    )
    return os.path.exists(output_path)


# ── Jobs store (in-memory) ──────────────────────────────────────────────────

jobs = {}  # job_id → {status, progress, result, error}


def run_pipeline(job_id, word, target_clips, lang):
    """Full pipeline in background thread."""
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

        # Download clips
        jobs[job_id]['status'] = 'downloading'
        clips = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(download_clip, info): info for info in results}
            for f in as_completed(futures):
                try:
                    path = f.result()
                    if path:
                        clips.append(path)
                        jobs[job_id]['progress'] = {
                            'step': 'download', 'done': len(clips), 'target': target_clips
                        }
                        if len(clips) >= target_clips:
                            break
                except:
                    pass

        if len(clips) < 2:
            jobs[job_id] = {'status': 'failed', 'error': 'Failed to download clips'}
            return

        # Compile
        jobs[job_id]['status'] = 'compiling'
        use_clips = clips[:target_clips]
        output_path = os.path.join(OUTPUT_DIR, f"{job_id}.mp4")

        if compile_clips(use_clips, output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            probe = subprocess.run(
                ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'json', output_path],
                capture_output=True, text=True
            )
            dur = json.loads(probe.stdout).get('format', {}).get('duration', '0')
            jobs[job_id] = {
                'status': 'done',
                'result': {
                    'video_id': job_id,
                    'duration': float(dur),
                    'size_mb': round(size_mb, 1),
                    'clips': len(use_clips),
                    'word': word
                }
            }
        else:
            jobs[job_id] = {'status': 'failed', 'error': 'Compilation failed'}

    except Exception as e:
        jobs[job_id] = {'status': 'failed', 'error': str(e)}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'word-cut-api'})


@app.route('/api/word-cut/generate', methods=['POST'])
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
def job_status(job_id):
    """Poll job status."""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(jobs[job_id])


@app.route('/api/word-cut/video/<job_id>', methods=['GET'])
def get_video(job_id):
    """Download generated video."""
    path = os.path.join(OUTPUT_DIR, f"{job_id}.mp4")
    if not os.path.exists(path):
        return jsonify({'error': 'Video not found'}), 404

    # Support range requests for streaming
    range_header = request.headers.get('Range')
    file_size = os.path.getsize(path)

    if range_header:
        byte1, byte2 = 0, None
        match = re.search(r'bytes=(\d+)-(\d*)', range_header)
        if match:
            byte1 = int(match.group(1))
            if match.group(2):
                byte2 = int(match.group(2))
        byte2 = byte2 or file_size - 1
        length = byte2 - byte1 + 1

        with open(path, 'rb') as f:
            f.seek(byte1)
            data = f.read(length)

        resp = Response(data, 206, mimetype='video/mp4')
        resp.headers.add('Content-Range', f'bytes {byte1}-{byte2}/{file_size}')
        resp.headers.add('Accept-Ranges', 'bytes')
        resp.headers.add('Content-Length', str(length))
        return resp

    return send_file(path, mimetype='video/mp4', as_attachment=True,
                     download_name=f'wordcut-{job_id}.mp4')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
