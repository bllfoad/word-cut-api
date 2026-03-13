#!/usr/bin/env python3
"""
yt-dlp Proxy API — Dumb YouTube operations proxy for zlabz.io
No business logic. Just wraps yt-dlp and ffmpeg.

Endpoints:
  POST /api/yt/search     → Search YouTube
  POST /api/yt/subtitles  → Get subtitles (json3)
  POST /api/yt/clip       → Download a precise clip
  GET  /health            → Health check
"""
import subprocess, json, os, re, tempfile, shutil
from functools import wraps
from flask import Flask, request, jsonify, Response, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=os.environ.get('CORS_ORIGINS', '*').split(','))

API_SECRET = os.environ.get('API_SECRET', 'wc_zlabz_2026_secret')


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-Key') or request.args.get('key')
        if key != API_SECRET:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'yt-proxy-api'})


@app.route('/api/yt/search', methods=['POST'])
@require_api_key
def yt_search():
    """Search YouTube for videos."""
    data = request.json or {}
    query = data.get('query', '').strip()
    max_results = min(int(data.get('max_results', 25)), 50)

    if not query:
        return jsonify({'error': 'query is required'}), 400

    try:
        r = subprocess.run(
            ['yt-dlp', '--flat-playlist', '--print', 'id', '--print', 'title',
             f'ytsearch{max_results}:{query}', '--no-warnings'],
            capture_output=True, text=True, timeout=30
        )
        lines = r.stdout.strip().split('\n')
        videos = []
        for i in range(0, len(lines) - 1, 2):
            vid_id = lines[i].strip()
            title = lines[i + 1].strip() if i + 1 < len(lines) else ''
            if vid_id and len(vid_id) == 11:
                videos.append({'id': vid_id, 'title': title})

        return jsonify({'videos': videos[:max_results]})
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Search timed out'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/yt/subtitles', methods=['POST'])
@require_api_key
def yt_subtitles():
    """Get subtitles for a video in json3 format."""
    data = request.json or {}
    video_id = data.get('video_id', '').strip()
    languages = data.get('languages', ['en', 'ar'])

    if not video_id:
        return jsonify({'error': 'video_id is required'}), 400

    workdir = tempfile.mkdtemp(prefix="ytsubs_")
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        lang_str = ','.join(languages)

        subprocess.run(
            ['yt-dlp', '--write-auto-sub', '--sub-lang', lang_str, '--skip-download',
             '--sub-format', 'json3', '-o', os.path.join(workdir, 'subs'),
             '--no-warnings', '--socket-timeout', '10', url],
            capture_output=True, text=True, timeout=20
        )

        # Find and read the json3 file
        import glob
        sub_files = glob.glob(os.path.join(workdir, 'subs*.json3'))
        if not sub_files:
            return jsonify({'error': 'No subtitles found', 'events': []}), 404

        # Return the first found subtitle file
        with open(sub_files[0]) as f:
            sub_data = json.load(f)

        return jsonify(sub_data)

    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Subtitle download timed out'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


@app.route('/api/yt/clip', methods=['POST'])
@require_api_key
def yt_clip():
    """Download a precise clip from a YouTube video."""
    data = request.json or {}
    video_id = data.get('video_id', '').strip()
    start = float(data.get('start', 0))
    end = float(data.get('end', 0))
    resolution = data.get('resolution', '720p')
    fmt = data.get('format', 'mp4')

    if not video_id:
        return jsonify({'error': 'video_id is required'}), 400
    if end <= start:
        return jsonify({'error': 'end must be greater than start'}), 400

    # Limit clip duration to 10s max
    if end - start > 10:
        return jsonify({'error': 'Clip too long (max 10s)'}), 400

    workdir = tempfile.mkdtemp(prefix="ytclip_")
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"

        # Download with padding for keyframe safety
        pad_start = max(0, start - 0.5)
        pad_end = end + 0.5

        res_height = 720 if resolution == '720p' else 1080
        clip_path = os.path.join(workdir, 'raw.mp4')

        subprocess.run(
            ['yt-dlp', '-f', f'best[height>={res_height}]/best',
             '--download-sections', f'*{pad_start}-{pad_end}',
             '-o', clip_path, '--no-warnings', '--socket-timeout', '15',
             '--force-keyframes-at-cuts', url],
            capture_output=True, text=True, timeout=60
        )

        if not os.path.exists(clip_path) or os.path.getsize(clip_path) < 5000:
            return jsonify({'error': 'Failed to download clip'}), 500

        # Precise cut with ffmpeg
        local_start = max(0, start - pad_start)
        duration = end - start
        precise_path = os.path.join(workdir, 'precise.mp4')

        subprocess.run(
            ['ffmpeg', '-y', '-i', clip_path,
             '-ss', str(local_start), '-t', str(duration),
             '-vf', f'scale=-2:{res_height}:force_original_aspect_ratio=decrease,pad={res_height * 16 // 9}:{res_height}:(ow-iw)/2:(oh-ih)/2,fps=30',
             '-c:v', 'libx264', '-preset', 'fast', '-crf', '20',
             '-profile:v', 'high', '-pix_fmt', 'yuv420p',
             '-c:a', 'aac', '-ar', '44100', '-ac', '2', '-b:a', '128k',
             '-movflags', '+faststart', precise_path],
            capture_output=True, timeout=30
        )

        if not os.path.exists(precise_path) or os.path.getsize(precise_path) < 3000:
            return jsonify({'error': 'Failed to process clip'}), 500

        # Stream the file back
        response = send_file(
            precise_path,
            mimetype='video/mp4',
            download_name=f'clip-{video_id}-{start:.1f}.mp4'
        )

        # Cleanup after response (register cleanup)
        @response.call_on_close
        def cleanup():
            shutil.rmtree(workdir, ignore_errors=True)

        return response

    except subprocess.TimeoutExpired:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({'error': 'Clip download timed out'}), 504
    except Exception as e:
        shutil.rmtree(workdir, ignore_errors=True)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
