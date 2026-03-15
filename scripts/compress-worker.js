#!/usr/bin/env node
/**
 * compress-worker.js — GitHub Actions HLS video compression worker
 *
 * Downloads video → FFmpeg encode to H.265 480p HLS (fmp4 segments) →
 * Uploads all HLS files to SpaceByte → Calls VPS callback.
 *
 * Sends real-time progress updates to VPS every second via PROGRESS_URL.
 *
 * Environment variables (set by GitHub Actions):
 *   SPACEBYTE_API_TOKEN        - SpaceByte API auth token
 *   SPACEBYTE_PARENT_FOLDER_ID - Parent folder (optional)
 *   VPS_COMPRESS_SECRET        - Shared secret for callback auth
 *   JOB_ID                     - Job queue ID
 *   MEDIA_ID                   - Media ID in database
 *   FILE_NAME                  - Base name for HLS folder
 *   SOURCE_URL                 - Direct download URL for source video
 *   CALLBACK_URL               - VPS callback endpoint (completion)
 *   PROGRESS_URL               - VPS progress endpoint (real-time)
 */

const fs = require("fs");
const path = require("path");
const { execSync, spawn } = require("child_process");
const axios = require("axios");

// ─── Config ──────────────────────────────────────────
const SB_TOKEN = process.env.SPACEBYTE_API_TOKEN;
const SB_PARENT = process.env.SPACEBYTE_PARENT_FOLDER_ID || null;
const SECRET = process.env.VPS_COMPRESS_SECRET;
const JOB_ID = process.env.JOB_ID;
const MEDIA_ID = process.env.MEDIA_ID;
const FILE_NAME = (process.env.FILE_NAME || "output").replace(/\.mp4$/i, "");
const SOURCE_URL = process.env.SOURCE_URL;
const CALLBACK_URL = process.env.CALLBACK_URL;
const PROGRESS_URL = process.env.PROGRESS_URL;
const CATEGORY = (process.env.CATEGORY || "movie").toLowerCase();
const AUDIO_LANGUAGE = (process.env.AUDIO_LANGUAGE || "").trim();

const SB_API = "https://spacebyte.in/api/v1";
const TEMP_DIR = "/tmp/compress";
const INPUT_FILE = path.join(TEMP_DIR, "input.mp4");
const HLS_DIR = path.join(TEMP_DIR, "hls");

const PARALLEL_UPLOADS = 10;
const PARALLEL_DOWNLOAD_CHUNKS = 4;
const DOWNLOAD_CHUNK_SIZE = 10 * 1024 * 1024; // 10 MB per chunk
const PROGRESS_INTERVAL = 1000; // 1 second

function log(msg) { console.log(`[${new Date().toISOString()}] ${msg}`); }

// ─── Progress Reporter ───────────────────────────────
let lastProgressSent = 0;
const progressState = {
  phase: "starting",
  percent: 0,
  speed: null,
  eta: null,
  detail: ""
};

async function sendProgress(force = false) {
  if (!PROGRESS_URL) return;
  const now = Date.now();
  if (!force && now - lastProgressSent < PROGRESS_INTERVAL) return;
  lastProgressSent = now;

  try {
    await axios.post(PROGRESS_URL, {
      secret: SECRET,
      jobId: JOB_ID,
      mediaId: MEDIA_ID,
      progress: { ...progressState }
    }, { timeout: 5000 });
  } catch (_) {
    // Non-fatal: don't let progress reporting failures kill the job
  }
}

function updateProgress(updates) {
  Object.assign(progressState, updates);
  sendProgress();
}

// ─── Retry helper for transient errors ──────────────
const RETRYABLE_CODES = new Set([429, 408, 500, 502, 503, 504, 522, 524]);
async function withRetry(fn, label = "", maxRetries = 5) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const status = err.response?.status;
      const isRetryable = RETRYABLE_CODES.has(status) || err.code === "ECONNRESET" || err.code === "ETIMEDOUT" || err.code === "ECONNABORTED";
      if (isRetryable && attempt < maxRetries) {
        const retryAfter = parseInt(err.response?.headers?.["retry-after"] || "0", 10);
        const wait = retryAfter > 0 ? retryAfter * 1000 : Math.min(3000 * Math.pow(2, attempt), 60000);
        log(`  ${status || err.code} ${label ? "(" + label + ") " : ""}waiting ${(wait/1000).toFixed(0)}s (attempt ${attempt+1}/${maxRetries})`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }
      throw err;
    }
  }
}

// ─── Step 1: Download source video (parallel chunks) ─
async function download() {
  log(`Downloading from: ${SOURCE_URL.slice(0, 80)}...`);
  updateProgress({ phase: "downloading", percent: 0, detail: "Checking source..." });

  // HEAD request to get content-length and check Range support
  let totalBytes = 0;
  let acceptsRanges = false;
  try {
    const head = await withRetry(() => axios.head(SOURCE_URL, { timeout: 30000, maxRedirects: 5 }), "HEAD");
    totalBytes = parseInt(head.headers["content-length"] || "0", 10);
    acceptsRanges = (head.headers["accept-ranges"] || "").toLowerCase() === "bytes";
  } catch (_) {
    const probe = await withRetry(() => axios.get(SOURCE_URL, { responseType: "stream", timeout: 30000, maxRedirects: 5 }), "probe");
    totalBytes = parseInt(probe.headers["content-length"] || "0", 10);
    acceptsRanges = (probe.headers["accept-ranges"] || "").toLowerCase() === "bytes";
    probe.data.destroy();
  }

  log(`Source: ${(totalBytes / 1024 / 1024).toFixed(1)} MB, Range support: ${acceptsRanges}`);

  // Use parallel chunked download if Range is supported and file is large enough
  if (acceptsRanges && totalBytes > DOWNLOAD_CHUNK_SIZE * 2) {
    await downloadParallel(totalBytes);
  } else {
    await downloadSingle(totalBytes);
  }

  const size = fs.statSync(INPUT_FILE).size;
  log(`Downloaded: ${(size / 1024 / 1024).toFixed(1)} MB`);
  updateProgress({ phase: "downloading", percent: 100, speed: null, eta: 0, detail: `${(size / 1024 / 1024).toFixed(1)} MB downloaded` });
  await sendProgress(true);
  return size;
}

async function downloadSingle(totalBytes) {
  log("Downloading (single stream)...");
  updateProgress({ phase: "downloading", percent: 0, detail: "Downloading..." });

  const resp = await withRetry(() => axios.get(SOURCE_URL, {
    responseType: "stream", timeout: 600000, maxRedirects: 5
  }), "download");

  if (!totalBytes) totalBytes = parseInt(resp.headers["content-length"] || "0", 10);
  let downloadedBytes = 0;
  let lastSpeedCheck = Date.now();
  let lastSpeedBytes = 0;
  const writer = fs.createWriteStream(INPUT_FILE);

  resp.data.on("data", (chunk) => {
    downloadedBytes += chunk.length;
    const now = Date.now();
    const elapsed = (now - lastSpeedCheck) / 1000;
    if (elapsed >= 1) {
      const speed = (downloadedBytes - lastSpeedBytes) / elapsed;
      const remaining = totalBytes > 0 ? (totalBytes - downloadedBytes) / speed : null;
      const pct = totalBytes > 0 ? Math.min(99, (downloadedBytes / totalBytes) * 100) : 0;
      updateProgress({
        phase: "downloading", percent: Math.round(pct * 10) / 10,
        speed: Math.round(speed),
        eta: remaining != null && Number.isFinite(remaining) ? Math.round(remaining) : null,
        detail: `${(downloadedBytes / 1024 / 1024).toFixed(1)} / ${totalBytes > 0 ? (totalBytes / 1024 / 1024).toFixed(1) + " MB" : "?"}`
      });
      lastSpeedCheck = now; lastSpeedBytes = downloadedBytes;
    }
  });

  resp.data.pipe(writer);
  await new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
    resp.data.on("error", reject);
  });
}

async function downloadParallel(totalBytes) {
  // Split into chunks
  const chunks = [];
  for (let start = 0; start < totalBytes; start += DOWNLOAD_CHUNK_SIZE) {
    const end = Math.min(start + DOWNLOAD_CHUNK_SIZE - 1, totalBytes - 1);
    chunks.push({ index: chunks.length, start, end });
  }

  log(`Parallel download: ${chunks.length} chunks × ${(DOWNLOAD_CHUNK_SIZE / 1024 / 1024).toFixed(0)} MB, ${PARALLEL_DOWNLOAD_CHUNKS} threads`);
  updateProgress({ phase: "downloading", percent: 0, detail: `0/${chunks.length} chunks (${PARALLEL_DOWNLOAD_CHUNKS} threads)` });

  // Pre-allocate the file
  const fd = fs.openSync(INPUT_FILE, "w");
  fs.ftruncateSync(fd, totalBytes);
  fs.closeSync(fd);

  let completedChunks = 0;
  let downloadedBytes = 0;
  const downloadStart = Date.now();

  async function downloadChunk(chunk) {
    const resp = await withRetry(() => axios.get(SOURCE_URL, {
      responseType: "arraybuffer",
      timeout: 300000,
      maxRedirects: 5,
      headers: { Range: `bytes=${chunk.start}-${chunk.end}` }
    }), `chunk ${chunk.index}`);

    // Write at exact offset
    const chunkFd = fs.openSync(INPUT_FILE, "r+");
    fs.writeSync(chunkFd, Buffer.from(resp.data), 0, resp.data.byteLength, chunk.start);
    fs.closeSync(chunkFd);

    completedChunks++;
    downloadedBytes += resp.data.byteLength;

    const elapsedSec = (Date.now() - downloadStart) / 1000;
    const speed = elapsedSec > 0 ? downloadedBytes / elapsedSec : 0;
    const remaining = speed > 0 ? (totalBytes - downloadedBytes) / speed : null;
    const pct = Math.min(99, (downloadedBytes / totalBytes) * 100);

    updateProgress({
      phase: "downloading",
      percent: Math.round(pct * 10) / 10,
      speed: Math.round(speed),
      eta: remaining != null && Number.isFinite(remaining) ? Math.round(remaining) : null,
      detail: `${completedChunks}/${chunks.length} chunks (${(downloadedBytes / 1024 / 1024).toFixed(1)} MB)`
    });
  }

  // Parallel execution with concurrency limit
  const queue = [...chunks];
  const active = new Set();

  while (queue.length > 0 || active.size > 0) {
    while (active.size < PARALLEL_DOWNLOAD_CHUNKS && queue.length > 0) {
      const chunk = queue.shift();
      const p = downloadChunk(chunk).catch(err => {
        log(`  Retry chunk ${chunk.index} (${err.message.slice(0, 60)})`);
        return downloadChunk(chunk);
      });
      active.add(p);
      p.finally(() => active.delete(p));
    }
    if (active.size > 0) await Promise.race([...active]);
  }
}

// ─── Step 2: FFmpeg encode to H.264 540p HLS ─────────
async function encodeHLS() {
  log("Encoding to H.265 480p HLS (fmp4 segments)...");
  fs.mkdirSync(HLS_DIR, { recursive: true });

  updateProgress({ phase: "converting", percent: 0, speed: null, eta: null, detail: "Probing input..." });
  await sendProgress(true);

  // Probe input for duration, resolution, and audio streams
  let duration = 0;
  let inputHeight = 0;
  let audioStreams = [];
  try {
    const probe = execSync(`ffprobe -v error -show_streams -show_format -of json "${INPUT_FILE}"`, { timeout: 30000 }).toString();
    const info = JSON.parse(probe);
    const video = info.streams?.find(s => s.codec_type === "video");
    duration = parseFloat(info.format?.duration || "0");
    inputHeight = parseInt(video?.height || "0", 10);
    audioStreams = (info.streams || []).filter(s => s.codec_type === "audio").map((s, i) => ({
      index: s.index,
      lang: (s.tags?.language || "").toLowerCase(),
      title: (s.tags?.title || "").toLowerCase(),
      channels: s.channels || 0,
      order: i
    }));
    if (video) log(`Input: ${video.width}x${video.height} ${video.codec_name} duration=${Math.round(duration)}s`);
    if (audioStreams.length > 1) log(`Audio streams: ${audioStreams.map(a => `#${a.index} lang=${a.lang} title=${a.title} ch=${a.channels}`).join(", ")}`);
  } catch (_) { /* non-fatal */ }

  // Select the best audio stream matching the requested language
  let audioMapArgs = [];
  if (AUDIO_LANGUAGE && audioStreams.length > 1) {
    const lang = AUDIO_LANGUAGE.toLowerCase();
    // Map common language names to ISO 639 codes
    const langCodes = {
      english: ["eng", "en"], hindi: ["hin", "hi"], telugu: ["tel", "te"],
      tamil: ["tam", "ta"], kannada: ["kan", "kn"], malayalam: ["mal", "ml"],
      korean: ["kor", "ko"], japanese: ["jpn", "ja"]
    };
    const codes = langCodes[lang] || [lang];
    const allCodes = [lang, ...codes];

    // Match by language tag or title
    let match = audioStreams.find(a => allCodes.includes(a.lang) || allCodes.some(c => a.title.includes(c)));
    if (match) {
      audioMapArgs = ["-map", "0:v:0", "-map", `0:${match.index}`];
      log(`Selected audio stream #${match.index} (lang=${match.lang}, title=${match.title}) for ${AUDIO_LANGUAGE}`);
    } else {
      log(`WARNING: No audio stream matching "${AUDIO_LANGUAGE}" found, using first audio track`);
    }
  }

  // Choose output height: cap at 480p, keep original if already smaller
  const targetHeight = Math.min(480, inputHeight || 480);

  const manifestPath = path.join(HLS_DIR, "index.m3u8");
  const args = [
    "-y", "-i", INPUT_FILE,
    ...audioMapArgs,
    "-c:v", "libx265",
    "-crf", "28",
    "-preset", "medium",
    "-vf", `scale=-2:'min(${targetHeight},ih)'`,
    "-pix_fmt", "yuv420p",
    "-tag:v", "hvc1",
    "-g", "48",
    "-keyint_min", "48",
    "-sc_threshold", "0",
    "-c:a", "aac", "-b:a", "96k", "-ac", "2",
    "-f", "hls",
    "-hls_time", "4",
    "-hls_playlist_type", "vod",
    "-hls_flags", "independent_segments",
    "-hls_segment_type", "fmp4",
    "-hls_fmp4_init_filename", "init.mp4",
    "-hls_segment_filename", path.join(HLS_DIR, "seg_%05d.m4s"),
    "-sn", "-dn",
    "-progress", "pipe:1",
    manifestPath
  ];

  return new Promise((resolve, reject) => {
    const child = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    let currentTimeSec = 0;

    // Parse -progress pipe:1 output (key=value lines)
    child.stdout.on("data", (chunk) => {
      const lines = chunk.toString().split("\n");
      for (const line of lines) {
        if (line.startsWith("out_time_us=")) {
          const us = parseInt(line.split("=")[1], 10);
          if (Number.isFinite(us) && us > 0) {
            currentTimeSec = us / 1000000;
            const pct = duration > 0 ? Math.min(99, (currentTimeSec / duration) * 100) : 0;
            const eta = duration > 0 && currentTimeSec > 0
              ? Math.round((duration - currentTimeSec) * (Date.now() - encodeStartTime) / (currentTimeSec * 1000))
              : null;

            updateProgress({
              phase: "converting",
              percent: Math.round(pct * 10) / 10,
              eta,
              detail: `${Math.round(currentTimeSec)}s / ${Math.round(duration)}s encoded`
            });
          }
        } else if (line.startsWith("speed=")) {
          const speedStr = line.split("=")[1]?.trim();
          if (speedStr && speedStr !== "N/A") {
            updateProgress({ detail: `${Math.round(currentTimeSec)}s / ${Math.round(duration)}s (${speedStr})` });
          }
        }
      }
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 8000) stderr = stderr.slice(-8000);
    });

    const encodeStartTime = Date.now();

    child.on("close", (code) => {
      if (code === 0) {
        try { fs.unlinkSync(INPUT_FILE); } catch (_) {}
        const files = fs.readdirSync(HLS_DIR);
        const totalSize = files.reduce((sum, f) => sum + fs.statSync(path.join(HLS_DIR, f)).size, 0);
        log(`HLS output: ${files.length} files, ${(totalSize / 1024 / 1024).toFixed(1)} MB total`);
        updateProgress({ phase: "converting", percent: 100, eta: 0, detail: `${files.length} segments created` });
        sendProgress(true);
        resolve({ files, totalSize, duration });
      } else {
        reject(new Error(`FFmpeg failed (code ${code}): ${stderr.slice(-300)}`));
      }
    });
    child.on("error", reject);
  });
}

// ─── SpaceByte folder helpers ────────────────────────
const sbHeaders = () => ({
  Authorization: `Bearer ${SB_TOKEN}`,
  "Content-Type": "application/json"
});

async function getOrCreateCategoryFolder() {
  // Category folder name: "Movies" or "Series"
  const categoryName = CATEGORY === "series" ? "Series" : "Movies";
  const parentId = SB_PARENT || null;

  // Search for existing folder (at root or under parent)
  try {
    const params = { per_page: 100 };
    if (parentId) params.parentIds = parentId;
    const resp = await withRetry(() => axios.get(`${SB_API}/drive/file-entries`, {
      headers: sbHeaders(), params, timeout: 30000
    }), "list folders");
    const entries = resp.data?.data || [];
    const existing = entries.find(f =>
      f.type === "folder" && (f.name || "").toLowerCase() === categoryName.toLowerCase()
    );
    if (existing) {
      log(`Found existing "${categoryName}" folder: ${existing.id}`);
      return String(existing.id);
    }
  } catch (err) {
    log(`Could not list existing folders: ${err.message} — will try creating`);
  }

  // Create the category folder
  const body = { name: categoryName };
  if (parentId) body.parentId = String(parentId);
  log(`Creating "${categoryName}" folder${parentId ? ` under parent ${parentId}` : " at root"}`);

  const resp = await withRetry(() => axios.post(`${SB_API}/folders`, body, {
    headers: sbHeaders(), timeout: 30000
  }), "create category folder");

  const id = resp.data?.folder?.id || resp.data?.id || resp.data?.data?.id;
  if (!id) throw new Error(`Failed to create ${categoryName} folder: ${JSON.stringify(resp.data).slice(0, 300)}`);
  return String(id);
}

// ─── Step 3: Upload to SpaceByte via direct S3 presigned URLs ──
async function uploadToSpaceByte(hlsFiles) {
  if (!SB_TOKEN) throw new Error("SPACEBYTE_API_TOKEN is required");
  log(`Uploading ${hlsFiles.length} HLS files to SpaceByte (direct S3)...`);
  updateProgress({ phase: "uploading", percent: 0, speed: null, eta: null, detail: "Uploading to SpaceByte..." });
  await sendProgress(true);

  const categoryFolderId = await getOrCreateCategoryFolder();
  const folderName = `hls_${FILE_NAME}_${Date.now()}`.slice(0, 200);

  const folderResp = await withRetry(() => axios.post(`${SB_API}/folders`, {
    name: folderName, parentId: categoryFolderId
  }, { headers: sbHeaders(), timeout: 30000 }), "create HLS folder");

  const folderId = folderResp.data?.folder?.id || folderResp.data?.id || folderResp.data?.data?.id;
  if (!folderId) throw new Error(`SpaceByte folder creation failed`);
  log(`SpaceByte folder: ${folderId}`);

  const fileMap = {};
  let uploaded = 0;
  let totalUploadedBytes = 0;
  const totalUploadSize = hlsFiles.reduce((sum, f) => sum + fs.statSync(path.join(HLS_DIR, f)).size, 0);
  const uploadStartTime = Date.now();

  async function uploadOne(fileName) {
    const filePath = path.join(HLS_DIR, fileName);
    const fileData = fs.readFileSync(filePath);
    const isManifest = fileName.endsWith(".m3u8");
    const contentType = isManifest ? "application/vnd.apple.mpegurl"
      : fileName.endsWith(".mp4") ? "video/mp4" : "video/iso.segment";
    const ext = fileName.split(".").pop();

    // Get presigned S3 URL
    const presign = await withRetry(() => axios.post(`${SB_API}/s3/simple/presign`, {
      filename: fileName, mime: contentType, size: fileData.length, extension: ext
    }, { headers: sbHeaders(), timeout: 30000 }), `presign ${fileName}`);

    const { url, key, acl } = presign.data;

    // Upload directly to S3
    await withRetry(() => axios.put(url, fileData, {
      headers: { "Content-Type": contentType, "x-amz-acl": acl },
      maxContentLength: Infinity, maxBodyLength: Infinity, timeout: 180000
    }), `s3put ${fileName}`, 3);

    // Register file entry in SpaceByte
    const entry = await withRetry(() => axios.post(`${SB_API}/s3/entries`, {
      filename: key, parentId: String(folderId), size: fileData.length,
      mime: contentType, clientMime: contentType, clientName: fileName, clientExtension: ext
    }, { headers: sbHeaders(), timeout: 30000 }), `register ${fileName}`);

    const id = entry.data?.fileEntry?.id;
    if (id) fileMap[fileName] = String(id);
    uploaded++;
    totalUploadedBytes += fileData.length;
    const pct = (uploaded / hlsFiles.length) * 100;
    const elapsedSec = (Date.now() - uploadStartTime) / 1000;
    const speed = elapsedSec > 0 ? totalUploadedBytes / elapsedSec : 0;
    const remaining = speed > 0 ? (totalUploadSize - totalUploadedBytes) / speed : null;
    updateProgress({
      phase: "uploading",
      percent: Math.round(pct * 10) / 10,
      speed: Math.round(speed),
      eta: remaining != null && Number.isFinite(remaining) ? Math.round(remaining) : null,
      detail: `${uploaded}/${hlsFiles.length} files (${(totalUploadedBytes / 1024 / 1024).toFixed(1)} / ${(totalUploadSize / 1024 / 1024).toFixed(1)} MB)`
    });
  }

  const queue = [...hlsFiles];
  const active = new Set();
  while (queue.length > 0 || active.size > 0) {
    while (active.size < PARALLEL_UPLOADS && queue.length > 0) {
      const fileName = queue.shift();
      const p = uploadOne(fileName).catch(err => {
        log(`  Upload failed: ${fileName} (${err.message.slice(0, 100)})`);
        throw err; // withRetry inside uploadOne already handles retries
      });
      active.add(p);
      p.finally(() => active.delete(p));
    }
    if (active.size > 0) await Promise.race([...active]);
  }

  const totalSize = hlsFiles.reduce((sum, f) => sum + fs.statSync(path.join(HLS_DIR, f)).size, 0);
  log(`SpaceByte backup: ${Object.keys(fileMap).length}/${hlsFiles.length} files, folder ${folderId}`);
  return { folderId: String(folderId), manifestFileId: fileMap["index.m3u8"] || null, fileMap, totalSize, segmentCount: hlsFiles.length };
}

// ─── Step 4: Callback to VPS ─────────────────────────
async function callback(sbData, originalSize) {
  log(`Calling back to VPS: ${CALLBACK_URL}`);
  updateProgress({ phase: "done", percent: 100, detail: "Sending results..." });
  await sendProgress(true);

  const resp = await withRetry(() => axios.post(CALLBACK_URL, {
    mediaId: MEDIA_ID,
    jobId: JOB_ID,
    hls: {
      segmentCount: sbData.segmentCount,
      totalSize: sbData.totalSize,
      folderId: sbData.folderId,
      manifestFileId: sbData.manifestFileId,
      fileMap: sbData.fileMap
    },
    originalSize,
    secret: SECRET
  }, {
    timeout: 60000,
    headers: { "Content-Type": "application/json" }
  }), "VPS callback", 3);

  if (resp.data?.ok) log("VPS confirmed");
  else log(`VPS response: ${JSON.stringify(resp.data).slice(0, 300)}`);
}

// ─── Main ────────────────────────────────────────────
async function main() {
  if (!MEDIA_ID || !CALLBACK_URL || !SOURCE_URL) {
    console.error("Missing required env vars (MEDIA_ID, CALLBACK_URL, SOURCE_URL)");
    process.exit(1);
  }
  if (!SB_TOKEN) {
    console.error("Missing SPACEBYTE_API_TOKEN");
    process.exit(1);
  }

  fs.mkdirSync(TEMP_DIR, { recursive: true });
  log(`=== HLS Compress: ${MEDIA_ID} (job ${JOB_ID}) ===`);

  const originalSize = await download();
  const { files, totalSize } = await encodeHLS();

  const reduction = originalSize > 0 ? ((1 - totalSize / originalSize) * 100).toFixed(1) : 0;
  log(`Size reduction: ${reduction}% (${(originalSize / 1024 / 1024).toFixed(1)} MB → ${(totalSize / 1024 / 1024).toFixed(1)} MB)`);

  // Upload to SpaceByte via direct S3 presigned URLs
  const sbData = await uploadToSpaceByte(files);

  await callback(sbData, originalSize);

  try { fs.rmSync(HLS_DIR, { recursive: true, force: true }); } catch (_) {}
  log("=== Done! ===");
}

main().catch(err => {
  console.error(`\nFatal: ${err.message}`);

  // Report failure
  const failProgress = async () => {
    if (PROGRESS_URL && SECRET) {
      await axios.post(PROGRESS_URL, {
        secret: SECRET, jobId: JOB_ID, mediaId: MEDIA_ID,
        progress: { phase: "failed", percent: 0, detail: err.message.slice(0, 200) }
      }, { timeout: 5000 }).catch(() => {});
    }
    if (CALLBACK_URL && SECRET) {
      await axios.post(CALLBACK_URL, {
        mediaId: MEDIA_ID, jobId: JOB_ID,
        error: err.message, secret: SECRET
      }, { timeout: 10000 }).catch(() => {});
    }
  };
  failProgress().finally(() => process.exit(1));
});
