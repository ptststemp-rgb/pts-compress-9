#!/usr/bin/env node
/**
 * compress-worker.js — GitHub Actions video compression worker
 *
 * Downloads video → FFmpeg encode to H.264 540p MP4 (faststart) →
 * Uploads MP4 to VPS → Calls VPS callback.
 *
 * Supports source URL types:
 *   - Direct download URLs
 *   - vcloud.zip links (auto-resolved to direct download)
 *   - YouTube share links (downloaded via yt-dlp)
 *
 * Sends real-time progress updates to VPS every second via PROGRESS_URL.
 *
 * Environment variables (set by GitHub Actions):
 *   VPS_COMPRESS_SECRET - Shared secret for VPS auth
 *   JOB_ID             - Job queue ID
 *   MEDIA_ID           - Media ID in database
 *   FILE_NAME          - Base name for output file
 *   SOURCE_URL         - Direct download URL for source video
 *   CALLBACK_URL       - VPS callback endpoint (completion)
 *   PROGRESS_URL       - VPS progress endpoint (real-time)
 *   UPLOAD_URL         - VPS upload endpoint base URL
 */

const fs = require("fs");
const path = require("path");
const { execSync, spawn } = require("child_process");
const axios = require("axios");

// ─── Config ──────────────────────────────────────────
const SECRET = process.env.VPS_COMPRESS_SECRET;
const JOB_ID = process.env.JOB_ID;
const MEDIA_ID = process.env.MEDIA_ID;
const FILE_NAME = (process.env.FILE_NAME || "output").replace(/\.mp4$/i, "");
const SOURCE_URL = process.env.SOURCE_URL;
const CALLBACK_URL = process.env.CALLBACK_URL;
const PROGRESS_URL = process.env.PROGRESS_URL;
const UPLOAD_URL = process.env.UPLOAD_URL; // e.g. http://163.245.223.113
const AUDIO_LANGUAGE = (process.env.AUDIO_LANGUAGE || "").trim();

const TEMP_DIR = "/tmp/compress";
const INPUT_FILE = path.join(TEMP_DIR, "input.mp4");
const OUTPUT_FILE = path.join(TEMP_DIR, "output.mp4");

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

// ─── URL Type Detection ─────────────────────────────
function detectSourceType(url) {
  if (/^https?:\/\/(www\.)?youtube\.com\/|^https?:\/\/youtu\.be\//i.test(url)) return "youtube";
  if (/^https?:\/\/(www\.)?vcloud\.zip\//i.test(url)) return "vcloud";
  return "direct";
}

// ─── vcloud.zip Resolver (2-step extraction) ────────
async function resolveVcloudUrl(url) {
  log("Resolving vcloud.zip link (step 1)...");
  updateProgress({ phase: "resolving", percent: 10, detail: "Resolving vcloud.zip link..." });

  // Step 1: Fetch vcloud.zip page → extract intermediate URL
  const page1 = await withRetry(() => axios.get(url, {
    timeout: 30000,
    maxRedirects: 5,
    headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" }
  }), "vcloud page1");

  const match1 = page1.data.match(/var\s+url\s*=\s*'([^']+)'/)
    || page1.data.match(/var\s+url\s*=\s*"([^"]+)"/);
  if (!match1) throw new Error("vcloud.zip: Could not extract intermediate URL from page 1");

  const intermediateUrl = match1[1];
  log(`vcloud.zip step 1 → ${intermediateUrl.slice(0, 80)}...`);
  updateProgress({ phase: "resolving", percent: 50, detail: "Extracting direct link..." });

  // Step 2: Fetch intermediate page → extract direct download URL
  const page2 = await withRetry(() => axios.get(intermediateUrl, {
    timeout: 30000,
    maxRedirects: 5,
    headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" }
  }), "vcloud page2");

  const match2 = page2.data.match(/var\s+url\s*=\s*'([^']+)'/)
    || page2.data.match(/var\s+url\s*=\s*"([^"]+)"/);
  if (!match2) throw new Error("vcloud.zip: Could not extract direct URL from page 2");

  const directUrl = match2[1];
  log(`vcloud.zip resolved → ${directUrl.slice(0, 100)}...`);
  updateProgress({ phase: "resolving", percent: 100, detail: "Link resolved" });
  await sendProgress(true);
  return directUrl;
}

// ─── YouTube Download (yt-dlp) ──────────────────────
async function downloadYouTube(url) {
  log(`Downloading from YouTube: ${url}`);
  updateProgress({ phase: "downloading", percent: 0, detail: "Downloading from YouTube via yt-dlp..." });
  await sendProgress(true);

  // Download best video+audio up to source quality (yt-dlp will merge to mkv/mp4)
  const args = [
    "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    "--merge-output-format", "mp4",
    "-o", INPUT_FILE,
    "--no-playlist",
    "--no-check-certificates",
    "--retries", "3",
    "--socket-timeout", "30",
    url
  ];

  return new Promise((resolve, reject) => {
    const child = spawn("yt-dlp", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      // Parse yt-dlp progress: [download]  45.2% of ~500MiB at 10.5MiB/s ETA 00:25
      const pctMatch = text.match(/(\d+\.?\d*)%/);
      if (pctMatch) {
        const pct = parseFloat(pctMatch[1]);
        const speedMatch = text.match(/at\s+([\d.]+\S+)/);
        const etaMatch = text.match(/ETA\s+(\S+)/);
        updateProgress({
          phase: "downloading",
          percent: Math.min(99, Math.round(pct * 10) / 10),
          detail: `YouTube: ${pct.toFixed(1)}%${speedMatch ? " at " + speedMatch[1] : ""}${etaMatch ? " ETA " + etaMatch[1] : ""}`
        });
      }
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 4000) stderr = stderr.slice(-4000);
    });

    child.on("close", (code) => {
      if (code === 0 && fs.existsSync(INPUT_FILE)) {
        const size = fs.statSync(INPUT_FILE).size;
        log(`YouTube download complete: ${(size / 1024 / 1024).toFixed(1)} MB`);
        updateProgress({ phase: "downloading", percent: 100, detail: `${(size / 1024 / 1024).toFixed(1)} MB downloaded` });
        sendProgress(true);
        resolve(size);
      } else {
        reject(new Error(`yt-dlp failed (code ${code}): ${stderr.slice(-300)}`));
      }
    });
    child.on("error", (err) => reject(new Error(`yt-dlp not found or error: ${err.message}`)));
  });
}

// ─── Step 1: Download source video (parallel chunks) ─
async function download(downloadUrl) {
  log(`Downloading from: ${downloadUrl.slice(0, 80)}...`);
  updateProgress({ phase: "downloading", percent: 0, detail: "Checking source..." });

  // HEAD request to get content-length and check Range support
  let totalBytes = 0;
  let acceptsRanges = false;
  try {
    const head = await withRetry(() => axios.head(downloadUrl, { timeout: 30000, maxRedirects: 5 }), "HEAD");
    totalBytes = parseInt(head.headers["content-length"] || "0", 10);
    acceptsRanges = (head.headers["accept-ranges"] || "").toLowerCase() === "bytes";
  } catch (_) {
    const probe = await withRetry(() => axios.get(downloadUrl, { responseType: "stream", timeout: 30000, maxRedirects: 5 }), "probe");
    totalBytes = parseInt(probe.headers["content-length"] || "0", 10);
    acceptsRanges = (probe.headers["accept-ranges"] || "").toLowerCase() === "bytes";
    probe.data.destroy();
  }

  log(`Source: ${(totalBytes / 1024 / 1024).toFixed(1)} MB, Range support: ${acceptsRanges}`);

  // Use parallel chunked download if Range is supported and file is large enough
  if (acceptsRanges && totalBytes > DOWNLOAD_CHUNK_SIZE * 2) {
    await downloadParallel(downloadUrl, totalBytes);
  } else {
    await downloadSingle(downloadUrl, totalBytes);
  }

  const size = fs.statSync(INPUT_FILE).size;
  log(`Downloaded: ${(size / 1024 / 1024).toFixed(1)} MB`);
  updateProgress({ phase: "downloading", percent: 100, speed: null, eta: 0, detail: `${(size / 1024 / 1024).toFixed(1)} MB downloaded` });
  await sendProgress(true);
  return size;
}

async function downloadSingle(downloadUrl, totalBytes) {
  log("Downloading (single stream)...");
  updateProgress({ phase: "downloading", percent: 0, detail: "Downloading..." });

  const resp = await withRetry(() => axios.get(downloadUrl, {
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

async function downloadParallel(downloadUrl, totalBytes) {
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
    const resp = await withRetry(() => axios.get(downloadUrl, {
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

// ─── Step 2: FFmpeg encode to H.264 540p MP4 (faststart) ─
async function encodeMP4() {
  log("Encoding to H.264 540p MP4 (faststart)...");

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
    const langCodes = {
      english: ["eng", "en"], hindi: ["hin", "hi"], telugu: ["tel", "te"],
      tamil: ["tam", "ta"], kannada: ["kan", "kn"], malayalam: ["mal", "ml"],
      korean: ["kor", "ko"], japanese: ["jpn", "ja"]
    };
    const codes = langCodes[lang] || [lang];
    const allCodes = [lang, ...codes];

    let match = audioStreams.find(a => allCodes.includes(a.lang) || allCodes.some(c => a.title.includes(c)));
    if (match) {
      audioMapArgs = ["-map", "0:v:0", "-map", `0:${match.index}`];
      log(`Selected audio stream #${match.index} (lang=${match.lang}, title=${match.title}) for ${AUDIO_LANGUAGE}`);
    } else {
      log(`WARNING: No audio stream matching "${AUDIO_LANGUAGE}" found, using first audio track`);
    }
  }

  // Choose output height: cap at 540p, keep original if already smaller
  const targetHeight = Math.min(540, inputHeight || 540);

  const args = [
    "-y", "-i", INPUT_FILE,
    ...audioMapArgs,
    "-c:v", "libx264",
    "-crf", "25",
    "-preset", "medium",
    "-vf", `scale=-2:'min(${targetHeight},ih)'`,
    "-pix_fmt", "yuv420p",
    "-c:a", "aac", "-b:a", "96k", "-ac", "2",
    "-movflags", "+faststart",
    "-sn", "-dn",
    "-progress", "pipe:1",
    OUTPUT_FILE
  ];

  return new Promise((resolve, reject) => {
    const child = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    let currentTimeSec = 0;

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
        const outputSize = fs.statSync(OUTPUT_FILE).size;
        log(`MP4 output: ${(outputSize / 1024 / 1024).toFixed(1)} MB`);
        updateProgress({ phase: "converting", percent: 100, eta: 0, detail: `${(outputSize / 1024 / 1024).toFixed(1)} MB encoded` });
        sendProgress(true);
        resolve({ outputSize, duration });
      } else {
        reject(new Error(`FFmpeg failed (code ${code}): ${stderr.slice(-300)}`));
      }
    });
    child.on("error", reject);
  });
}

// ─── Step 3: Upload MP4 to VPS ───────────────────────
async function uploadToVPS() {
  const fileSize = fs.statSync(OUTPUT_FILE).size;
  log(`Uploading ${(fileSize / 1024 / 1024).toFixed(1)} MB to VPS...`);
  updateProgress({ phase: "uploading", percent: 0, speed: null, eta: null, detail: "Uploading to VPS..." });
  await sendProgress(true);

  const uploadUrl = `${UPLOAD_URL}/api/media/${encodeURIComponent(MEDIA_ID)}/upload`;
  const uploadStart = Date.now();

  // Stream the file to VPS with progress tracking
  const fileStream = fs.createReadStream(OUTPUT_FILE);
  let uploadedBytes = 0;

  fileStream.on("data", (chunk) => {
    uploadedBytes += chunk.length;
    const now = Date.now();
    const elapsedSec = (now - uploadStart) / 1000;
    const speed = elapsedSec > 0 ? uploadedBytes / elapsedSec : 0;
    const remaining = speed > 0 ? (fileSize - uploadedBytes) / speed : null;
    const pct = (uploadedBytes / fileSize) * 100;

    updateProgress({
      phase: "uploading",
      percent: Math.round(pct * 10) / 10,
      speed: Math.round(speed),
      eta: remaining != null && Number.isFinite(remaining) ? Math.round(remaining) : null,
      detail: `${(uploadedBytes / 1024 / 1024).toFixed(1)} / ${(fileSize / 1024 / 1024).toFixed(1)} MB`
    });
  });

  const resp = await withRetry(() => axios.put(uploadUrl, fs.createReadStream(OUTPUT_FILE), {
    headers: {
      "X-Compress-Secret": SECRET,
      "Content-Type": "application/octet-stream",
      "Content-Length": fileSize
    },
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    timeout: 600000 // 10 min for large files
  }), "VPS upload", 3);

  if (!resp.data?.ok) throw new Error(`VPS upload failed: ${JSON.stringify(resp.data).slice(0, 300)}`);

  const elapsed = ((Date.now() - uploadStart) / 1000).toFixed(1);
  log(`Uploaded to VPS in ${elapsed}s (${(fileSize / 1024 / 1024).toFixed(1)} MB)`);
  updateProgress({ phase: "uploading", percent: 100, detail: `Uploaded in ${elapsed}s` });
  await sendProgress(true);

  return fileSize;
}

// ─── Step 4: Callback to VPS ─────────────────────────
async function callback(videoSize, originalSize) {
  log(`Calling back to VPS: ${CALLBACK_URL}`);
  updateProgress({ phase: "done", percent: 100, detail: "Sending results..." });
  await sendProgress(true);

  const resp = await withRetry(() => axios.post(CALLBACK_URL, {
    mediaId: MEDIA_ID,
    jobId: JOB_ID,
    video: { size: videoSize },
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
  if (!MEDIA_ID || !CALLBACK_URL || !SOURCE_URL || !UPLOAD_URL) {
    console.error("Missing required env vars (MEDIA_ID, CALLBACK_URL, SOURCE_URL, UPLOAD_URL)");
    process.exit(1);
  }

  fs.mkdirSync(TEMP_DIR, { recursive: true });
  log(`=== Compress: ${MEDIA_ID} (job ${JOB_ID}) ===`);

  // Detect source URL type and resolve/download accordingly
  let resolvedUrl = SOURCE_URL;
  const sourceType = detectSourceType(SOURCE_URL);
  log(`Source type: ${sourceType}`);

  let originalSize;
  if (sourceType === "youtube") {
    originalSize = await downloadYouTube(SOURCE_URL);
  } else {
    if (sourceType === "vcloud") {
      resolvedUrl = await resolveVcloudUrl(SOURCE_URL);
    }
    originalSize = await download(resolvedUrl);
  }

  const { outputSize } = await encodeMP4();

  const reduction = originalSize > 0 ? ((1 - outputSize / originalSize) * 100).toFixed(1) : 0;
  log(`Size reduction: ${reduction}% (${(originalSize / 1024 / 1024).toFixed(1)} MB → ${(outputSize / 1024 / 1024).toFixed(1)} MB)`);

  const videoSize = await uploadToVPS();
  await callback(videoSize, originalSize);

  try { fs.unlinkSync(OUTPUT_FILE); } catch (_) {}
  log("=== Done! ===");
}

main().catch(err => {
  console.error(`\nFatal: ${err.message}`);

  // Clean up temp files on failure
  try { if (fs.existsSync(INPUT_FILE)) fs.unlinkSync(INPUT_FILE); } catch (_) {}
  try { if (fs.existsSync(OUTPUT_FILE)) fs.unlinkSync(OUTPUT_FILE); } catch (_) {}
  try { if (fs.existsSync(TEMP_DIR)) fs.rmSync(TEMP_DIR, { recursive: true, force: true }); } catch (_) {}
  log("Cleaned up temp files after failure");

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
