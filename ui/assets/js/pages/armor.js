import { escapeHtml, escapeAttr } from "../shared/html-utils.js";

const uploadZone = document.getElementById("uploadZone");
const fileInput = document.getElementById("fileInput");
const uploadedCount = document.getElementById("uploadedCount");
const runBtn = document.getElementById("runBtn");
const runStatus = document.getElementById("runStatus");
const liveProgress = document.getElementById("liveProgress");
const pipelineStages = document.getElementById("pipelineStages");
const uploadedFileList = document.getElementById("uploadedFileList");
const runScope = document.getElementById("runScope");
const pipelineLog = document.getElementById("pipelineLog");
const fileListBody = document.getElementById("fileListBody");
const noRuns = document.getElementById("noRuns");
const chunkLogsBody = document.getElementById("chunkLogsBody");
const uploadZoneText = document.getElementById("uploadZoneText");
const scannedFilesBody = document.getElementById("scannedFilesBody");
const noScannedFiles = document.getElementById("noScannedFiles");
let progressPollId = null;
let lastLogKey = "";
/** Files uploaded in this session only (shown in uploader; run processes only these). */
let uploadedNow = [];

function setUploadEnabled(enabled) {
  if (enabled) {
    uploadZone.classList.remove("disabled");
    uploadZoneText.textContent = "Drop files here or click to select (PDF, DOCX, XLSX, TXT)";
  } else {
    uploadZone.classList.add("disabled");
    uploadZoneText.textContent =
      "Scan in progress — upload disabled. New uploads allowed when scan finishes.";
  }
}

uploadZone.onclick = () => fileInput.click();
uploadZone.ondragover = (e) => {
  e.preventDefault();
  uploadZone.classList.add("hover");
};
uploadZone.ondragleave = () => uploadZone.classList.remove("hover");
uploadZone.ondrop = (e) => {
  e.preventDefault();
  uploadZone.classList.remove("hover");
  if (uploadZone.classList.contains("disabled")) return;
  if (e.dataTransfer.files.length) doUpload(e.dataTransfer.files);
};
fileInput.onchange = () => {
  if (uploadZone.classList.contains("disabled")) return;
  if (fileInput.files.length) doUpload(fileInput.files);
};

async function doUpload(files) {
  const fd = new FormData();
  for (let i = 0; i < files.length; i++) fd.append("files", files[i]);
  const r = await fetch("/api/upload", { method: "POST", body: fd });
  const data = await r.json();
  if (data.uploaded && data.uploaded.length) {
    uploadedNow = [...uploadedNow, ...data.uploaded];
    refreshUploadedDisplay();
  }
}

function renderUploadedFileList(files) {
  if (!files || files.length === 0) {
    uploadedFileList.innerHTML = "";
    return;
  }
  uploadedFileList.innerHTML =
    "Names: " + files.map((f) => '<span class="file-name">' + escapeHtml(f) + "</span>").join("");
}

function renderRunScope(files) {
  if (!files || files.length === 0) {
    runScope.textContent =
      "Upload files above to run. Run will process only the files you upload in this session.";
    return;
  }
  runScope.textContent =
    "Run will process the following " + files.length + " file(s) only: " + files.join(", ");
}

function refreshUploadedDisplay() {
  uploadedCount.textContent = uploadedNow.length;
  runBtn.disabled = uploadedNow.length === 0;
  renderUploadedFileList(uploadedNow);
  renderRunScope(uploadedNow);
}

const STAGE_ORDER = [
  "extract",
  "chunking",
  "language_detection",
  "presidio",
  "gliner_xlarge",
  "gliner_gretelai",
  "gliner_urchade",
  "gliner_arabic",
  "qwen_ner",
  "agreement",
  "qwen_judge",
  "anonymisation",
  "chunk_done",
];
let armorQwenProgressLabel = "Qwen";
async function refreshArmorSettings() {
  try {
    const s = await fetch("/api/settings").then((r) => r.json());
    armorQwenProgressLabel = s.qwen_progress_label || "Qwen";
    const el = document.querySelector('.stage[data-stage="qwen_ner"]');
    if (el && s.qwen_ner_tab_label) el.textContent = s.qwen_ner_tab_label;
  } catch {
    /* offline */
  }
}
function setStageActive(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  pipelineStages.querySelectorAll(".stage").forEach((el) => {
    el.classList.remove("active", "done");
    const i = STAGE_ORDER.indexOf(el.dataset.stage);
    if (i === idx) el.classList.add("active");
    else if (idx >= 0 && i >= 0 && i < idx) el.classList.add("done");
  });
}

function appendPipelineLog(p) {
  const stage = (p.stage || "starting").replace(/_/g, " ");
  const key = [
    p.file,
    p.chunk_index,
    p.total_chunks,
    stage,
    p.language,
    p.gliner_model,
    p.presidio_count,
    p.gliner_count,
    p.agreed_count,
    p.replacements_count,
  ].join("|");
  if (key === lastLogKey) return;
  lastLogKey = key;
  const ts = new Date().toLocaleTimeString("en-GB", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const filePart = p.file ? " File: " + escapeHtml(p.file) + " |" : "";
  const chunkPart =
    p.chunk_index != null && p.total_chunks != null
      ? " Chunk " + p.chunk_index + "/" + p.total_chunks + " |"
      : "";
  const extra = [];
  if (p.language != null) extra.push("Language: " + p.language);
  if (p.gliner_model != null) extra.push("Model: " + p.gliner_model);
  if (p.presidio_count != null) extra.push("Presidio: " + p.presidio_count);
  if (p.gliner_count != null) extra.push("GLiNER: " + p.gliner_count);
  if (p.qwen_count != null) extra.push((p.qwen_label || "Qwen") + ": " + p.qwen_count);
  if (p.agreed_count != null) extra.push("Agreed: " + p.agreed_count);
  if (p.replacements_count != null) extra.push("Replacements: " + p.replacements_count);
  const extraStr = extra.length ? " " + extra.join(", ") : "";
  const line =
    '<div class="log-line"><span class="ts">[' +
    ts +
    ']</span><span class="file">' +
    filePart +
    "</span>" +
    chunkPart +
    ' <span class="stage">Stage: ' +
    escapeHtml(stage) +
    "</span>" +
    escapeHtml(extraStr) +
    "</div>";
  pipelineLog.insertAdjacentHTML("beforeend", line);
  pipelineLog.scrollTop = pipelineLog.scrollHeight;
}

function renderLiveProgress(p) {
  if (!p.running && !p.error && !p.run_id) return;
  if (p.error) {
    liveProgress.innerHTML = '<span class="error">' + escapeHtml(p.error) + "</span>";
    return;
  }
  if (!p.running && p.run_id) {
    liveProgress.innerHTML = "";
    return;
  }
  const stage = p.stage || "starting";
  const stageLabel = stage.replace(/_/g, " ");
  setStageActive(stage);
  appendPipelineLog(p);
  const parts = [];
  if (p.file) parts.push('<span class="file">File: ' + escapeHtml(p.file) + "</span>");
  if (p.chunk_index != null && p.total_chunks != null)
    parts.push('<span class="chunk-info">Chunk ' + p.chunk_index + "/" + p.total_chunks + "</span>");
  if (p.language != null) parts.push("Language: " + escapeHtml(p.language));
  if (p.gliner_model != null) parts.push("GLiNER: " + escapeHtml(p.gliner_model));
  if (p.chunk_size != null) parts.push("(" + p.chunk_size + " chars)");
  if (p.presidio_count != null) parts.push("Presidio: " + p.presidio_count);
  if (p.gliner_count != null) parts.push("GLiNER count: " + p.gliner_count);
  if (p.qwen_count != null) parts.push((p.qwen_label || "Qwen") + ": " + p.qwen_count);
  if (p.agreed_count != null) parts.push("Agreed: " + p.agreed_count);
  if (p.replacements_count != null) parts.push("Replacements: " + p.replacements_count);
  liveProgress.innerHTML =
    "<span>Stage: <strong>" +
    escapeHtml(stageLabel) +
    "</strong></span> " +
    (parts.length ? " — " + parts.join(" · ") : "");
}

async function pollProgress() {
  try {
    const r = await fetch("/api/progress");
    const p = await r.json();
    renderLiveProgress(p);
    if (p.running) {
      setUploadEnabled(false);
      progressPollId = setTimeout(pollProgress, 1000);
    } else {
      progressPollId = null;
      setUploadEnabled(true);
      if (p.error) {
        runStatus.innerHTML =
          '<div class="status error">' +
          escapeHtml(p.error) +
          (p.stderr ? "<pre>" + escapeHtml(p.stderr) + "</pre>" : "") +
          "</div>";
      } else if (p.run_id) {
        runStatus.innerHTML =
          '<div class="status success">Done. Run ID: ' + escapeHtml(p.run_id) + "</div>";
        loadLastRun();
        loadScannedFiles();
      }
      runBtn.disabled = uploadedNow.length === 0;
      document.querySelectorAll(".btn-reanalyse").forEach((b) => {
        b.disabled = false;
      });
    }
  } catch (e) {
    progressPollId = null;
    setUploadEnabled(true);
    runStatus.innerHTML = '<div class="status error">' + escapeHtml(e.message) + "</div>";
    runBtn.disabled = uploadedNow.length === 0;
    document.querySelectorAll(".btn-reanalyse").forEach((b) => {
      b.disabled = false;
    });
  }
}

async function startRun(filesToRun) {
  runBtn.disabled = true;
  setUploadEnabled(false);
  document.querySelectorAll(".btn-reanalyse").forEach((b) => {
    b.disabled = true;
  });
  lastLogKey = "";
  pipelineLog.innerHTML = "";
  runStatus.innerHTML = '<div class="status running">Running pipeline…</div>';
  liveProgress.innerHTML = "Starting…";
  if (progressPollId) clearTimeout(progressPollId);
  try {
    const body = filesToRun && filesToRun.length ? JSON.stringify({ files: filesToRun }) : null;
    const r = await fetch("/api/run", {
      method: "POST",
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body || undefined,
    });
    const data = await r.json();
    if (data.error) {
      setUploadEnabled(true);
      runStatus.innerHTML =
        '<div class="status error">' +
        escapeHtml(data.error) +
        (data.stderr ? "<pre>" + escapeHtml(data.stderr) + "</pre>" : "") +
        "</div>";
      liveProgress.innerHTML = "";
      runBtn.disabled = uploadedNow.length === 0;
      document.querySelectorAll(".btn-reanalyse").forEach((b) => {
        b.disabled = false;
      });
    } else if (data.started) {
      progressPollId = setTimeout(pollProgress, 500);
    } else {
      setUploadEnabled(true);
      runStatus.innerHTML =
        '<div class="status success">Done. Run ID: ' + (data.run_id || "") + "</div>";
      loadLastRun();
      loadScannedFiles();
      runBtn.disabled = uploadedNow.length === 0;
      document.querySelectorAll(".btn-reanalyse").forEach((b) => {
        b.disabled = false;
      });
    }
  } catch (e) {
    setUploadEnabled(true);
    runStatus.innerHTML = '<div class="status error">' + escapeHtml(e.message) + "</div>";
    liveProgress.innerHTML = "";
    runBtn.disabled = uploadedNow.length === 0;
    document.querySelectorAll(".btn-reanalyse").forEach((b) => {
      b.disabled = false;
    });
  }
}

runBtn.onclick = async () => {
  await startRun(uploadedNow);
};

function fmtNum(v) {
  return v != null && v !== "" ? String(v) : "—";
}
function fmtPct(v) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  return (isNaN(n) ? "—" : n.toFixed(1)) + "%";
}

async function loadScannedFiles(showRefreshStatus) {
  const refreshBtn = document.getElementById("refreshScannedBtn");
  if (showRefreshStatus && refreshBtn) {
    refreshBtn.disabled = true;
    refreshBtn.textContent = "Refreshing…";
  }
  try {
    const res = await fetch("/api/scanned-files");
    if (!res.ok) throw new Error(res.statusText || "Request failed");
    const data = await res.json();
    const list = data.scanned_files || [];
    const combined = data.combined || {};
    if (list.length === 0) {
      document.getElementById("combinedResultsBox").style.display = "none";
      noScannedFiles.style.display = "block";
      noScannedFiles.textContent = "No previously scanned files.";
      scannedFilesBody.innerHTML = "";
      return;
    }
    noScannedFiles.style.display = "none";
    document.getElementById("combinedResultsBox").style.display = "block";
    document.getElementById("combinedArmor").textContent = combined.armor_entities ?? 0;
    document.getElementById("combinedLlm").textContent = combined.llm_entities ?? 0;
    document.getElementById("combinedSame").textContent = combined.same ?? 0;
    document.getElementById("combinedDiff").textContent = combined.different_llm ?? 0;
    document.getElementById("combinedRecall").textContent =
      combined.recall_pct != null && combined.recall_pct !== ""
        ? Number(combined.recall_pct).toFixed(1)
        : "—";
    document.getElementById("combinedPrecision").textContent =
      combined.precision_pct != null && combined.precision_pct !== ""
        ? Number(combined.precision_pct).toFixed(1)
        : "—";
    const latestRunId = data.latest_run_id || "";
    scannedFilesBody.innerHTML = list
      .map((f) => {
        const scannedAt = f.last_scanned_at ? new Date(f.last_scanned_at).toLocaleString() : "—";
        const justAnalysed = f.last_run_id && f.last_run_id === latestRunId;
        const fileCell =
          escapeHtml(f.file_name) +
          (justAnalysed
            ? ' <span class="badge-just-analysed" title="In most recent run">Just analysed</span>'
            : "");
        const armorEnt = fmtNum(f.armor_entities);
        let llmEnt = fmtNum(f.llm_entities);
        if (f.llm_error) {
          const errShort =
            escapeHtml((f.llm_error || "").slice(0, 120)) +
            ((f.llm_error || "").length > 120 ? "…" : "");
          llmEnt =
            '<span class="llm-cell" title="' +
            escapeAttr(f.llm_error) +
            '">' +
            llmEnt +
            ' <small class="llm-err">' +
            errShort +
            "</small></span>";
        }
        const same = fmtNum(f.same);
        const diffLlm = fmtNum(f.different_llm);
        const recall = fmtPct(f.recall_pct);
        const precision = fmtPct(f.precision_pct);
        const reportLink =
          '<a href="/report-viewer.html?run_id=' +
          encodeURIComponent(f.last_run_id || "") +
          '" target="_blank" class="btn secondary btn-inline">View report</a>';
        const reevalBtn =
          '<button type="button" class="btn secondary btn-reanalyse btn-inline" data-file="' +
          escapeAttr(f.file_name) +
          '" title="Run full anonymisation on this file (must still be in uploads)">Re-evaluate</button>';
        const delBtn =
          '<button type="button" class="btn danger btn-delete-file btn-inline" data-filename="' +
          escapeAttr(f.file_name) +
          '">Delete</button>';
        return (
          "<tr" +
          (justAnalysed ? ' class="row-just-analysed"' : "") +
          "><td>" +
          fileCell +
          "</td><td>" +
          escapeHtml(f.last_run_id || "—") +
          "</td><td>" +
          escapeHtml(scannedAt) +
          "</td><td>" +
          armorEnt +
          "</td><td>" +
          llmEnt +
          "</td><td>" +
          same +
          "</td><td>" +
          diffLlm +
          "</td><td>" +
          recall +
          "</td><td>" +
          precision +
          "</td><td>" +
          reevalBtn +
          "</td><td>" +
          reportLink +
          "</td><td>" +
          delBtn +
          "</td></tr>"
        );
      })
      .join("");
    scannedFilesBody.querySelectorAll(".btn-delete-file").forEach((btn) => {
      btn.onclick = () => deleteScannedFile(btn.getAttribute("data-filename"));
    });
    scannedFilesBody.querySelectorAll(".btn-reanalyse").forEach((btn) => {
      btn.onclick = () => startRun([btn.getAttribute("data-file")]);
    });
  } catch (e) {
    document.getElementById("combinedResultsBox").style.display = "none";
    noScannedFiles.style.display = "block";
    noScannedFiles.textContent = "Failed to load list: " + (e.message || "Unknown error");
    scannedFilesBody.innerHTML = "";
  } finally {
    if (refreshBtn) {
      refreshBtn.disabled = false;
      refreshBtn.textContent = "Refresh list";
    }
  }
}

async function deleteScannedFile(filename) {
  if (!filename || !confirm('Delete file "' + filename + '" from uploads and remove its LLM analysis?'))
    return;
  try {
    const r = await fetch("/api/scanned-file/" + encodeURIComponent(filename), { method: "DELETE" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      alert(data.error || "Delete failed");
      return;
    }
    await loadScannedFiles();
  } catch (e) {
    alert(e.message || "Delete failed");
  }
}

async function runLlmNer(scope) {
  const pendingBtn = document.getElementById("runNerPendingBtn");
  const allBtn = document.getElementById("runNerAllBtn");
  const statusEl = document.getElementById("llmNerStatus");
  pendingBtn.disabled = true;
  allBtn.disabled = true;
  statusEl.textContent =
    scope === "pending" ? "Running NER for pending files…" : "Running NER for all files…";
  try {
    const r = await fetch("/api/run-llm-ner", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scope: scope }),
    });
    const data = await r.json();
    if (!r.ok) {
      statusEl.textContent = "Error: " + (data.error || r.statusText);
      return;
    }
    statusEl.textContent = "Done.";
    await loadScannedFiles();
  } catch (e) {
    statusEl.textContent = "Error: " + (e.message || "Request failed");
  } finally {
    pendingBtn.disabled = false;
    allBtn.disabled = false;
  }
}

async function loadLastRun() {
  const runs = (await fetch("/api/runs").then((r) => r.json())).runs || [];
  if (runs.length === 0) {
    noRuns.style.display = "block";
    fileListBody.innerHTML = "";
    chunkLogsBody.innerHTML = "";
    return;
  }
  noRuns.style.display = "none";
  const runId = runs[0].run_id;
  const run = await fetch("/api/runs/" + runId).then((r) => r.json());
  fileListBody.innerHTML = (run.files || [])
    .map((f) => {
      const types = (f.entity_types || []).join(", ") || "—";
      const reanalyseBtn =
        '<button type="button" class="btn secondary btn-reanalyse btn-inline" data-file="' +
        escapeAttr(f.file_name) +
        '" title="Run full anonymisation on this file">Re-evaluate</button>';
      return (
        "<tr><td>" +
        escapeHtml(f.file_name) +
        "</td><td>" +
        (f.entity_count ?? 0) +
        '</td><td class="types">' +
        escapeHtml(types) +
        "</td><td>" +
        (f.chunks_processed ?? 0) +
        "</td><td>" +
        reanalyseBtn +
        '</td><td><a href="/report-viewer.html?run_id=' +
        encodeURIComponent(runId) +
        '" target="_blank" class="btn secondary btn-inline">View report</a></td></tr>'
      );
    })
    .join("");
  fileListBody.querySelectorAll(".btn-reanalyse").forEach((btn) => {
    btn.onclick = () => startRun([btn.getAttribute("data-file")]);
  });
  const allLogs = (run.files || []).flatMap((f) =>
    (f.chunk_logs || []).map((c) => ({ file: f.file_name, ...c })),
  );
  chunkLogsBody.innerHTML = allLogs.length
    ? allLogs
        .map((c) => {
          return (
            '<div class="chunk-log"><span class="chunk-idx">Chunk ' +
            c.chunk_index +
            '</span> <span class="size">' +
            (c.chunk_size || 0) +
            " chars</span> — " +
            (c.file ? escapeHtml(c.file) + " — " : "") +
            "Presidio: " +
            (c.presidio_count || 0) +
            ", GLiNER: " +
            (c.gliner_count || 0) +
            ", " +
            escapeHtml(armorQwenProgressLabel) +
            ": " +
            (c.qwen_count || 0) +
            ", Agreed: " +
            (c.agreed_count || 0) +
            ", Replacements: " +
            (c.replacements_count || 0) +
            "</div>"
          );
        })
        .join("")
    : '<p class="no-data">No chunk data.</p>';
}

async function restoreHiddenFiles() {
  const btn = document.getElementById("restoreHiddenBtn");
  if (btn) btn.disabled = true;
  try {
    const r = await fetch("/api/restore-scanned-files", { method: "POST" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      alert(data.error || "Failed");
      return;
    }
    await loadScannedFiles(true);
    const hint = document.getElementById("restoreHiddenHint");
    if (hint) hint.style.display = "none";
  } catch (e) {
    alert(e.message || "Failed");
  } finally {
    if (btn) btn.disabled = false;
  }
}

(async function init() {
  await refreshArmorSettings();
  const refreshScannedBtn = document.getElementById("refreshScannedBtn");
  if (refreshScannedBtn) refreshScannedBtn.onclick = () => loadScannedFiles(true);
  document.getElementById("runNerPendingBtn").onclick = () => runLlmNer("pending");
  document.getElementById("runNerAllBtn").onclick = () => runLlmNer("all");
  const restoreHiddenBtn = document.getElementById("restoreHiddenBtn");
  if (restoreHiddenBtn) restoreHiddenBtn.onclick = restoreHiddenFiles;
  refreshUploadedDisplay();
  try {
    const p = await fetch("/api/progress").then((r) => r.json());
    setUploadEnabled(!p.running);
    if (p.running) progressPollId = setTimeout(pollProgress, 500);
  } catch {
    setUploadEnabled(true);
  }
  await loadLastRun();
  await loadScannedFiles();
})();
