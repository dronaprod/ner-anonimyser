import { escapeHtml } from "../shared/html-utils.js";
import { typeColor, highlightText } from "../shared/report-core.js";

async function renderFile(data, fileIndex) {
  const file = data.files[fileIndex];
  if (!file) return;
  const originalEl = document.getElementById("originalText");
  const anonymizedEl = document.getElementById("anonymizedText");
  originalEl.innerHTML = highlightText(file.original_text, file.all_replacements, "original");
  anonymizedEl.innerHTML = highlightText(file.anonymized_text, file.all_replacements, "anonymized");
  const findingsBody = document.getElementById("findingsBody");
  findingsBody.innerHTML = (file.all_findings || [])
    .map((f) => {
      const foundBy = (f.found_by || []).join(", ");
      const color = typeColor(f.pii_type);
      return (
        "<tr><td>" +
        escapeHtml(f.value) +
        '</td><td><span class="type-badge" style="background:' +
        color +
        "33;color:" +
        color +
        '">' +
        escapeHtml(f.pii_type) +
        "</span></td><td>" +
        (f.score != null ? f.score : "") +
        '</td><td class="found-by">' +
        escapeHtml(foundBy) +
        "</td></tr>"
      );
    })
    .join("");
  const llmBody = document.getElementById("llmFindingsBody");
  const llmNoData = document.getElementById("llmNoData");
  try {
    const llmRes = await fetch("/api/file-llm-entities/" + encodeURIComponent(file.file_name || ""));
    const llmData = await llmRes.json();
    const list = llmData.llm_entities_list || [];
    if (list.length === 0) {
      llmBody.innerHTML = "";
      llmNoData.hidden = false;
    } else {
      llmNoData.hidden = true;
      llmBody.innerHTML = list
        .map((e) => {
          const color = typeColor(e.label);
          return (
            "<tr><td>" +
            escapeHtml(e.text || "") +
            '</td><td><span class="type-badge" style="background:' +
            color +
            "33;color:" +
            color +
            '">' +
            escapeHtml(e.label || "") +
            "</span></td></tr>"
          );
        })
        .join("");
    }
  } catch {
    llmBody.innerHTML = "";
    llmNoData.hidden = false;
  }
  const replacementsBody = document.getElementById("replacementsBody");
  replacementsBody.innerHTML = (file.all_replacements || [])
    .map((r) => {
      const color = typeColor(r.pii_type);
      return (
        "<tr><td>" +
        escapeHtml(r.original_value) +
        "</td><td>" +
        escapeHtml(r.anonymized_value) +
        '</td><td><span class="type-badge" style="background:' +
        color +
        "33;color:" +
        color +
        '">' +
        escapeHtml(r.pii_type) +
        "</span></td></tr>"
      );
    })
    .join("");
  const dropped = file.all_dropped_findings || [];
  const droppedBody = document.getElementById("droppedBody");
  const noDropped = document.getElementById("noDropped");
  if (dropped.length === 0) {
    droppedBody.innerHTML = "";
    noDropped.hidden = false;
  } else {
    noDropped.hidden = true;
    droppedBody.innerHTML = dropped
      .map((d) => {
        const foundBy = (d.found_by || []).join(", ");
        const color = typeColor(d.pii_type);
        return (
          "<tr><td>" +
          escapeHtml(d.value) +
          '</td><td><span class="type-badge" style="background:' +
          color +
          "33;color:" +
          color +
          '">' +
          escapeHtml(d.pii_type || "") +
          "</span></td><td>" +
          escapeHtml(foundBy) +
          '</td><td class="reason">' +
          escapeHtml(d.reason || "") +
          "</td></tr>"
        );
      })
      .join("");
  }
}

async function load() {
  const runId = new URLSearchParams(window.location.search).get("run_id");
  const reportUrl = runId ? "/report.json?run_id=" + encodeURIComponent(runId) : "/report.json";
  try {
    const res = await fetch(reportUrl);
    if (!res.ok) throw new Error(res.statusText || "Failed to load report");
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    document.getElementById("meta").textContent =
      data.created_at + " — " + (data.source_files || []).join(", ");
    document.getElementById("summary").innerHTML =
      '<div class="card"><span class="num">' +
      (data.total_chunks_anonymized ?? 0) +
      '</span><span class="label">Chunks anonymised</span></div>' +
      '<div class="card"><span class="num">' +
      (data.total_chunks_not_anonymized ?? 0) +
      '</span><span class="label">Chunks unchanged</span></div>' +
      '<div class="card"><span class="num">' +
      (data.files?.length ?? 0) +
      '</span><span class="label">Files</span></div>';
    const select = document.getElementById("fileSelect");
    if (data.files && data.files.length > 1) {
      select.hidden = false;
      select.innerHTML = data.files
        .map((f, i) => '<option value="' + i + '">' + escapeHtml(f.file_name) + "</option>")
        .join("");
      select.onchange = () => renderFile(data, parseInt(select.value, 10));
    }
    await renderFile(data, 0);
  } catch (e) {
    const el = document.getElementById("originalText");
    el.className = "text-view error";
    el.textContent =
      "Error: " + e.message + ". Select a run from ARMOR or run the pipeline first.";
  }
}

load();
