import { escapeHtml } from "../shared/html-utils.js";
import { typeColor, highlightText } from "../shared/report-core.js";

function renderFile(data, fileIndex) {
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
}

async function load() {
  try {
    const res = await fetch("/report.json");
    if (!res.ok) throw new Error(res.statusText || "Failed to load report");
    const data = await res.json();
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
    renderFile(data, 0);
  } catch (e) {
    const el = document.getElementById("originalText");
    el.className = "text-view error";
    el.textContent =
      "Error: " +
      e.message +
      ". Run the NER pipeline first (without --no-report), then start the UI.";
  }
}

load();
