export function escapeHtml(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

export function escapeAttr(s) {
  if (s == null) return "";
  return escapeHtml(s).replace(/"/g, "&quot;");
}
