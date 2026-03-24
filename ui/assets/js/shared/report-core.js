import { escapeHtml } from "./html-utils.js";

export const TYPE_COLORS = {
  person: "#f59e0b",
  name: "#f59e0b",
  organization: "#8b5cf6",
  "phone number": "#06b6d4",
  email: "#10b981",
  address: "#6366f1",
  date: "#ec4899",
  location: "#14b8a6",
  suspicious_token: "#64748b",
  ssn: "#ef4444",
  "credit card number": "#ef4444",
  "bank account number": "#ef4444",
  "passport number": "#ef4444",
  "ip address": "#f97316",
  username: "#a855f7",
};

export function typeColor(t) {
  const k = (t || "").toLowerCase();
  return TYPE_COLORS[k] || "#64748b";
}

export function highlightText(text, replacements, side) {
  if (!replacements || replacements.length === 0) return escapeHtml(text);
  const key = side === "original" ? "original_value" : "anonymized_value";
  const sorted = [...replacements].sort((a, b) => {
    const posA = text.indexOf(a[key]);
    const posB = text.indexOf(b[key]);
    return posA - posB;
  });
  let out = "";
  let last = 0;
  for (const r of sorted) {
    const val = r[key];
    const pos = text.indexOf(val, last);
    if (pos === -1) continue;
    out += escapeHtml(text.slice(last, pos));
    const color = typeColor(r.pii_type);
    out +=
      "<mark style=\"background:" +
      color +
      "33;color:" +
      color +
      "\" data-type=\"" +
      escapeHtml(r.pii_type) +
      "\">" +
      escapeHtml(val) +
      "</mark>";
    last = pos + val.length;
  }
  out += escapeHtml(text.slice(last));
  return out;
}
