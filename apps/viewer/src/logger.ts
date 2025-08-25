import { MAX_LOG_ENTRIES } from "./constants";
import { el } from "./dom";

type Level = "INFO" | "WARN" | "ERROR";
type LogEntry = {
  timestamp: string;
  level: Level;
  message: string;
  data?: any;
};

const entries: LogEntry[] = [];
let visible = false;

export function setLogsVisible(v: boolean) {
  visible = v;
  el.logs.style.display = visible ? "block" : "none";
  render();
}

export function log(level: Level, message: string, data?: any) {
  const ts = new Date().toLocaleTimeString("fr-FR");
  entries.push({ timestamp: ts, level, message, data });
  if (entries.length > MAX_LOG_ENTRIES) entries.shift();
  if (visible) render();

  if (level !== "INFO") console.log(`[${level}] ${message}`, data ?? "");
}

export function render() {
  const html = entries
    .slice()
    .reverse()
    .map((e) => {
      const d = e.data ? ` - ${JSON.stringify(e.data)}` : "";
      return `<div class="log-entry">
        <span class="log-time">${e.timestamp}</span>
        <span class="log-level ${e.level}">${e.level}</span>
        ${e.message}${d}
      </div>`;
    })
    .join("");
  el.logs.innerHTML = html;
}
