import { el } from "./dom";
import { setLogsVisible, log } from "./logger";
import { connectSSE, disconnectSSE, isSSEEnabled } from "./sse";
import { refreshAll } from "./api";
import { resetForPairChange } from "./state";
import { resizeChart } from "./chart";

log("INFO", "Crypto Viewer initialized");

refreshAll().then(() => {
  if (isSSEEnabled()) connectSSE();
});

el.refreshBtn.addEventListener("click", refreshAll);

let sseOn = true;
el.toggleSSEBtn.addEventListener("click", () => {
  sseOn = !sseOn;
  if (sseOn) {
    el.toggleSSEBtn.textContent = "● Live ON";
    el.toggleSSEBtn.className = "success";
    connectSSE();
  } else {
    el.toggleSSEBtn.textContent = "○ Live OFF";
    el.toggleSSEBtn.className = "danger";
    disconnectSSE();
  }
});

let logsVisible = false;
el.toggleLogsBtn.addEventListener("click", () => {
  logsVisible = !logsVisible;
  setLogsVisible(logsVisible);
  el.toggleLogsBtn.textContent = logsVisible ? "Hide Logs" : "Show Logs";
});

function onPairParamChange(reconnect = true) {
  resetForPairChange();
  refreshAll();
  if (sseOn && reconnect) connectSSE();
}

el.coin.addEventListener("change", () => onPairParamChange(true));
el.currency.addEventListener("change", () => onPairParamChange(true));
el.win.addEventListener("change", () => onPairParamChange(true));
el.range.addEventListener("change", () => refreshAll());

window.addEventListener("resize", () => resizeChart());
