function setStatus(message) {
  const node = document.querySelector("#form-status");
  if (node) node.textContent = message;
}

const generationProgressState = {
  current: 0,
  target: 0,
  rafId: null,
};

function renderGenerationProgress(progress) {
  const fill = document.querySelector("#generation-progress-fill");
  const percent = document.querySelector("#generation-progress-percent");
  if (!fill || !percent) return;
  const normalized = Math.max(0, Math.min(1, Number(progress || 0)));
  fill.style.width = `${Math.round(normalized * 100)}%`;
  percent.textContent = `${Math.round(normalized * 100)}%`;
}

function animateGenerationProgress() {
  const diff = generationProgressState.target - generationProgressState.current;
  if (Math.abs(diff) < 0.002) {
    generationProgressState.current = generationProgressState.target;
    renderGenerationProgress(generationProgressState.current);
    generationProgressState.rafId = null;
    return;
  }
  generationProgressState.current += diff * 0.16;
  renderGenerationProgress(generationProgressState.current);
  generationProgressState.rafId = window.requestAnimationFrame(animateGenerationProgress);
}

function setExportStatus(message) {
  const node = document.querySelector("#export-status");
  if (node) node.textContent = message;
}

function setGenerationProgress({ visible = true, progress = 0, stage = "准备中", message = "" } = {}) {
  const shell = document.querySelector("#generation-progress");
  const fill = document.querySelector("#generation-progress-fill");
  const percent = document.querySelector("#generation-progress-percent");
  const stageNode = document.querySelector("#generation-progress-stage");
  const messageNode = document.querySelector("#generation-progress-message");
  if (!shell || !fill || !percent || !stageNode || !messageNode) return;
  shell.hidden = !visible;
  if (!visible) {
    if (generationProgressState.rafId) {
      window.cancelAnimationFrame(generationProgressState.rafId);
      generationProgressState.rafId = null;
    }
    generationProgressState.current = 0;
    generationProgressState.target = 0;
    renderGenerationProgress(0);
    return;
  }
  const normalized = Math.max(0, Math.min(1, Number(progress || 0)));
  stageNode.textContent = stage || "处理中";
  messageNode.textContent = message || "系统正在准备报告内容...";
  if (normalized < generationProgressState.current - 0.02) {
    generationProgressState.current = normalized;
    renderGenerationProgress(normalized);
  }
  generationProgressState.target = normalized;
  if (!generationProgressState.rafId) {
    generationProgressState.rafId = window.requestAnimationFrame(animateGenerationProgress);
  }
}

function resetGenerationProgress() {
  setGenerationProgress({ visible: false, progress: 0, stage: "准备中", message: "" });
}

function formatJobStage(stage) {
  const labels = {
    queued: "排队中",
    prepare: "准备数据",
    history: "构建历史趋势",
    sources: "定位官方源",
    materials: "读取官方材料",
    parse: "解析官方原文",
    normalize: "校验解析结果",
    views: "整理机构观点",
    visuals: "排版图表",
    assemble: "封装报告",
    completed: "生成完成",
    failed: "生成失败",
  };
  return labels[stage] || "处理中";
}

async function pollReportJob(jobId) {
  while (true) {
    const response = await fetch(`/report-jobs/${jobId}`);
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || "报告任务状态读取失败。");
    }
    const job = await response.json();
    setGenerationProgress({
      visible: true,
      progress: job.progress,
      stage: formatJobStage(job.stage),
      message: job.message,
    });
    if (job.status === "completed") {
      return job;
    }
    if (job.status === "failed") {
      throw new Error(job.error || job.message || "报告生成失败。");
    }
    await new Promise((resolve) => window.setTimeout(resolve, 700));
  }
}

function compareQuarterDesc(left, right) {
  const leftYear = Number(String(left).slice(0, 4));
  const rightYear = Number(String(right).slice(0, 4));
  const leftQuarter = Number(String(left).slice(-1));
  const rightQuarter = Number(String(right).slice(-1));
  if (leftYear !== rightYear) return rightYear - leftYear;
  return rightQuarter - leftQuarter;
}

function buildQuarterOptions(select, quarters, preferredValue = "") {
  if (!select) return;
  select.innerHTML = "";
  if (!quarters.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "请先选择公司";
    option.selected = true;
    select.appendChild(option);
    select.setAttribute("disabled", "disabled");
    return;
  }
  select.removeAttribute("disabled");
  const sorted = [...quarters].sort(compareQuarterDesc);
  sorted.forEach((quarter) => {
    const option = document.createElement("option");
    option.value = quarter;
    option.textContent = quarter;
    if (preferredValue && preferredValue === quarter) {
      option.selected = true;
    }
    select.appendChild(option);
  });
  if (!preferredValue && select.options.length) {
    select.options[0].selected = true;
  }
}

function bootCompanyCards() {
  const cards = [...document.querySelectorAll("[data-company-card]")];
  const companyInput = document.querySelector("#company-id");
  const quarterSelect = document.querySelector("#calendar-quarter");
  const historyWindowInput = document.querySelector("#history-window");
  const generateButton = document.querySelector("#generate-button");
  const forceRefreshInput = document.querySelector("#force-refresh");
  if (!cards.length || !companyInput || !quarterSelect || !historyWindowInput) return;

  let lastRequestId = 0;

  const fallbackQuarters = (card) => JSON.parse(card.dataset.quarters || "[]");

  const loadQuarterOptions = async (card, preferredValue = "") => {
    const requestId = ++lastRequestId;
    const historyWindow = Number(historyWindowInput.value || 12);
    quarterSelect.innerHTML = '<option value="">正在加载季度...</option>';
    quarterSelect.setAttribute("disabled", "disabled");
    generateButton?.setAttribute("disabled", "disabled");
    setStatus("正在按当前历史窗口加载可选季度...");
    try {
      const response = await fetch(`/companies/${card.dataset.companyId}/quarters?history_window=${historyWindow}`);
      if (!response.ok) throw new Error("季度列表加载失败。");
      const payload = await response.json();
      if (requestId !== lastRequestId) return;
      const quarters = payload.supported_quarters || [];
      buildQuarterOptions(quarterSelect, quarters, preferredValue);
      if (!quarters.length) {
        setStatus(`当前公司在 ${historyWindow} 季窗口下暂无可选季度。`);
        return;
      }
      generateButton?.removeAttribute("disabled");
      setStatus(forceRefreshInput?.checked ? "公司已选中；本次将强制刷新官方源后生成报告。" : "公司已选中；默认会智能复用近期缓存并按需补抓官方资料。");
    } catch (error) {
      if (requestId !== lastRequestId) return;
      const quarters = fallbackQuarters(card);
      buildQuarterOptions(quarterSelect, quarters, preferredValue);
      if (quarters.length) {
        generateButton?.removeAttribute("disabled");
        setStatus("已回退到本地季度列表，可以继续生成报告。");
      } else {
        setStatus(error.message || "季度列表加载失败。");
      }
    }
  };

  const clearSelection = () => {
    cards.forEach((item) => {
      item.classList.remove("active");
      item.setAttribute("aria-pressed", "false");
    });
    companyInput.value = "";
    buildQuarterOptions(quarterSelect, []);
    generateButton?.setAttribute("disabled", "disabled");
    setStatus("请先点选一家公司；再次点击同一张卡片可取消选择。默认会智能复用近期缓存。");
  };

  const applySelection = async (card) => {
    const alreadySelected = card.classList.contains("active");
    if (alreadySelected) {
      clearSelection();
      return;
    }
    cards.forEach((item) => {
      const selected = item === card;
      item.classList.toggle("active", selected);
      item.setAttribute("aria-pressed", selected ? "true" : "false");
    });
    companyInput.value = card.dataset.companyId || "";
    await loadQuarterOptions(card);
  };

  cards.forEach((card) => {
    card.addEventListener("click", () => applySelection(card));
  });
  historyWindowInput.addEventListener("change", async () => {
    const activeCard = document.querySelector("[data-company-card].active");
    if (!activeCard) return;
    await loadQuarterOptions(activeCard, quarterSelect.value);
  });
  forceRefreshInput?.addEventListener("change", () => {
    const activeCard = document.querySelector("[data-company-card].active");
    if (!activeCard) {
      setStatus(forceRefreshInput.checked ? "已开启强制刷新；选中公司后会重新抓取官方源。" : "请先点选一家公司；默认会智能复用近期缓存。");
      return;
    }
    setStatus(forceRefreshInput.checked ? "本次将强制刷新官方源并重新抓取原文材料。" : "本次会优先复用近期缓存，必要时自动补抓官方资料。");
  });
  clearSelection();
}

async function maybeUploadTranscript(fileInput) {
  const file = fileInput?.files?.[0];
  if (!file) return null;
  const body = new FormData();
  body.append("file", file);
  const response = await fetch("/uploads", { method: "POST", body });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || "Transcript upload failed.");
  }
  const payload = await response.json();
  return payload.upload_id;
}

async function generateReport(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const submitButton = document.querySelector("#generate-button");
  const fileInput = document.querySelector("#transcript-file");
  const forceRefreshInput = document.querySelector("#force-refresh");
  submitButton?.setAttribute("disabled", "disabled");
  setStatus(forceRefreshInput?.checked ? "报告任务已提交，正在强制刷新官方源并重新排版..." : "报告任务已提交，正在按需读取缓存并补抓官方资料...");
  setGenerationProgress({
    visible: true,
    progress: 0.03,
    stage: "排队中",
    message: "系统正在创建后台生成任务...",
  });

  try {
    const uploadId = await maybeUploadTranscript(fileInput);
    const payload = {
      company_id: form.querySelector("#company-id").value,
      calendar_quarter: form.querySelector("#calendar-quarter").value,
      history_window: Number(form.querySelector("#history-window").value || 12),
      manual_transcript_upload_id: uploadId,
      force_refresh: Boolean(forceRefreshInput?.checked),
    };
    if (!payload.company_id) {
      throw new Error("请先选择一家公司。");
    }
    if (!payload.calendar_quarter) {
      throw new Error("请先选择自然季度。");
    }
    const response = await fetch("/report-jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || "Report generation failed.");
    }
    const job = await response.json();
    setGenerationProgress({
      visible: true,
      progress: job.progress,
      stage: formatJobStage(job.stage),
      message: job.message,
    });
    const doneJob = job.status === "completed" ? job : await pollReportJob(job.job_id);
    setStatus("报告已生成，正在打开预览页...");
    setGenerationProgress({
      visible: true,
      progress: 1,
      stage: "生成完成",
      message: "预览页即将打开，之后可以继续导出 PDF。",
    });
    const previewUrl = new URL(doneJob.preview_url, window.location.origin);
    previewUrl.searchParams.set("refresh", String(Date.now()));
    window.location.href = previewUrl.toString();
  } catch (error) {
    setStatus(error.message || "生成失败。");
    resetGenerationProgress();
    submitButton?.removeAttribute("disabled");
  }
}

async function exportReport(button) {
  const reportId = button.dataset.reportId;
  if (!reportId) return;
  button.setAttribute("disabled", "disabled");
  setExportStatus("正在导出 PDF...");
  try {
    const response = await fetch(`/reports/${reportId}/export.pdf`, { method: "POST" });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || "PDF export failed.");
    }
    const payload = await response.json();
    setExportStatus("PDF 已生成，正在打开下载链接。");
    window.location.href = payload.download_url;
  } catch (error) {
    setExportStatus(error.message || "PDF 导出失败。");
    button.removeAttribute("disabled");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  bootCompanyCards();
  resetGenerationProgress();
  document.querySelector("#report-form")?.addEventListener("submit", generateReport);
  const exportButton = document.querySelector("[data-export-button]");
  if (exportButton) {
    exportButton.addEventListener("click", () => exportReport(exportButton));
  }
});
