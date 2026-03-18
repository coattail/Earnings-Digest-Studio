# Earnings Digest Studio

本地私用的深度版财报汇总工具。当前样本内置 `NVDA` 与 `AVGO`，支持：

- 选择公司与自然季度
- 生成图文并茂的 14 页深度版报告
- 同时覆盖当季结果、电话会摘要、风险催化剂、近 12 季成长与结构分析
- 网页预览与 PDF 导出共用同一份 HTML 模板
- 可选上传 `PDF / TXT / HTML` transcript 作为电话会补充材料

## 运行方式

```bash
cd /Users/yuwan/Documents/New\ project/earnings-digest-studio
.venv/bin/uvicorn app.main:app --reload
```

打开：

`http://127.0.0.1:8000`

## 测试

```bash
cd /Users/yuwan/Documents/New\ project/earnings-digest-studio
.venv/bin/python -m unittest discover -s tests
```

## 当前数据范围

- `NVDA`：内置最新季度样本，12 季结构页走完整 segment 模式
- `AVGO`：内置最新季度样本，历史结构页按“结构降级”模式展示

## 说明

- 当前版本优先实现“深度版报告体验”与核心 API。
- PDF 导出依赖 Playwright Chromium，首次使用前需要安装浏览器：

```bash
.venv/bin/python -m playwright install chromium
```
