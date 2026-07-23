# minibroker — DeepSeek Audit

**Date:** 2026-07-13
**Path:** `C:\Users\TATI\Desktop\DEV\minibroker\`
**Stack:** Python / FastAPI + aiosqlite + aiofiles
**Tier:** 3 — Medium
**Dependencies:** Partial (`__pycache__` only)

---

## 🔴 Security Vulnerabilities

| Severity | File | Line(s) | Vulnerability | Exact Fix |
|----------|------|---------|---------------|-----------|
| 🟡 MEDIUM | CORS middleware | — | `allow_credentials=True` — cookies/auth headers sent cross-origin. If `allow_origins` is not locked down, this is a credential theft vector. | Verify `allow_origins` is an explicit list, not `["*"]`. Set to specific frontend URL. With `allow_credentials=True`, `allow_origins` cannot be `"*"` per CORS spec. |
| 🟡 MEDIUM | — | — | No auth layer — message broker with no authentication. Acceptable for internal service but should be documented. | If exposed externally, add API key auth. |
| ✅ | `core/offsets.py` | — | All queries use `?` placeholders with parameter tuples — no SQL injection. Good. | — |

---

## 🟠 Performance Issues

| Severity | File | Line(s) | Issue | Exact Fix |
|----------|------|---------|-------|-----------|
| 🟡 MEDIUM | SQLite usage | — | Uses `aiosqlite` for async — good choice. No blocking I/O. | — |

---

## 🔧 Session: 2026-07-14 — Multi-Agent Deep Audit Sweep (Round 1)

**Status:** Not audited in this round. Previously noted (July 5): 17/17 tests passing, clean layered design. Sweep Round 2 will cover Tier 3.

| Category | Package | Issue | Fix |
|----------|---------|-------|-----|
| 🔴 CRITICAL | ALL 8 packages | `requirements.txt` has **ZERO version pins** — every `pip install` resolves to latest. Includes: `fastapi`, `uvicorn`, `pydantic`, `aiofiles`, `aiosqlite`, `pytest`, `pytest-asyncio`, `httpx`. | Pin all packages with `==` exact versions. |
| 🟡 MEDIUM | `aiofiles` | Async file I/O — appropriate for message broker disk persistence. | Pin version. |
| 🟡 MEDIUM | `aiosqlite` | Async SQLite — good for FastAPI. | Pin version. |

### Missing Dev Tooling
- No `.python-version`
- No `pytest-cov`
- No `requirements-lock.txt`

---

## 📋 Priority Fix Queue

1. **[CRITICAL — Unpinned Deps]** `requirements.txt` — Pin all 8 packages with exact `==` versions.
2. **[MEDIUM — CORS]** Verify `allow_origins` is not `["*"]` when `allow_credentials=True`.
3. **[MEDIUM — Dev Tooling]** Add `.python-version`, `pytest-cov`, `requirements-lock.txt`.
4. **[LOW — Auth]** Document that minibroker is for internal use. If external, add API key auth.
