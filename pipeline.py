# -*- coding: utf-8 -*-
"""
pipeline.py
===========
Thin orchestration layer between the Flask app and the scraper/generator.
Owns the error registry and progress emission contract so the UI always
knows exactly what's happening and why something failed.

Error severity levels:
  FATAL   — pipeline cannot continue, user must intervene
  WARNING — pipeline continued with a fallback, result may differ
  INFO    — informational, no action needed

Progress events emitted via the `emit_fn` callback:
  { "step": str, "status": "running"|"done"|"warning"|"error",
    "message": str, "pct": int }
"""

import os
import sys
import traceback
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Optional


# ── Error registry ─────────────────────────────────────────────────

@dataclass
class PipelineError:
    severity:  str        # FATAL | WARNING | INFO
    step:      str        # which pipeline step raised this
    code:      str        # short machine-readable code e.g. "SERIAL_DOWNLOAD_TIMEOUT"
    message:   str        # human-readable description
    detail:    str = ""   # technical detail / traceback fragment
    input_val: str = ""   # what input was being processed when it failed

    def to_dict(self) -> dict:
        return {
            "severity":  self.severity,
            "step":      self.step,
            "code":      self.code,
            "message":   self.message,
            "detail":    self.detail,
            "input_val": self.input_val,
        }


class ErrorRegistry:
    """
    Accumulates pipeline errors for display and copy-to-clipboard reporting.
    Fatal errors are also re-raised to halt the pipeline.
    """

    def __init__(self):
        self._errors: list[PipelineError] = []

    def add(self, error: PipelineError) -> None:
        self._errors.append(error)

    def fatal(self, step: str, code: str, message: str,
              detail: str = "", input_val: str = "") -> None:
        """Record a fatal error and raise to halt pipeline."""
        err = PipelineError("FATAL", step, code, message, detail, input_val)
        self._errors.append(err)
        raise PipelineAbort(err)

    def warn(self, step: str, code: str, message: str,
             detail: str = "", input_val: str = "") -> None:
        """Record a warning — pipeline continues."""
        self._errors.append(
            PipelineError("WARNING", step, code, message, detail, input_val)
        )

    def info(self, step: str, code: str, message: str) -> None:
        """Record an informational event."""
        self._errors.append(
            PipelineError("INFO", step, code, message)
        )

    def has_fatal(self) -> bool:
        return any(e.severity == "FATAL" for e in self._errors)

    def has_warnings(self) -> bool:
        return any(e.severity == "WARNING" for e in self._errors)

    def all_errors(self) -> list[dict]:
        return [e.to_dict() for e in self._errors]

    def report_text(self) -> str:
        """Returns a plain-text error report for copy-to-clipboard."""
        lines = [f"DeliveryReportPro Error Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                 "=" * 60]
        for e in self._errors:
            lines.append(f"[{e.severity}] {e.step} / {e.code}")
            lines.append(f"  {e.message}")
            if e.detail:
                lines.append(f"  Detail: {e.detail}")
            if e.input_val:
                lines.append(f"  Input:  {e.input_val}")
        return "\n".join(lines)


class PipelineAbort(Exception):
    """Raised by ErrorRegistry.fatal() to halt pipeline execution."""
    def __init__(self, error: PipelineError):
        self.error = error
        super().__init__(error.message)


# ── Progress emitter ───────────────────────────────────────────────

class ProgressEmitter:
    """
    Wraps the SocketIO emit function with step context.
    All pipeline steps call this instead of print().
    """

    STEPS = [
        ("login",    "Signing in to HomeSource",      5),
        ("bulk",     "Downloading batch invoice",     20),
        ("serial",   "Downloading serial inventory",  45),
        ("orders",   "Downloading orders detail",     60),
        ("route",    "Downloading route sheet",       70),
        ("monday",   "Fetching crate status",         80),
        ("charges",  "Calculating delivery costs",    88),
        ("build",    "Building report rows",          93),
        ("excel",    "Writing Excel report",          97),
        ("done",     "Report ready",                 100),
    ]

    def __init__(self, emit_fn: Callable):
        self._emit = emit_fn
        self._step_pct = {s[0]: s[2] for s in self.STEPS}
        self._step_label = {s[0]: s[1] for s in self.STEPS}

    def running(self, step: str, message: str = "") -> None:
        pct = self._step_pct.get(step, 0)
        self._emit("progress", {
            "step":    step,
            "label":   self._step_label.get(step, step),
            "status":  "running",
            "message": message or self._step_label.get(step, step),
            "pct":     pct,
        })

    def done(self, step: str, message: str = "") -> None:
        pct = self._step_pct.get(step, 0)
        self._emit("progress", {
            "step":    step,
            "label":   self._step_label.get(step, step),
            "status":  "done",
            "message": message or "Complete",
            "pct":     pct,
        })

    def warn(self, step: str, message: str) -> None:
        self._emit("progress", {
            "step":    step,
            "label":   self._step_label.get(step, step),
            "status":  "warning",
            "message": message,
            "pct":     self._step_pct.get(step, 0),
        })

    def error(self, step: str, message: str) -> None:
        self._emit("progress", {
            "step":    step,
            "label":   self._step_label.get(step, step),
            "status":  "error",
            "message": message,
            "pct":     self._step_pct.get(step, 0),
        })

    def prompt(self, prompt_id: str, context: dict) -> None:
        """Ask the UI to display an interactive prompt card."""
        self._emit("prompt", {"id": prompt_id, "context": context})

    def log(self, message: str, level: str = "info") -> None:
        """Send a raw log line to the live feed."""
        self._emit("log", {"message": message, "level": level})

    def map_data(self, route_data: dict) -> None:
        """Send route data to the map panel."""
        self._emit("map_data", route_data)

    def result(self, data: dict) -> None:
        """Send final report summary to the UI."""
        self._emit("result", data)


# ── Pipeline runner ────────────────────────────────────────────────

def run_pipeline(config: dict, delivery_date: datetime,
                 emit_fn: Callable,
                 diesel_price: float = 3.80,
                 prompt_answers: dict = None) -> dict:
    """
    Full pipeline: scrape → generate → emit result.

    Args:
        config:         decrypted config dict (HS creds, API keys, paths)
        delivery_date:  target delivery date
        emit_fn:        SocketIO emit function for progress/log/prompt events
        prompt_answers: dict of pre-answered prompts {prompt_id: value}
                        populated by UI responses during runtime

    Returns:
        result dict with keys: filepath, fin_rows, errors, warnings
    """
    errors  = ErrorRegistry()
    emitter = ProgressEmitter(emit_fn)
    answers = prompt_answers if prompt_answers is not None else {}

    # Inject config into environment for scraper/generator modules
    os.environ["HS_USERNAME"]       = config.get("HS_USERNAME", "")
    os.environ["HS_PASSWORD"]       = config.get("HS_PASSWORD", "")
    os.environ["HS_BASE_URL"]       = config.get("HS_BASE_URL", "")
    os.environ["GOOGLE_API_KEY"]    = config.get("GOOGLE_API_KEY", "")
    os.environ["MONDAY_API_TOKEN"]  = config.get("MONDAY_API_TOKEN", "")
    os.environ["ORS_API_KEY"]       = config.get("ORS_API_KEY", "")

    result = {
        "filepath":   None,
        "fin_rows":   [],
        "errors":     [],
        "warnings":   [],
        "report_text": "",
    }

    try:
        # ── Scrape or use cached files ────────────────────────────
        import os as _os
        INBOX = _os.path.join(
            _os.environ.get("APPDATA", _os.path.expanduser("~")),
            "DeliveryReportPro", "scrape_inbox"
        )

        def _find_inbox(pattern):
            """Find the most recent file in inbox matching a pattern."""
            import glob
            matches = sorted(glob.glob(_os.path.join(INBOX, f"*{pattern}*")),
                           key=_os.path.getmtime, reverse=True)
            return matches[0] if matches else None

        use_cache = config.get("USE_CACHE", False)
        cached_bulk   = _find_inbox("bulk-invoice")
        cached_serial = _find_inbox("serial-number-inventory")
        cached_orders = _find_inbox("orders-detail")
        cached_route  = _find_inbox("route_sheet")

        has_cache = all([cached_bulk, cached_serial, cached_orders])

        if use_cache and has_cache:
            emitter.log("Using cached export files — skipping scrape", "warn")
            emitter.done("login",  "Skipped — using cached data")
            emitter.done("bulk",   f"Cached: {_os.path.basename(cached_bulk)}")
            emitter.done("serial", f"Cached: {_os.path.basename(cached_serial)}")
            emitter.done("orders", f"Cached: {_os.path.basename(cached_orders)}")
            if cached_route:
                emitter.done("route", f"Cached: {_os.path.basename(cached_route)}")
            else:
                emitter.warn("route", "No cached route sheet found")
            scrape_results = {
                "delivery_date": delivery_date,
                "bulk_invoice":  cached_bulk,
                "serial":        cached_serial,
                "orders_detail": cached_orders,
                "route_sheet":   cached_route,
            }
        else:
            from financial_scraper_app import run as scrape
            scrape_results = scrape(
                delivery_date=delivery_date,
                emitter=emitter,
                errors=errors,
            )

        missing_critical = [
            k for k in ["bulk_invoice", "serial", "orders_detail"]
            if not scrape_results.get(k)
        ]
        if missing_critical:
            errors.fatal(
                step="scrape",
                code="MISSING_CRITICAL_FILES",
                message=f"Critical export files missing: {', '.join(missing_critical)}. "
                        "Check HomeSource connection and try again.",
            )

        if not scrape_results.get("route_sheet"):
            errors.warn(
                step="scrape",
                code="MISSING_ROUTE_SHEET",
                message="Route sheet not downloaded — stops will sort by truck then order number.",
            )
            emitter.warn("route", "Route sheet missing — continuing without stop order")

        # ── Generate ──────────────────────────────────────────────
        from financial_generator_app import run as generate
        filepath, fin_rows = generate(
            bulk_path        = scrape_results["bulk_invoice"],
            serial_path      = scrape_results["serial"],
            orders_path      = scrape_results["orders_detail"],
            route_sheet_path = scrape_results.get("route_sheet"),
            delivery_date    = delivery_date,
            diesel_price     = diesel_price,
            emitter          = emitter,
            errors           = errors,
            prompt_answers   = answers,
        )

        result["filepath"]  = filepath
        result["fin_rows"]  = fin_rows

    except PipelineAbort as e:
        emitter.error(e.error.step, e.error.message)
        result["errors"] = errors.all_errors()
        result["report_text"] = errors.report_text()
        return result

    except Exception as e:
        tb = traceback.format_exc()
        errors.add(PipelineError(
            severity  = "FATAL",
            step      = "unknown",
            code      = "UNEXPECTED_ERROR",
            message   = f"Unexpected error: {type(e).__name__}: {e}",
            detail    = tb[-800:],  # last 800 chars of traceback
        ))
        emitter.error("unknown", f"Unexpected error: {e}")

    finally:
        # Always zero credentials from environment after run
        for key in ["HS_USERNAME", "HS_PASSWORD", "HS_BASE_URL", "GOOGLE_API_KEY",
                    "MONDAY_API_TOKEN", "ORS_API_KEY"]:
            os.environ.pop(key, None)

    result["errors"]      = errors.all_errors()
    result["warnings"]    = [e for e in errors.all_errors() if e["severity"] == "WARNING"]
    result["report_text"] = errors.report_text()
    return result
