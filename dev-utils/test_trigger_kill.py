#!/usr/bin/env python3
"""Local test for MarketSweepApp trigger / kill / status methods.

Mocks the underlying AdkApp so no GCP credentials or deployed engine are needed.
Run:  python3 test_trigger_kill.py
"""
import threading
import time
import sys


# ── Fake AdkApp that simulates a long-running sweep ──
class FakeAdkApp:
    """stream_query yields events slowly, simulating a real multi-sector sweep."""

    def stream_query(self, **kwargs):
        user_id = kwargs.get("user_id", "test")
        print(f"  [FakeAdkApp] stream_query started (user={user_id})", flush=True)
        for i in range(30):                    # 30 events, 1 per second = ~30s sweep
            time.sleep(1)
            yield {"event": i, "sector": f"sector_{i}"}
        print(f"  [FakeAdkApp] stream_query finished all events", flush=True)

    def query(self, **kwargs):
        return list(self.stream_query(**kwargs))


# ── Rebuild MarketSweepApp in isolation (no GCP imports) ──
class MarketSweepApp:
    """Copy of the wrapper from market_team.py — standalone for testing."""

    def __init__(self, app):
        self._app = app
        self._sweep_thread = None
        self._stop_event = None
        self._sweep_started_at = None
        self._lock = None

    def set_up(self):
        """Mirrors Vertex AI post-deserialization init. Must be called before use."""
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def trigger(self, **kwargs):
        from datetime import datetime

        with self._lock:
            if self._sweep_thread and self._sweep_thread.is_alive():
                return {"status": "already_running",
                        "message": f"Sweep already in progress since {self._sweep_started_at}. "
                                   f"Call 'kill' first to stop it."}
            self._stop_event.clear()

        input_data = kwargs.get("input", {})
        user_id = input_data.get("user_id", "scheduler_async")
        message = input_data.get("message", "Execute the daily market sweep.")

        def _run():
            try:
                print(f"\n[TRIGGER] Background sweep started for user={user_id}", flush=True)
                for event in self._app.stream_query(user_id=user_id, message=message):
                    if self._stop_event.is_set():
                        print(f"\n[TRIGGER] Sweep CANCELLED by kill signal.", flush=True)
                        return
                if self._stop_event.is_set():
                    print(f"\n[TRIGGER] Sweep CANCELLED by kill signal.", flush=True)
                    return
                print(f"\n[TRIGGER] Background sweep completed.", flush=True)
            except Exception as e:
                if self._stop_event.is_set():
                    print(f"\n[TRIGGER] Sweep CANCELLED (with exception during teardown: {e})", flush=True)
                else:
                    print(f"\n[TRIGGER] Background sweep FAILED: {e}", flush=True)

        with self._lock:
            self._sweep_started_at = datetime.utcnow().isoformat() + "Z"
            t = threading.Thread(target=_run, daemon=False, name="market-sweep")
            self._sweep_thread = t
            t.start()

        return {"status": "triggered", "message": "Sweep started in background",
                "started_at": self._sweep_started_at}

    def kill(self, **kwargs):
        with self._lock:
            if not self._sweep_thread or not self._sweep_thread.is_alive():
                return {"status": "no_sweep_running", "message": "Nothing to kill."}
            self._stop_event.set()
            started = self._sweep_started_at

        self._sweep_thread.join(timeout=5)
        alive = self._sweep_thread.is_alive()

        return {
            "status": "killed" if not alive else "kill_signal_sent",
            "message": f"Sweep started at {started} "
                       f"{'has stopped' if not alive else 'received kill signal (will stop at next checkpoint)'}."
        }

    def status(self, **kwargs):
        with self._lock:
            if self._sweep_thread and self._sweep_thread.is_alive():
                return {"status": "running", "started_at": self._sweep_started_at}
            return {"status": "idle"}


# ── Test runner ──
def sep(title):
    print(f"\n{'='*50}")
    print(f"  TEST: {title}")
    print(f"{'='*50}")


def assert_eq(label, actual, expected):
    ok = actual == expected
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}: got '{actual}', expected '{expected}'")
    if not ok:
        raise AssertionError(f"{label}: {actual} != {expected}")


class AssertionError(Exception):
    pass


def run_tests():
    failures = []
    app = MarketSweepApp(FakeAdkApp())
    app.set_up()  # Vertex AI calls this after deserialization

    # ── 1. Status when idle ──
    sep("status() when idle")
    try:
        r = app.status()
        assert_eq("status", r["status"], "idle")
    except AssertionError as e:
        failures.append(str(e))

    # ── 2. Kill when nothing running ──
    sep("kill() when idle")
    try:
        r = app.kill()
        assert_eq("status", r["status"], "no_sweep_running")
    except AssertionError as e:
        failures.append(str(e))

    # ── 3. Trigger a sweep ──
    sep("trigger() — start sweep")
    try:
        r = app.trigger(input={"user_id": "test_user", "message": "test sweep"})
        assert_eq("status", r["status"], "triggered")
        print(f"  started_at = {r.get('started_at')}")
    except AssertionError as e:
        failures.append(str(e))

    time.sleep(1)  # let the thread start

    # ── 4. Status while running ──
    sep("status() while running")
    try:
        r = app.status()
        assert_eq("status", r["status"], "running")
    except AssertionError as e:
        failures.append(str(e))

    # ── 5. Double trigger (should be blocked) ──
    sep("trigger() — double trigger guard")
    try:
        r = app.trigger(input={"user_id": "second_call"})
        assert_eq("status", r["status"], "already_running")
    except AssertionError as e:
        failures.append(str(e))

    # ── 6. Kill the running sweep ──
    sep("kill() — stop running sweep")
    try:
        r = app.kill()
        print(f"  kill response: {r}")
        assert_eq("status in [killed, kill_signal_sent]",
                   r["status"] in ("killed", "kill_signal_sent"), True)
    except AssertionError as e:
        failures.append(str(e))

    time.sleep(2)  # let thread fully exit

    # ── 7. Status after kill ──
    sep("status() after kill")
    try:
        r = app.status()
        assert_eq("status", r["status"], "idle")
    except AssertionError as e:
        failures.append(str(e))

    # ── 8. Trigger again after kill (should work) ──
    sep("trigger() — re-trigger after kill")
    try:
        r = app.trigger(input={"user_id": "post_kill_user", "message": "second sweep"})
        assert_eq("status", r["status"], "triggered")
    except AssertionError as e:
        failures.append(str(e))

    time.sleep(1)

    # ── 9. Kill the second sweep too (cleanup) ──
    sep("kill() — cleanup second sweep")
    try:
        r = app.kill()
        print(f"  kill response: {r}")
        assert_eq("status in [killed, kill_signal_sent]",
                   r["status"] in ("killed", "kill_signal_sent"), True)
    except AssertionError as e:
        failures.append(str(e))

    time.sleep(2)

    # ── Summary ──
    print(f"\n{'='*50}")
    if failures:
        print(f"  RESULT: {len(failures)} FAILURE(S)")
        for f in failures:
            print(f"    - {f}")
        print(f"{'='*50}")
        return 1
    else:
        print(f"  RESULT: ALL 9 TESTS PASSED")
        print(f"{'='*50}")
        return 0


if __name__ == "__main__":
    sys.exit(run_tests())
