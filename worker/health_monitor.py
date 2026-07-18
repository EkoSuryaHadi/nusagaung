import time
import os
import sys
import json
import urllib.request

HEALTH_API_URL = os.environ.get("GAUNG_HEALTH_URL", "http://localhost:3000/api/health")
CHECK_INTERVAL_SECONDS = 60


def check_health():
    """Poll Gaung Health API and report system status."""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Polling health status from {HEALTH_API_URL}...")
    try:
        req = urllib.request.Request(HEALTH_API_URL, headers={"User-Agent": "GaungHealthDaemon/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            status = data.get("status")
            latency = data.get("totalLatencyMs", 0)

            if status == "HEALTHY":
                print(f"[HEALTH-OK] Status: HEALTHY ({latency}ms) - All services operational.")
            else:
                print(f"[HEALTH-WARN] Status: {status} ({latency}ms) - Degradation detected!")
                print(json.dumps(data.get("checks", {}), indent=2))
            return status == "HEALTHY"
    except Exception as e:
        print(f"[HEALTH-ALERT] Failed to reach Health API: {e}")
        return False


if __name__ == "__main__":
    print(f"=== Gaung DataOps Health Monitor Daemon Started (Interval: {CHECK_INTERVAL_SECONDS}s) ===")
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        is_healthy = check_health()
        sys.exit(0 if is_healthy else 1)
    
    while True:
        check_health()
        time.sleep(CHECK_INTERVAL_SECONDS)
