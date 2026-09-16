import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


stop_file = Path(sys.argv[1])
for line in sys.stdin:
    if stop_file.exists():
        break
    timestamp = datetime.now(timezone.utc).strftime("[%Y-%m-%dT%H:%M:%SZ]")
    print(timestamp, line, end="", flush=True)

# Keep draining after startup so Tilt never blocks or gets a broken pipe.
with open("/dev/null", "w") as discard:
    shutil.copyfileobj(sys.stdin, discard)
