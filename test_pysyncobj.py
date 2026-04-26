"""
Diagnostic: does pysyncobj's @replicated body actually run on this Python version?

Background: pysyncobj's @replicated decorator uses sys._getframe + f_locals
mutation to inject a versioned copy of the method (e.g. 'increment_v0') into
the class body. PEP 667 (Python 3.13+) changed f_locals to return a snapshot
in some contexts, which would break this technique. This script verifies
whether the trick still works on this Python.
"""
import os
import sys
import tempfile
import time

print(f"Python: {sys.version}")

import pysyncobj
ver = getattr(getattr(pysyncobj, "version", None), "VERSION", "unknown")
print(f"pysyncobj: {ver}")

from pysyncobj import SyncObj, SyncObjConf, replicated


class Counter(SyncObj):
    def __init__(self, journal):
        conf = SyncObjConf(journalFile=journal)
        super().__init__("localhost:19999", [], conf)
        self.value = 0

    @replicated
    def increment(self):
        print(f"  [BODY RAN] value before: {self.value}", flush=True)
        self.value += 1
        print(f"  [BODY RAN] value after: {self.value}", flush=True)
        return self.value


# 1) Did the decorator successfully add the versioned copy to the class?
print(f"\nClass has attr 'increment':    {hasattr(Counter, 'increment')}")
print(f"Class has attr 'increment_v0': {hasattr(Counter, 'increment_v0')}  "
      f"(<-- if False, the frame trick failed)")

# 2) Spin up a single-node cluster
journal = os.path.join(tempfile.gettempdir(), "test_pysyncobj.journal")
for f in [journal, journal + ".meta", journal + ".bak", journal + ".flag"]:
    if os.path.exists(f):
        os.remove(f)

print("\nCreating Counter (single-node, self-elects as leader)...")
c = Counter(journal)

for i in range(20):
    if c._isReady():
        print(f"Ready after {i * 0.5:.1f}s. Leader: {c._getLeader()}")
        break
    time.sleep(0.5)
else:
    print("Cluster never became ready in 10s. Aborting.")
    sys.exit(1)

print(f"Method registry size: {len(c._idToMethod)}  "
      f"(<-- 0 means no @replicated methods got registered)")

# 3) Call the @replicated method
print("\nCalling c.increment(sync=True, timeout=5.0)...")
try:
    result = c.increment(sync=True, timeout=5.0)
    print(f"Returned: {result!r}")
    print(f"c.value: {c.value}")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# 4) Verdict
print("\n=== Diagnosis ===")
if not hasattr(Counter, "increment_v0"):
    print("CONFIRMED: pysyncobj's frame-injection trick FAILED on this Python.")
    print("The @replicated body cannot be registered for replication.")
    print("This is the bug. Recommended fixes: downgrade Python or switch library.")
elif c.value == 0:
    print("Class has _v0 but body never ran. Different bug; need to investigate further.")
else:
    print("pysyncobj works correctly on this Python. The bug is elsewhere.")

# Force-exit so SyncObj's background threads don't keep us hanging
os._exit(0)
