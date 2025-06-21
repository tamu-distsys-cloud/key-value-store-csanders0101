# ------------------------------------------------------------------------------
#                               Academic Honesty
# ------------------------------------------------------------------------------
#
# This project implementation (`client.py`, `server.py`) was developed with 
# structured assistance from **ChatGPT o3**. The AI was used to guide design 
# decisions, explain unclear requirements, and improve documentation and code 
# structure via multiple iterative prompts.
#
# Contributions included:
# - Static key-to-shard mapping logic
# - Client retry loops with replica fallback
# - Handling of duplicate requests via client sequence tracking
# - Thread-safe updates using locks
# - Passing all course-provided test cases (e.g., `test_test.py`)
#
# No other external sources or third-party libraries (beyond course-provided files)
# were used in creating this submission.
#
# ------------------------------------------------------------------------------


import random, time
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

def _key_id(key: str) -> int:
    # Convert a string key into a consistent non-negative integer
    return int(key) if key.isdigit() else sum(ord(c) for c in key)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.id        = nrand()
        self.lock = threading.Lock()
        self.seq       = 0 
        self.last_rep  = {}        # Maps shard ID to preferred replica offset

    def _call(self, rpc: str, args, shard: int):
        n  = self.cfg.nservers
        R  = self.cfg.nreplicas
        start_off = self.last_rep.get(shard, 0)

        deadline = time.time() + 2.0  # Timeout limit for this operation
        while time.time() < deadline:
            for k in range(R):
                shard_id = (shard + (start_off + k) % R) % n
                try:
                    rep = self.servers[shard_id].call(rpc, args)
                    self.last_rep[shard] = (start_off + k) % R
                    return rep
                except TimeoutError:
                    continue
            time.sleep(0.05)  # Brief pause before retrying replicas

        # If all replicas fail for the given shard within the timeout window
        raise TimeoutError()


    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        shard = _key_id(key) % self.cfg.nservers
        args  = GetArgs(key)
        rep: GetReply = self._call("KVServer.Get", args, shard)
        return rep.value
    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        shard = _key_id(key) % self.cfg.nservers
        self.seq += 1
        args = PutAppendArgs(key, value, self.id, self.seq)
        rep: PutAppendReply = self._call(f"KVServer.{op}", args, shard)
        return rep.value if rep is not None else ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")