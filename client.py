# ------------------------------------------------------------------------------
#                               Academic Honesty
# ------------------------------------------------------------------------------
#
# This project (`client.py`, `server.py`) was developed using structured help 
# from **ChatGPT o3**. The AI assisted with design strategy, clarifying ambiguous 
# instructions, and enhancing code organization/documentation through multiple 
# iterations.
#
# Specific guidance included:
# - Mapping keys deterministically to shards
# - Resilient client-side retries with fallback across replicas
# - Deduplication of client requests using sequence tracking
# - Ensuring thread-safety through locking
# - Compatibility with all required course test cases (e.g., `test_test.py`)
#
# No other tools, libraries, or sources (beyond course materials) were involved.
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
    # Map a string key to a stable, non-negative integer
    return int(key) if key.isdigit() else sum(ord(ch) for ch in key)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg     = cfg
        self.id      = nrand()
        self.lock    = threading.Lock()
        self.seq     = 0
        self.last_replica = {}  # shard_id → last successful replica offset

    def _call(self, rpc: str, args, shard: int):
        num_servers = self.cfg.nservers
        num_replicas = self.cfg.nreplicas
        offset = self.last_replica.get(shard, 0)
        timeout = time.time() + 2.0

        while time.time() < timeout:
            for attempt in range(num_replicas):
                replica_id = (shard + (offset + attempt) % num_replicas) % num_servers
                try:
                    reply = self.servers[replica_id].call(rpc, args)
                    self.last_replica[shard] = (offset + attempt) % num_replicas
                    return reply
                except TimeoutError:
                    continue
            time.sleep(0.05)  # Retry delay between replica attempts

        raise TimeoutError("RPC call failed across all replicas within deadline.")
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
        
        #Retrieve the current value associated with `key`.
        #Will retry indefinitely on transient errors.
        #Returns an empty string if key is missing.
        
        shard = _key_id(key) % self.cfg.nservers
        args = GetArgs(key)
        response: GetReply = self._call("KVServer.Get", args, shard)
        return response.value
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
        #Internal method shared by Put and Append operations.
        #Tracks request sequence to enable deduplication.
        
        shard = _key_id(key) % self.cfg.nservers
        with self.lock:
            self.seq += 1
            args = PutAppendArgs(key, value, self.id, self.seq)
        response: PutAppendReply = self._call(f"KVServer.{op}", args, shard)
        return response.value if response else ""

    def put(self, key: str, value: str):
        #Insert or overwrite the value associated with `key`.
        self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        #Append `value` to existing content at `key`, returning result.
        return self.put_append(key, value, "Append")
