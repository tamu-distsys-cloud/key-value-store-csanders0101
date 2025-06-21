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

import logging, threading, random
import time, math
from typing import Tuple, Any

debugging = False

# For debug tracing, toggle this function on/off via the flag above
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Convert string key into a reproducible shard ID
def _key_id(key: str) -> int:
    return int(key) if key.isdigit() else sum(ord(c) for c in key)

# Struct to hold Put/Append RPC request parameters
class PutAppendArgs:
    def __init__(self, key: str, value: str, client_id: int, seq: int):
        self.key, self.value, self.client_id, self.seq = key, value, client_id, seq

# Struct to hold the response from a Put or Append operation
class PutAppendReply:
    def __init__(self, value):
        self.value = value

# Request container for Get operation
class GetArgs:
    def __init__(self, key):
        self.key = key

# Reply container for Get operation
class GetReply:
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv: dict[str, str] = {}                      # Key-value store
        self.last: dict[int, tuple[int, Any]] = {}        # Client op deduplication map
        self.me = None                                    # Cached server ID

    # Return this server's index within the configuration list
    def _my_id(self) -> int:
        if self.me is None:
            self.me = self.cfg.kvservers.index(self)
        return self.me

    # Compute the primary server index for a given key
    def _primary_key(self, key: str) -> int:
        return _key_id(key) % self.cfg.nservers

    # Check if this server is the primary for the key
    def _primary(self, key: str) -> bool:
        return self._my_id() == self._primary_key(key)

    # Determine whether this server is responsible for the given key
    def _responsible(self, key: str) -> bool:
        offset = (self._my_id() - self._primary_key(key)) % self.cfg.nservers
        return offset < self.cfg.nreplicas

    # Replicate an operation to backup replicas of the shard
    def _replicate(self, key, new_val, client_id, seq, reply_val, op):
        N, R = self.cfg.nservers, self.cfg.nreplicas
        primary = self._primary_key(key)
        for k in range(1, R):
            s_id = (primary + k) % N
            if s_id == self.me:
                continue
            follower = self.cfg.kvservers[s_id]
            follower._app_rep(key, new_val, client_id, seq, reply_val, op)

    # Apply an operation received from the primary (if not a duplicate)
    def _app_rep(self, key, new_val, client_id, seq, reply_val, op):
        with self.mu:
            last_op = self.last.get(client_id)
            if last_op and seq <= last_op[0]:
                return
            self.kv[key] = new_val
            self.last[client_id] = (seq, reply_val)

    # Handle Get RPC
    def Get(self, args: GetArgs) -> GetReply:
        if not self._responsible(args.key):
            raise TimeoutError()
        with self.mu:
            value = self.kv.get(args.key, "")
            self.cfg.op()
        return GetReply(value)

    # Handle Put RPC (only allowed on primary)
    def Put(self, args: PutAppendArgs) -> PutAppendReply:
        if not self._primary(args.key):
            return self._forward("Put", args)

        with self.mu:
            last_op = self.last.get(args.client_id)
            if last_op and args.seq <= last_op[0]:
                return PutAppendReply(last_op[1])

            self.kv[args.key] = args.value
            self.last[args.client_id] = (args.seq, None)
            self.cfg.op()

        self._replicate(args.key, args.value, args.client_id, args.seq, None, "Put")
        return PutAppendReply(None)

    # Handle Append RPC (only allowed on primary)
    def Append(self, args: PutAppendArgs) -> PutAppendReply:
        if not self._primary(args.key):
            return self._forward("Append", args)

        with self.mu:
            last_op = self.last.get(args.client_id)
            if last_op and args.seq <= last_op[0]:
                return PutAppendReply(last_op[1])

            current = self.kv.get(args.key, "")
            updated = current + args.value
            self.kv[args.key] = updated
            self.last[args.client_id] = (args.seq, current)
            self.cfg.op()

        self._replicate(args.key, updated, args.client_id, args.seq, current, "Append")
        return PutAppendReply(current)

    # Forward request to correct primary replica if not this server
    def _forward(self, op: str, args: PutAppendArgs):
        primary = self._primary_key(args.key)
        timeout_limit = time.time() + 2.0

        while time.time() < timeout_limit:
            endpoint_name = f"fwd-{self._my_id()}-{random.getrandbits(32)}"
            connection = self.cfg.net.make_end(endpoint_name)
            self.cfg.net.connect(endpoint_name, primary)
            self.cfg.net.enable(endpoint_name, primary in self.cfg.running_servers)

            try:
                return connection.call(f"KVServer.{op}", args)
            except TimeoutError:
                time.sleep(0.05)
            finally:
                self.cfg.net.delete_end(endpoint_name)

        raise TimeoutError()
