"""The recorder's parts, one module each; `market_tape/record.py` is the entry
point and import surface, and its docstring is the architecture.

Dependencies run one way: `core` imports the parts, `shard` imports `intake`
and `queue`, `intake` imports `queue`, `budget` imports `meter`; no part
imports `core`, and none imports `market_tape.record`.
"""
