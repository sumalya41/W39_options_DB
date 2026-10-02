"""options_research — functional, modular analysis package.

ABSOLUTE RULE: every module here is a set of pure functions. No class holds mutable
state across calls. Side effects (file reads, logging) are isolated to ``io/`` and to
thin driver scripts outside this package.
"""
