"""Pure symbol parsing: string in, immutable record out. No I/O, no global state.

Eurex convention observed in the playbook:
- futures:  "FCEU SI 20260316 PS"
- options:  "EUCO SI 20260710 PS EU P 1.165"
Real data additionally carries an optional trailing numeric token after the strike
(observed: "EUCO SI 20260410 PS EU C 1.1575 0") — [verify] its meaning (variant/version);
captured as ``variant`` rather than discarded. Unknown/unparsed tokens must be surfaced,
never silently dropped.
"""
from __future__ import annotations

import re
from datetime import date
from typing import NamedTuple, Optional

_FUTURE_RE = re.compile(r"^(?P<product>\w+)\s+(?P<token1>\w+)\s+(?P<expiry>\d{8})\s+(?P<token2>\w+)$")
_OPTION_RE = re.compile(
    r"^(?P<product>\w+)\s+(?P<token1>\w+)\s+(?P<expiry>\d{8})\s+(?P<token2>\w+)\s+"
    r"(?P<style>\w+)\s+(?P<right>[CP])\s+(?P<strike>[\d.]+)(?:\s+(?P<variant>\w+))?$"
)


class ParsedSymbol(NamedTuple):
    raw: str
    kind: str  # "future" | "option" | "unknown"
    product: Optional[str] = None
    expiry: Optional[date] = None
    right: Optional[str] = None
    strike: Optional[float] = None
    style: Optional[str] = None
    variant: Optional[str] = None


def _to_date(token: str) -> date:
    return date(int(token[:4]), int(token[4:6]), int(token[6:8]))


def parse_symbol(symbol: str) -> ParsedSymbol:
    """Deterministic: same string always yields the same ParsedSymbol."""
    s = symbol.strip()
    m = _OPTION_RE.match(s)
    if m:
        return ParsedSymbol(
            raw=symbol,
            kind="option",
            product=m.group("product"),
            expiry=_to_date(m.group("expiry")),
            right=m.group("right"),
            strike=float(m.group("strike")),
            style=m.group("style"),
            variant=m.group("variant"),
        )
    m = _FUTURE_RE.match(s)
    if m:
        return ParsedSymbol(
            raw=symbol,
            kind="future",
            product=m.group("product"),
            expiry=_to_date(m.group("expiry")),
        )
    return ParsedSymbol(raw=symbol, kind="unknown")


def build_contract_key(parsed: ParsedSymbol) -> Optional[str]:
    """Stable economic contract key; None when the symbol could not be parsed."""
    if parsed.kind == "option" and parsed.expiry and parsed.right and parsed.strike is not None:
        return f"{parsed.product}|{parsed.expiry:%Y%m%d}|{parsed.right}|{parsed.strike:.5f}"
    if parsed.kind == "future" and parsed.expiry:
        return f"{parsed.product}|{parsed.expiry:%Y%m%d}|FUT"
    return None
