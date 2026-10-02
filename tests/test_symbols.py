from options_research.symbols import build_contract_key, parse_symbol


def test_parse_future():
    p = parse_symbol("FCEU SI 20260316 PS")
    assert p.kind == "future"
    assert p.product == "FCEU"
    assert p.expiry.isoformat() == "2026-03-16"


def test_parse_option_call():
    p = parse_symbol("EUCO SI 20260710 PS EU C 1.165")
    assert p.kind == "option"
    assert p.right == "C"
    assert p.strike == 1.165
    assert build_contract_key(p) == "EUCO|20260710|C|1.16500"


def test_parse_option_with_trailing_variant_token():
    """Real data observed format: an extra token after the strike, e.g. "... 1.1575 0"."""
    p = parse_symbol("EUCO SI 20260410 PS EU C 1.1575 0")
    assert p.kind == "option"
    assert p.right == "C"
    assert p.strike == 1.1575
    assert p.variant == "0"
    assert build_contract_key(p) == "EUCO|20260410|C|1.15750"


def test_parse_unknown():
    p = parse_symbol("garbage symbol !!")
    assert p.kind == "unknown"
    assert build_contract_key(p) is None


def test_parse_is_deterministic():
    s = "EUCO SI 20260710 PS EU P 1.165"
    assert parse_symbol(s) == parse_symbol(s)
