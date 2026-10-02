import polars as pl
import pytest

from options_research.io.lazyio import RawDataWriteError, derived_path, sink


def test_sink_refuses_to_overwrite_raw_data(tmp_path):
    lf = pl.DataFrame({"a": [1, 2]}).lazy()
    raw_like = tmp_path / "databento_options_clean.parquet"
    with pytest.raises(RawDataWriteError):
        sink(lf, raw_like)


def test_sink_writes_to_derived_path(tmp_path):
    lf = pl.DataFrame({"a": [1, 2]}).lazy()
    out = sink(lf, tmp_path / "derived" / "step01.parquet")
    assert out.exists()
    assert pl.read_parquet(out)["a"].to_list() == [1, 2]


def test_derived_path_builds_standard_location():
    assert str(derived_path("step01", derived_root="data/derived")) == str(
        __import__("pathlib").Path("data/derived/step01.parquet")
    )
