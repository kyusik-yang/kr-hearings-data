"""Loader tests on local fake assets (no network): KR_HEARINGS_CACHE points to a temp cache."""
import importlib

import pandas as pd
import pytest


@pytest.fixture()
def kh(tmp_path, monkeypatch):
    monkeypatch.setenv("KR_HEARINGS_CACHE", str(tmp_path))
    import kr_hearings_data._loader as L
    L = importlib.reload(L)
    v = tmp_path / L.LATEST_VERSION
    v.mkdir()
    pd.DataFrame({"conf_num": [1, 2, 3], "term": [20, 20, 21],
                  "hearing_type": ["상임위원회", "국정감사", "상임위원회"], "n_turns": [2, 1, 1]}
                 ).to_parquet(v / f"meetings_{L.LATEST_VERSION}.parquet")
    pd.DataFrame({"conf_num": [1, 1, 2], "turn_seq": [1, 2, 1], "text": ["a", "b", "c"]}
                 ).to_parquet(v / f"turns_t20_{L.LATEST_VERSION}.parquet")
    pd.DataFrame({"conf_num": [3], "turn_seq": [1], "text": ["d"]}).to_parquet(v / f"turns_t21_{L.LATEST_VERSION}.parquet")
    pd.DataFrame({"conf_num": [1], "leg_turn_seq": [1], "wit_turn_seq": [2], "hearing_type": ["상임위원회"]}
                 ).to_parquet(v / f"dyads_t20_{L.LATEST_VERSION}.parquet")

    def no_net(url, dest):
        raise AssertionError(f"unexpected download {url}")
    monkeypatch.setattr(L, "_download_file", no_net)
    return L


def test_asset_names(kh):
    assert kh._asset_name("turns", "v10", 21) == "turns_t21_v10.parquet"
    assert kh._asset_name("meetings", "v10") == "meetings_v10.parquet"
    assert kh._asset_name("dyads", "v10.1", 16) == "dyads_t16_v10.1.parquet"
    assert kh.LATEST_VERSION == "v10.2"
    with pytest.raises(ValueError, match="no longer distributed"):
        kh._asset_name("speeches", "v9")
    with pytest.raises(ValueError):
        kh._asset_name("turns", "v10")
    with pytest.raises(ValueError):
        kh._asset_name("nope", "v10")


def test_load_turns_by_term_and_hearing_type(kh):
    t = kh.load_turns(term=20)
    assert t.turn_seq.tolist() == [1, 2, 1]
    t = kh.load_turns(term=20, hearing_type="국정감사", columns=["turn_seq", "text"])
    assert list(t.columns) == ["turn_seq", "text"] and t.text.tolist() == ["c"]
    assert kh.load_speeches(term=21).text.tolist() == ["d"]          # v10 speeches = turns


def test_load_meetings_and_dyads(kh):
    assert len(kh.load_meetings(term=20)) == 2
    d = kh.load_dyads(term=20)
    assert d.leg_turn_seq.tolist() == [1]
    with pytest.raises(ValueError):
        kh.load_turns(term=15)
