"""Variant ids: compatibility with sc-inspector's `variant_id.py` (golden files copied from sc-inspector-data)."""

import json
from pathlib import Path

import pytest

from k8s_inspector import variant as v

GOLDEN = Path(__file__).parent / "fixtures" / "publish" / "hcloud_cax31" / "0"

X86 = {
    "Architecture:": "x86_64",
    "Vendor ID:": "GenuineIntel",
    "CPU family:": "6",
    "Model:": "85",
    "Stepping:": "4",
    "Model name:": "Intel(R) Xeon(R) Platinum 8175M CPU @ 2.50GHz",
    "Flags:": "avx2 avx avx",
    "CPU(s):": "2",
    "L3 cache:": "34603008 (1 instance)",
}
X86_OTHER = {**X86, "Model name:": "Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz", "Model:": "85"}


def test_golden_lscpu_gives_stored_variant_json_byte_for_byte():
    fields = v.parse_lscpu_stdout((GOLDEN / "lscpu" / "stdout").read_bytes())
    stored_text = (GOLDEN / "variant.json").read_text()
    stored = json.loads(stored_text)
    assert v.variant_id_from_fields(fields) == stored["id"] == "aarch64|arm|neoverse-n1|1|r3p1"
    assert v.match_variant(fields, stored)
    rebuilt = v.build_variant_json(
        fields, source=stored["source"], first_seen=stored["first_seen"], lscpu_version=stored["lscpu_version"]
    )
    assert json.dumps(rebuilt, indent=2) + "\n" == stored_text


def test_x86_ids_normalise_the_model_name():
    assert v.variant_id_from_fields(X86) == "x86_64|genuineintel|6|85|4|8175m"
    kvm = {**X86, "Model name:": "Intel(R) Xeon(R) CPU @ 2.20GHz"}
    assert v.variant_id_from_fields(kvm).endswith("|")  # no digit left: empty sku token
    assert v.variant_id_from_fields({**X86, "Model name:": "AMD EPYC-Rome Processor"}).endswith("|")


def test_typed_fields_in_variant_json():
    lscpu = v.build_variant_json(X86, source="observed", first_seen="2026-01-01T00:00:00")["lscpu"]
    assert (lscpu["CPU family:"], lscpu["Model:"], lscpu["Stepping:"], lscpu["CPU(s):"]) == (6, 85, 4, 2)
    assert lscpu["Flags:"] == ["avx", "avx2"]
    assert lscpu["L3 cache:"] == "34603008 (1 instance)"
    with pytest.raises(ValueError):
        v.build_variant_json(X86, source="nonsense")


def test_parse_lscpu_accepts_bytes_str_and_parsed():
    rows = [{"field": "Architecture:", "data": "x86_64"}, {"field": "Empty:", "data": None}]
    text = json.dumps({"lscpu": rows})
    expected = {"Architecture:": "x86_64", "Empty:": ""}
    assert v.parse_lscpu_stdout(text) == v.parse_lscpu_stdout(text.encode()) == expected
    assert v.parse_lscpu_stdout(rows) == expected
    with pytest.raises(ValueError):
        v.parse_lscpu_stdout("{}")


def test_pick_variant():
    stored = v.build_variant_json(X86, source="observed")
    other = v.build_variant_json(X86_OTHER, source="observed")
    assert v.pick_variant({}, X86) == 0
    assert v.pick_variant({0: stored}, X86) == 0
    assert v.pick_variant({0: stored}, X86_OTHER) == 1
    assert v.pick_variant({0: stored, 1: other}, X86_OTHER) == 1
    assert v.pick_variant({0: stored, 1: other}, {**X86, "Model name:": "Intel 9999"}) == 2
    assert v.pick_variant({1: other}, X86) == 0  # variant 0 has no variant.json yet: claim it, like the legacy
    assert not v.match_variant(X86, {"lscpu": "broken"})
