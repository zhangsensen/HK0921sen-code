import pytest

from utils.validation import validate_symbol


def test_validate_symbol_accepts_uppercase():
    assert validate_symbol("0700.hk") == "0700.HK"


def test_validate_symbol_rejects_invalid():
    with pytest.raises(ValueError):
        validate_symbol("bad_symbol")


def test_validate_symbol_requires_string():
    with pytest.raises(TypeError):
        validate_symbol(1234)  # type: ignore[arg-type]
