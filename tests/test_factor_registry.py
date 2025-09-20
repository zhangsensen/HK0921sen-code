import pytest

pytest.importorskip("numpy")
pytest.importorskip("pandas")

from hk_factor_discovery.factors import all_factors


def test_factor_registry_contains_72_unique_factors():
    factors = all_factors()
    names = [factor.name for factor in factors]
    assert len(factors) == 72
    assert len(set(names)) == 72
