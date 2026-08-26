from decimal import Decimal

import pytest

from forecast_store.naming import parse_quantile_column, quantile_column

# The spec's own examples (§7.3).
SPEC_EXAMPLES = [
    (Decimal("0.05"), "q05"),
    (Decimal("0.5"), "q50"),
    (Decimal("0.025"), "q02_5"),
    (Decimal("0.999"), "q99_9"),
]


@pytest.mark.parametrize(("level", "column"), SPEC_EXAMPLES)
def test_spec_examples(level, column):
    assert quantile_column(level) == column
    assert parse_quantile_column(column) == level


@pytest.mark.parametrize(
    "level",
    ["0.01", "0.05", "0.1", "0.25", "0.3", "0.5", "0.7", "0.9", "0.95", "0.975", "0.999", "0.005"],
)
def test_round_trip(level):
    d = Decimal(level)
    assert parse_quantile_column(quantile_column(d)) == d


def test_float_inputs_are_exact():
    # float 0.05 must not leak binary expansion into the name
    assert quantile_column(0.05) == "q05"
    assert quantile_column(0.1) == "q10"


def test_sub_one_percent_levels():
    assert quantile_column(Decimal("0.005")) == "q00_5"


@pytest.mark.parametrize("bad", [0, 1, 1.5, -0.1, "nope"])
def test_invalid_levels_rejected(bad):
    with pytest.raises(ValueError):
        quantile_column(bad)


@pytest.mark.parametrize("bad", ["q5", "q050", "q50_0", "p50", "q", "q100", "mean"])
def test_non_canonical_names_rejected(bad):
    with pytest.raises(ValueError):
        parse_quantile_column(bad)
