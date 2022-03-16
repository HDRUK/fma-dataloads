from functions.helpers import *

custodian_datasets = [
    {"identifier": "xyz"},
    {"identifier": "abc"},
    {"identifier": "pqr"},
    {"identifier": "mno"},
]
gateway_datasets = [{"pid": "xyz"}, {"pid": "abc"}, {"pid": "pqr"}, {"pid": "def"}]


def test_datasets_to_archive():
    """
    Function should identifify correct datasets to archive in the Gateway.
    """
    datasets = datasets_to_archive(custodian_datasets, gateway_datasets)

    assert len(datasets) == 1
    assert datasets[0] == {"pid": "def"}


def test_datasets_to_add():
    """
    Function should identifify correct datasets that are new to the Gateway.
    """
    datasets = extract_new_datasets(custodian_datasets, gateway_datasets)

    assert len(datasets) == 1
    assert datasets[0] == {"identifier": "mno"}


def test_extract_overlapping_datasets():
    datasets_1, datasets_2 = extract_overlapping_datasets(
        custodian_datasets, gateway_datasets
    )

    expected_datasets_1 = [
        {"identifier": "xyz"},
        {"identifier": "abc"},
        {"identifier": "pqr"},
    ]
    expected_datasets_2 = [{"pid": "xyz"}, {"pid": "abc"}, {"pid": "pqr"}]

    assert len(datasets_1) == 3
    assert len(datasets_2) == 3
    assert [a == b for a, b in zip(datasets_1, expected_datasets_1)]
    assert [a == b for a, b in zip(datasets_2, expected_datasets_2)]
