from functions.helpers import *

custodian_datasets = [{"identifier": "xyz"}, {"identifier": "abc"}, {"identifier": "pqr"}, {"identifier": "mno"}]
gateway_datasets = [{"pid": "xyz"}, {"pid": "abc"}, {"pid": "pqr"}, {"pid": "def"}]


def test_datasets_to_archive():
    """
    Function should identifify correct datasets to archive in the Gateway.
    """
    datasets = datasets_to_archive(custodian_datasets=custodian_datasets, gateway_datasets=gateway_datasets)

    assert len(datasets) == 1
    assert datasets[0] == {"pid": "def"}


def test_datasets_to_add():
    """
    Function should identifify correct datasets that are new to the Gateway.
    """
    datasets = extract_new_datasets(custodian_datasets=custodian_datasets, gateway_datasets=gateway_datasets)

    assert len(datasets) == 1
    assert datasets[0] == {"identifier": "mno"}
