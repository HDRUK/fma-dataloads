from mongomock import ObjectId
from bson.json_util import loads, dumps

from functions.queries import *


def test_get_gateway_datasets(initialise_db):
    """
    Function should return a list of datasets from the mock Mongo server.
    """
    db = initialise_db

    datasets_bson = get_gateway_datasets(db, "FAKEY")
    datasets = loads(dumps(datasets_bson))

    assert len(datasets) == 3
    assert list(datasets[0].keys()) == ["pid", "publisherName", "status", "_id"]
    assert list(map(lambda x: x["pid"], datasets)) == [
        "dataset1",
        "dataset2",
        "dataset3",
    ]


def test_get_gateway_datasets__raise_exception():
    """
    Function should raise if Exception is error encountered.
    """
    try:
        get_gateway_datasets("badDB", "FAKEY")
    except Exception as error:
        assert error is not None


def test_get_latest_gateway_dataset(initialise_db):
    """
    Function should return a single dataset as per the persistent ID.
    """
    db = initialise_db

    dataset = get_latest_gateway_dataset(db, "pid1")

    expected = {
        "_id": ObjectId("6241d1025a55d137b0fa0b89"),
        "type": "dataset",
        "pid": "pid1",
        "createdAt": "2021-10-05T16:25:43Z",
        "activeflag": "active",
    }

    assert dataset == expected


def test_get_latest_gateway_dataset__raise_exception():
    """
    Function should raise exception if error encountered.
    """
    try:
        get_latest_gateway_dataset("badDB", "pid1")
    except Exception as error:
        assert error is not None


def test_get_latest_gateway_dataset__index_error(initialise_db):
    """
    Function should return None if no datasets found for given PID.
    """
    db = initialise_db

    datasets = get_latest_gateway_dataset(db, "notAPID")

    assert datasets is None


def test_archive_gateway_datasets(initialise_db):
    """
    Function should archive the relevant datasets in the database.
    """
    db = initialise_db

    archive_gateway_datasets(db, [{"pid": "pid2"}])

    archived_dataset = db.tools.find_one({"pid": "pid2"})

    assert archived_dataset["activeflag"] == "archive"


def test_archive_gateway_datasets__raise_exception():
    """
    Function should raise exception if error encountered.
    """
    try:
        archive_gateway_datasets("badDB", [])
    except Exception as error:
        assert error is not None


def test_add_new_datasets(initialise_db):
    """
    Function successfully add new dict to the tools collection.
    """
    db = initialise_db
    to_add = [
        {
            "_id": ObjectId("6211d1025a55d137b0fa0b89"),
            "type": "dataset",
            "pid": "pid4",
            "createdAt": "2021-10-08T16:25:43Z",
            "activeflag": "active",
        },
    ]

    add_new_datasets(db, to_add)

    added_dataset = db.tools.find_one({"pid": "pid4"})

    assert added_dataset is not None


def test_add_new_datasets__raise_exception():
    """
    Function should raise exception if error encountered.
    """
    try:
        add_new_datasets("badDB", [])
    except Exception as error:
        assert error is not None


def test_get_publisher(initialise_db):
    """
    Function should retrieve the correct publisher from the database given a publisher name.
    """
    db = initialise_db

    publisher = get_publisher(db, "FAKEY")

    expected = {
        "publisherDetails": {"name": "FAKEY"},
        "_id": ObjectId("6421d1025a55d137b0fa0b89"),
        "active": True,
    }

    assert publisher == expected


def test_get_publisher__raise_exception():
    """
    Function should raise exception if error encountered.
    """
    try:
        get_publisher("badDB", [])
    except Exception as error:
        assert error is not None


def test_update_publisher(initialise_db):
    """
    Function should update the "active" field of the publisher to false.
    """
    db = initialise_db

    update_publisher(db, False, "FAKEY")

    updated_publisher = get_publisher(db, "FAKEY")

    assert updated_publisher["federation"]["active"] is False


def test_update_publisher__raise_exception():
    """
    Function should raise exception if error encountered.
    """
    try:
        update_publisher("badDB", False, "FAKEY")
    except Exception as error:
        assert error is not None


def test_sync_datasets(initialise_db):
    """
    Function should update the sync collection correctly.
    """
    db = initialise_db
    sync_list = [
        {"pid": "dataset1", "publisherName": "FAKEY", "status": "ok"},
        {"pid": "dataset2", "publisherName": "FAKEY", "status": "ok"},
        {"pid": "dataset3", "publisherName": "FAKEY", "status": "ok"},
    ]

    sync_datasets(db, sync_list)

    sync_test = db.sync.find_one({"pid": "dataset1"})

    assert sync_test["status"] == "ok"


def test_sync_datasets__raise_exception():
    """
    Function should raise exception if error encountered.
    """
    try:
        get_publisher("badDB", [])
    except Exception as error:
        assert error is not None
