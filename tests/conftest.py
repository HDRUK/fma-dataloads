import pytest
import mongomock

from mongomock import ObjectId


@pytest.fixture(scope="module")
def initialise_db():
    """
    Creates an in-memory noSQL db and adds test data.
    """
    client = mongomock.MongoClient()
    db = client["custodian"]

    db.sync.insert_many(
        [
            {"pid": "dataset1", "publisherName": "FAKEY", "status": "fetch_failed"},
            {"pid": "dataset2", "publisherName": "FAKEY", "status": "fetch_failed"},
            {"pid": "dataset3", "publisherName": "FAKEY", "status": "fetch_failed"},
        ]
    )

    db.tools.insert_many(
        [
            {
                "_id": ObjectId("6241d1025a55d137b0fa0b89"),
                "type": "dataset",
                "pid": "pid1",
                "createdAt": "2021-10-05T16:25:43Z",
                "activeflag": "active",
            },
            {
                "_id": ObjectId("6231d1025a55d137b0fa0b89"),
                "type": "dataset",
                "pid": "pid2",
                "createdAt": "2021-10-06T16:25:43Z",
                "activeflag": "active",
            },
            {
                "_id": ObjectId("6221d1025a55d137b0fa0b89"),
                "type": "dataset",
                "pid": "pid3",
                "createdAt": "2021-10-07T16:25:43Z",
                "activeflag": "active",
            },
        ]
    )

    db.publishers.insert_one(
        {
            "publisherDetails": {"name": "FAKEY"},
            "_id": ObjectId("6421d1025a55d137b0fa0b89"),
            "active": True,
        }
    )

    yield db
    client.drop_database("db")
