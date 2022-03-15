import sys


def get_gateway_datasets(db, publisher):
    """
    Get a list of datasets from the Gateway relevant to a given custodian (i.e., publisher).
    """
    try:
        datasets = db.sync.find({"publisherName": publisher})

        return datasets
    except Exception as e:
        print("Error retrieving gateway datasets: ", e)
        sys.exit(1)


def get_latest_gateway_dataset(db, pid=""):
    """
    Get the latest version of a given dataset from the tools collection in the Gateway
    """
    datasets = db.tools.find({"type": "dataset", "pid": pid}).sort("createdAt", -1)

    return datasets[0]


def archive_gateway_datasets(db, archived_datasets=[]):
    """
    Archive datasets on the Gateway given a list of datasets (which are then mapped to IDs).
    """
    db.tools.update_many(
        {"datasetv2.identifier": {"$in": list(map(lambda x: x["pid"], archived_datasets))}},
        {"$set": {"activeflag": "archive"}},
    )


def add_new_datasets(db, new_datasets=[]):
    """
    Add new datasets to the Gateway given a list of datasets.
    """
    db.tools.insert_many(new_datasets)


def get_publisher(db, publisher_name):
    """
    Get the relevant publisher documentation given a publisher name.
    """
    try:
        return db.publishers.find_one({"publisherDetails.name": publisher_name})
    except Exception as e:
        print("Error retrieving publisher details: ", e)
        sys.exit(1)


def sync_datasets(db, sync_list=[]):
    """
    Remove any existing sync status for a given PID and add new sync entry
    """
    db.sync.delete_many({"pid": {"$in": list(map(lambda x: x["pid"], sync_list))}})
    db.sync.insert_many(sync_list)
