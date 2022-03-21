from .exceptions import CriticalError


def get_gateway_datasets(db, publisher):
    """
    Get a list of datasets from the Gateway relevant to a given custodian (i.e., publisher).
    """
    try:
        datasets = db.sync.find(
            {"publisherName": publisher},
        )

        return datasets
    except Exception as e:
        raise CriticalError(
            f"Error retrieving gateway datasets for publisher {publisher}: {e}"
        )


def get_latest_gateway_dataset(db, pid=""):
    """
    Get the latest version of a given dataset from the tools collection in the Gateway
    """
    try:
        datasets = db.tools.find({"type": "dataset", "pid": pid}).sort("createdAt", -1)

        return datasets[0]
    except IndexError:
        return None
    except Exception as e:
        print(e.__class__.__name__)
        raise CriticalError(
            f"Error retrieving latest version of dataset {pid} from the Gateway: {e}"
        )


def archive_gateway_datasets(db, archived_datasets=[]):
    """
    Archive datasets on the Gateway given a list of datasets (which are then mapped to IDs).
    """
    try:
        db.tools.update_many(
            {"pid": {"$in": list(map(lambda x: x["pid"], archived_datasets))}},
            {"$set": {"activeflag": "archive"}},
        )
    except Exception as e:
        raise CriticalError(f"Error archiving datasets on the Gateway: {e}")


def add_new_datasets(db, new_datasets=[]):
    """
    Add new datasets to the Gateway given a list of datasets.
    """
    try:
        db.tools.insert_many(new_datasets)
    except Exception as e:
        raise CriticalError(
            f"Error inserting list of new datasets into the Gateway: {e}"
        )


def get_publisher(db, publisher_name):
    """
    Get the relevant publisher documentation given a publisher name.
    """
    try:
        return db.publishers.find_one({"publisherDetails.name": publisher_name})
    except Exception as e:
        raise CriticalError(
            f"Error retrieving the publisher details from the publisher collection for publisher name {publisher_name}: {e}"
        )


def sync_datasets(db, sync_list=[]):
    """
    Remove any existing sync status for a given PID and add new sync entry.
    """
    try:
        db.sync.delete_many({"pid": {"$in": list(map(lambda x: x["pid"], sync_list))}})
        db.sync.insert_many(sync_list)
    except Exception as e:
        raise CriticalError(f"Error updating the sync collection on the Gateway: {e}")
