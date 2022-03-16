import os
import sys
import uuid
import json

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

from functions import *


def initialise_db(mongo_uri):
    try:
        db = MongoClient(mongo_uri)[os.getenv("DATABASE_DATABASE")]
        return db
    except Exception as e:
        print("Error connecting to database: ", e)
        # Some logging and email logic
        sys.exit(1)


def main():
    mongo_uri = f'mongodb://{os.getenv("DATABASE_USER")}:{os.getenv("DATABASE_PASSWORD")}@{os.getenv("DATABASE_HOST")}:{os.getenv("DATABASE_PORT")}/{os.getenv("DATABASE_DATABASE")}'

    db = initialise_db(mongo_uri)

    publisher = get_publisher(db=db, publisher_name=os.getenv("CUSTODIAN_NAME"))

    if publisher["federation"]["active"] == False:
        print("Federation is deactivated for this custodian")
        sys.exit(1)

    ##########################################
    # GET Datasets from Custodian and Gateway
    ##########################################

    auth_token = ""

    secret_name = publisher["federation"]["auth"]["secretKey"]

    custodian_datasets_url = publisher["federation"]["endpoints"]["baseURL"] + publisher["federation"]["endpoints"]["datasets"]

    if publisher["federation"]["auth"]["type"] == "oauth":
        custodian_token_url = publisher["federation"]["endpoints"]["baseURL"] + "/oauth/token"
        secrets = get_client_secret(secret_name=secret_name)
        auth_token = get_access_token(custodian_token_url, secrets["client_id"], secrets["client_secret"])
        custodian_datasets = get_datasets(custodian_datasets_url, access_token=auth_token)

    elif publisher["federation"]["auth"]["type"] == "api_key":
        secrets = get_client_secret(secret_name=secret_name)
        auth_token = secrets["api_key"]
        custodian_datasets = get_datasets(custodian_datasets_url, api_key=secrets["api_key"])

    else:
        custodian_datasets = get_datasets(custodian_datasets_url)

    gateway_datasets = list(get_gateway_datasets(db=db, publisher=publisher["publisherDetails"]["name"]))

    ##########################################
    # ARCHIVE logic
    ##########################################

    archived_datasets = datasets_to_archive(custodian_datasets, gateway_datasets)

    ##########################################
    # ADDITION logic
    ##########################################

    new_datasets = extract_new_datasets(custodian_datasets, gateway_datasets)

    invalid_datasets = []
    valid_datasets = []

    for i in new_datasets:
        dataset = get_dataset(custodian_datasets_url, auth_token, i["identifier"])

        validation_schema = i["@schema"] if "@schema" in i else ""

        if not validation_schema:
            validation_schema = os.getenv("DEFAULT_SCHEMA_URL")

        if not_valid := validate_json(validation_schema, dataset):
            invalid_datasets.append(not_valid)
        else:
            question_answers = generate_question_answers(dataset)
            valid_datasets.append(
                {
                    "datasetv2": dataset,
                    "name": dataset["summary"]["title"],
                    "datasetVersion": dataset["version"],
                    "type": "dataset",
                    "pid": dataset["identifier"],
                    "datasetid": str(uuid.uuid4()),
                    "questionAnswers": json.dumps(question_answers),
                    "activeflag": "inReview",
                    "is5Safes": True,
                    "structuralMetadata": dataset["structuralMetadata"],
                    "timestamps": {"created": datetime.now(), "updated": datetime.now(), "submitted": datetime.now()},
                    "source": "FMA",
                    "createdAt": datetime.now(),
                    "updatedAt": datetime.now(),
                }
            )

    ##########################################
    # UPDATE logic
    ##########################################

    if len(gateway_datasets) > 0:

        custodian_versions, gateway_versions = extract_overlapping_datasets(custodian_datasets, gateway_datasets)

        for i in gateway_versions:
            custodian_version = list(filter(lambda x: x["identifier"] == i["pid"], custodian_versions))[0]

            time_elapsed = datetime.timestamp(datetime.now()) - datetime.timestamp(i["lastSync"])

            if i["status"] == "ok" and i["version"] == custodian_version["version"]:
                # No version change - move to next dataset
                continue

            if i["status"] != "ok" and time_elapsed < 60 * 60 * 24 * 7:
                # Previously failed validation but within 7 day window - move to next dataset
                continue

            new_datasetv2 = get_dataset(custodian_datasets_url, auth_token, custodian_version["identifier"])

            validation_schema = custodian_version["@schema"] if "@schema" in custodian_version else ""

            if not validation_schema:
                validation_schema = os.getenv("DEFAULT_SCHEMA_URL")

            if not_valid := validate_json(validation_schema, new_datasetv2):
                invalid_datasets.append(not_valid)
            else:
                activeflag = "active"

                latest_dataset = get_latest_gateway_dataset(db=db, pid=i["pid"])

                if latest_dataset["datasetVersion"] != "active":
                    activeflag = "inReview"

                question_answers = generate_question_answers(new_datasetv2)
                valid_datasets.append(
                    {
                        "datasetv2": new_datasetv2,
                        "name": new_datasetv2["summary"]["title"],
                        "datasetVersion": new_datasetv2["version"],
                        "type": "dataset",
                        "pid": i["pid"],
                        "datasetid": str(uuid.uuid4()),
                        "questionAnswers": json.dumps(question_answers),
                        "activeflag": activeflag,
                        "is5Safes": True,
                        "structuralMetadata": new_datasetv2["structuralMetadata"],
                        "timestamps": {"created": datetime.now(), "updated": datetime.now(), "submitted": datetime.now()},
                        "source": "FMA",
                        "createdAt": datetime.now(),
                        "updatedAt": datetime.now(),
                    }
                )
                archived_datasets.append(i)

    ##########################################
    # Database operations
    ##########################################

    sync_list = []

    if len(archived_datasets) > 0:
        archive_gateway_datasets(db=db, archived_datasets=archived_datasets)

    if len(valid_datasets) > 0:
        add_new_datasets(db=db, new_datasets=valid_datasets)
        sync_list.extend(create_sync_array(datasets=valid_datasets, sync_status="ok", publisher=publisher))

    if len(invalid_datasets) > 0:
        sync_list.extend(create_sync_array(datasets=invalid_datasets, sync_status="validation_failed", publisher=publisher))

    if len(sync_list) > 0:
        sync_datasets(db=db, sync_list=sync_list)

    ##########################################
    # Emails and Notifications
    ##########################################

    # Send mail
    send_mail(publisher=publisher, archived_datasets=archived_datasets, new_datasets=valid_datasets, failed_validation=invalid_datasets)


if __name__ == "__main__":
    print("\x1B[2J\x1B[0f")
    load_dotenv()
    main()
