import os
import sys
import argparse

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from google.cloud import logging

from functions import *


parser = argparse.ArgumentParser(
    description="Federated Metadata Automation ingestion script."
)

parser.add_argument(
    "--publisher",
    type=str,
    help="The name of publisher/custodian matching the name saved in the HDRUK Gateway publisher collection (publisherDetails.name)",
    required=True,
)

args = vars(parser.parse_args())


LOG_NAME = os.getenv("LOGGING_LOG_NAME")
MONGO_URI = (
    "mongodb://"
    f'{os.getenv("DATABASE_USER")}:'
    f'{os.getenv("DATABASE_PASSWORD")}@'
    f'{os.getenv("DATABASE_HOST")}:'
    f'{os.getenv("DATABASE_PORT")}/'
    f'{os.getenv("DATABASE_DATABASE")}'
)
CUSTODIAN_NAME = args["publisher"]


def main():
    try:
        logger = initialise_logging(LOG_NAME)
        db = initialise_db(MONGO_URI)

        ##########################################
        # GET publisher details
        ##########################################

        publisher = get_publisher(db=db, publisher_name=CUSTODIAN_NAME)

        if publisher["federation"]["active"] != True:
            raise ValueError(
                f"Federation is deactivated for custodian {CUSTODIAN_NAME}"
            )

        logger.log_text(
            f"Initiating FMA ingestion for {CUSTODIAN_NAME}", severity="INFO"
        )

        ##########################################
        # GET datasets from custodian and gateway
        ##########################################

        auth_token = ""

        secret_name = publisher["federation"]["auth"]["secretKey"]

        custodian_datasets_url = (
            publisher["federation"]["endpoints"]["baseURL"]
            + publisher["federation"]["endpoints"]["datasets"]
        )

        if publisher["federation"]["auth"]["type"] == "oauth":
            custodian_token_url = (
                publisher["federation"]["endpoints"]["baseURL"] + "/oauth/token"
            )
            secrets = get_client_secret(secret_name=secret_name)
            access_token = get_access_token(
                custodian_token_url,
                secrets["client_id"],
                secrets["client_secret"],
            )
            auth_token = f"Bearer {access_token}"
            custodian_datasets = get_datasets(custodian_datasets_url, auth_token)

        elif publisher["federation"]["auth"]["type"] == "api_key":
            secrets = get_client_secret(secret_name=secret_name)
            auth_token = f"Basic {secrets['api_key']}"
            custodian_datasets = get_datasets(custodian_datasets_url, auth_token)

        else:
            custodian_datasets = get_datasets(custodian_datasets_url)

        gateway_datasets = list(
            get_gateway_datasets(db=db, publisher=publisher["publisherDetails"]["name"])
        )

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
        sync_list = []

        for i in new_datasets:
            try:
                dataset = get_dataset(
                    custodian_datasets_url, auth_token, i["identifier"]
                )
            except RequestException as e:
                # Fetching single dataset failed - update sync status
                print(f'Error retrieving new dataset {i["identifier"]}:', e)
                sync_list.extend(
                    create_sync_array(
                        datasets=[i],
                        sync_status="fetch_failed_new",
                        publisher=publisher,
                    )
                )
                continue

            validation_schema = i["@schema"] if "@schema" in i else ""

            if not validation_schema:
                validation_schema = os.getenv("DEFAULT_SCHEMA_URL")

            if not_valid := validate_json(validation_schema, dataset):
                invalid_datasets.append(not_valid)
            else:
                valid_datasets.append(transform_dataset(dataset=dataset))

        ##########################################
        # UPDATE logic
        ##########################################

        if len(gateway_datasets) > 0:

            (
                custodian_versions,
                gateway_versions,
            ) = extract_overlapping_datasets(custodian_datasets, gateway_datasets)

            for i in gateway_versions:
                custodian_version = list(
                    filter(
                        lambda x: x["identifier"] == i["pid"],
                        custodian_versions,
                    )
                )[0]

                time_elapsed = datetime.timestamp(datetime.now()) - datetime.timestamp(
                    i["lastSync"]
                )

                if i["status"] == "ok" and i["version"] == custodian_version["version"]:
                    # No version change - move to next dataset
                    continue

                if i["status"] != "ok" and time_elapsed < 60 * 60 * 24 * 7:
                    # Previously failed validation but within 7 day window - move to next dataset
                    continue

                try:
                    new_datasetv2 = get_dataset(
                        custodian_datasets_url,
                        auth_token,
                        custodian_version["identifier"],
                    )
                except RequestException as e:
                    # Fetching single dataset failed - update sync status
                    print(
                        f'Error retrieving new dataset {custodian_version["identifier"]}:',
                        e,
                    )
                    sync_list.extend(
                        create_sync_array(
                            datasets=[i],
                            sync_status="fetch_failed_update",
                            publisher=publisher,
                        )
                    )
                    continue

                validation_schema = (
                    custodian_version["@schema"]
                    if "@schema" in custodian_version
                    else ""
                )

                if not validation_schema:
                    validation_schema = os.getenv("DEFAULT_SCHEMA_URL")

                if not_valid := validate_json(validation_schema, new_datasetv2):
                    invalid_datasets.append(not_valid)
                else:
                    latest_dataset = get_latest_gateway_dataset(db=db, pid=i["pid"])

                    valid_datasets.append(
                        transform_dataset(
                            dataset=new_datasetv2, previous_version=latest_dataset
                        )
                    )
                    if latest_dataset["datasetVersion"] in ["active", "inReview"]:
                        # Only archive previously active or inReview datasets, keep rejected datasets as rejected
                        archived_datasets.append(i)

        ##########################################
        # Database operations
        ##########################################

        if len(archived_datasets) > 0:
            archive_gateway_datasets(db=db, archived_datasets=archived_datasets)

        if len(valid_datasets) > 0:
            add_new_datasets(db=db, new_datasets=valid_datasets)
            sync_list.extend(
                create_sync_array(
                    datasets=valid_datasets,
                    sync_status="ok",
                    publisher=publisher,
                )
            )

        if len(invalid_datasets) > 0:
            sync_list.extend(
                create_sync_array(
                    datasets=invalid_datasets,
                    sync_status="validation_failed",
                    publisher=publisher,
                )
            )

        if len(sync_list) > 0:
            sync_datasets(db=db, sync_list=sync_list)

        ##########################################
        # Emails
        ##########################################

        if (
            len(archived_datasets) > 0
            or len(valid_datasets) > 0
            or len(invalid_datasets) > 0
        ):
            send_summary_mail(
                publisher=publisher,
                archived_datasets=archived_datasets,
                new_datasets=valid_datasets,
                failed_validation=invalid_datasets,
            )

    except Exception as e:
        # Critical error raised, log error, send an error email and exit the script
        print("ERROR:", e)
        logger.log_struct({"error": str(e), "source": CUSTODIAN_NAME}, severity="ERROR")
        send_error_mail(publisher_name=CUSTODIAN_NAME, error=str(e))
        sys.exit(1)


def initialise_logging(log_name):
    try:
        client = logging.Client()
        logger = client.logger(log_name)
        return logger
    except Exception as e:
        print("Error instantiating logger: ", e)
        raise


def initialise_db(mongo_uri):
    try:
        db = MongoClient(mongo_uri)[os.getenv("DATABASE_DATABASE")]
        return db
    except Exception as e:
        print("Error connecting to database: ", e)
        raise


if __name__ == "__main__":
    print("\x1B[2J\x1B[0f")
    load_dotenv()
    main()
