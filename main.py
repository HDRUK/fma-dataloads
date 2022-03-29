import os
import sys
import base64
import logging
import pymongo

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

from functions import *

load_dotenv()


def main(event) -> None:
    """
    ENTRYPOINT: sync metadata from a given custodian.
    Args:
        event (dict): Event payload inc. base64 encoded publisher name (ex. { "data": "U0FJTA==" })
    """
    try:
        custodian_name = base64.b64decode(event["data"]).decode("utf-8")

        logging.basicConfig(level=logging.INFO)
        db = initialise_db(os.getenv("MONGO_URI"))

        ##########################################
        # GET publisher details
        ##########################################

        publisher = get_publisher(db=db, publisher_name=custodian_name)

        if publisher["federation"]["active"] != True:
            raise Exception(f"Federation is deactivated for custodian {custodian_name}")

        logging.info(f"Initiating FMA ingestion for {custodian_name}")

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
        # PID no longer exists in custodian list

        archived_datasets = datasets_to_archive(custodian_datasets, gateway_datasets)

        ##########################################
        # ADDITION logic
        ##########################################
        # PID is completely new to Gateway

        new_datasets = extract_new_datasets(custodian_datasets, gateway_datasets)

        sync_list = []
        invalid_datasets = []
        valid_datasets = []
        fetch_failed_datasets = []
        unsupported_version_datasets = []

        for i in new_datasets:
            try:
                dataset = get_dataset(
                    custodian_datasets_url, auth_token, i["identifier"]
                )
            except RequestException as e:
                # Fetching single dataset failed - update sync status
                logging.error(f'Error retrieving new dataset {i["identifier"]}: {e}')

                sync_list.extend(
                    create_sync_array(
                        datasets=[i],
                        sync_status="fetch_failed",
                        publisher=publisher,
                    )
                )
                fetch_failed_datasets.append(i)
                continue

            validation_schema = i["@schema"] if "@schema" in i else ""

            if not verify_schema_version(validation_schema):
                logging.warning(f'Schema not supported for dataset {i["identifier"]}')

                sync_list.extend(
                    create_sync_array(
                        datasets=[i],
                        sync_status="unsupported_version",
                        publisher=publisher,
                    )
                )
                unsupported_version_datasets.append(i)
                continue

            if not_valid := validate_json(validation_schema, dataset):
                invalid_datasets.append(not_valid)
            else:
                valid_datasets.append(transform_dataset(dataset=dataset))

        ##########################################
        # UPDATE logic
        ##########################################
        # PID already exists in sync collection

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

                if i["status"] != "ok" and time_elapsed < 5:
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
                    logging.warning(
                        f'Schema not supported for dataset {i["identifier"]}'
                    )

                    sync_list.extend(
                        create_sync_array(
                            datasets=[i],
                            sync_status="fetch_failed",
                            publisher=publisher,
                        )
                    )
                    fetch_failed_datasets.append(i)
                    continue

                validation_schema = (
                    custodian_version["@schema"]
                    if "@schema" in custodian_version
                    else ""
                )

                if not verify_schema_version(validation_schema):
                    logging.warning(
                        f'Schema not supported for dataset {i["identifier"]}'
                    )

                    sync_list.extend(
                        create_sync_array(
                            datasets=[i],
                            sync_status="unsupported_version",
                            publisher=publisher,
                        )
                    )
                    unsupported_version_datasets.append(i)
                    continue

                if not_valid := validate_json(validation_schema, new_datasetv2):
                    invalid_datasets.append(not_valid)
                else:
                    latest_dataset = get_latest_gateway_dataset(db=db, pid=i["pid"])

                    if not latest_dataset:
                        # New dataset not in tools, exists in sync, but previously fetch_failed or validation_failed
                        valid_datasets.append(transform_dataset(dataset=new_datasetv2))
                        continue

                    valid_datasets.append(
                        transform_dataset(
                            dataset=new_datasetv2, previous_version=latest_dataset
                        )
                    )

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
            or len(fetch_failed_datasets) > 0
            or len(unsupported_version_datasets) > 0
        ):
            send_summary_mail(
                publisher=publisher,
                archived_datasets=archived_datasets,
                new_datasets=valid_datasets,
                failed_validation=invalid_datasets,
                fetch_failed_datasets=fetch_failed_datasets,
                unsupported_version_datasets=unsupported_version_datasets,
            )

        logging.info(f"FMA ingestion for {custodian_name} completed")

    except CriticalError as e:
        # Critical error raised, log error, set federation.active to false, send an error email and exit the script
        logging.critical(e)
        update_publisher(db, status=False, publisher_name=custodian_name)
        send_error_mail(publisher_name=custodian_name, error=str(e))
        sys.exit(1)

    except Exception as e:
        # Unknown exception raised, log error and exit the program
        logging.critical(e)
        sys.exit(1)


def initialise_db(mongo_uri) -> pymongo.database.Database:
    uri = mongo_uri + "/" + os.getenv("MONGO_DATABASE")
    db = MongoClient(uri)[os.getenv("MONGO_DATABASE")]
    return db
