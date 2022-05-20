import os
import time
import http
import base64
import logging

from dotenv import load_dotenv
from pymongo import MongoClient
from flask import Flask, request, Response

from functions import *


load_dotenv()
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
client = MongoClient(os.getenv("MONGO_URI") + "/" + os.getenv("MONGO_DATABASE"))
db = client[os.getenv("MONGO_DATABASE")]


@app.route("/", methods=["POST"])
def trigger() -> Response:
    """
    HTTP wrapper for Cloud Scheduler.

    Description:
        HTTP request runs ingestion procedure and responds 200 (SUCCESS) or 500 (ERROR).
    """
    start_time = time.time()

    request_data = request.get_json()
    custodian_id = base64.b64decode(request_data["data"]).decode("utf-8")

    try:
        main(custodian_id=custodian_id)
    except Exception as error:
        logging.critical(error)
        return ("", http.HTTPStatus.INTERNAL_SERVER_ERROR)
    else:
        logging.info(f"FMA ingestion for {custodian_id} completed")
        logging.info(f"Run time: {round(time.time()-start_time, 2)} seconds")

    return ("", http.HTTPStatus.OK)


def main(custodian_id: str) -> None:
    """
    Sync metadata for a given publisher/custodian catalogue.

    Args:
        custodian_id: The relevant MongoDB _id for the Gateway publisher collection.

    Description:
        Authorise with publishers catalogue (if req.), pull list of datasets, compare
        datasets with Gateway sync collection for updates, new and archived datasets and
        modify the Gateway database accordingly.
    """
    try:
        ##########################################
        # GET publisher details
        ##########################################

        publisher = get_publisher(db=db, custodian_id=custodian_id)
        custodian_name = publisher["publisherDetails"]["name"]

        if not publisher["federation"]["active"]:
            raise Exception(f"Federation is deactivated for custodian {custodian_name}")

        logging.info(f"Initiating FMA ingestion for {custodian_name}")

        ##########################################
        # GET datasets from custodian and gateway
        ##########################################

        headers = {}

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
            headers = {"Authorization": f"Bearer {access_token}"}
            custodian_datasets = get_datasets(custodian_datasets_url, headers)

        elif publisher["federation"]["auth"]["type"] == "api_key":
            secrets = get_client_secret(secret_name=secret_name)
            headers = {
                "apikey": secrets["api_key"],
            }
            custodian_datasets = get_datasets(custodian_datasets_url, headers)

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
        new_valid_datasets = []
        updated_valid_datasets = []
        previous_version_datasets = []
        unsupported_version_datasets = []

        for i in new_datasets:
            try:
                dataset = get_dataset(
                    custodian_datasets_url, headers, i["persistentId"]
                )
            except RequestError as error:
                # Fetching single dataset failed - update sync status
                logging.error(
                    f'Error retrieving new dataset {i["persistentId"]}: {error}'
                )

                sync_list.extend(
                    create_sync_array(
                        datasets=[i],
                        sync_status="fetch_failed",
                        publisher=publisher,
                    )
                )
                continue

            validation_schema = i["@schema"] if "@schema" in i else ""

            if not verify_schema_version(validation_schema):
                logging.warning(f'Schema not supported for dataset {i["persistentId"]}')

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
                new_valid_datasets.append(
                    transform_dataset(publisher=publisher, dataset=dataset)
                )

        ##########################################
        # UPDATE logic
        ##########################################
        # PID already exists in sync collection

        if len(gateway_datasets) > 0 and len(custodian_datasets) > 0:
            (
                custodian_versions,
                gateway_versions,
            ) = extract_overlapping_datasets(custodian_datasets, gateway_datasets)

            for i in gateway_versions:
                custodian_version = list(
                    filter(
                        lambda x, pid=i["pid"]: x["persistentId"] == pid,
                        custodian_versions,
                    )
                )[0]

                if (
                    i["status"] in ["ok", "validation_failed"]
                    and i["version"] == custodian_version["version"]
                ):
                    # No version change - move to next dataset
                    continue

                try:
                    new_datasetv2 = get_dataset(
                        custodian_datasets_url,
                        headers,
                        custodian_version["persistentId"],
                    )
                except RequestError as error:
                    # Fetching single dataset failed - update sync status
                    logging.error(
                        f'Error retrieving updated dataset {custodian_version["persistentId"]}: {error}'
                    )

                    sync_list.extend(
                        create_sync_array(
                            datasets=[i],
                            sync_status="fetch_failed",
                            publisher=publisher,
                        )
                    )
                    continue

                validation_schema = (
                    custodian_version["@schema"]
                    if "@schema" in custodian_version
                    else ""
                )

                if not verify_schema_version(validation_schema):
                    logging.warning(
                        f'Schema not supported for dataset {custodian_version["persistentId"]}'
                    )

                    sync_list.extend(
                        create_sync_array(
                            datasets=[i],
                            sync_status="unsupported_version",
                            publisher=publisher,
                        )
                    )
                    unsupported_version_datasets.append(custodian_version)
                    continue

                if not_valid := validate_json(validation_schema, new_datasetv2):
                    invalid_datasets.append(not_valid)
                else:
                    latest_dataset = get_latest_gateway_dataset(db=db, pid=i["pid"])

                    if not latest_dataset:
                        # New dataset not in tools, exists in sync, but previously fetch_failed or validation_failed
                        new_valid_datasets.append(
                            transform_dataset(
                                publisher=publisher, dataset=new_datasetv2
                            )
                        )
                        continue

                    updated_valid_datasets.append(
                        transform_dataset(
                            publisher=publisher,
                            dataset=new_datasetv2,
                            previous_version=latest_dataset,
                        )
                    )

                    previous_version_datasets.append(i)

        ##########################################
        # Database operations
        ##########################################

        if len([*archived_datasets, *previous_version_datasets]) > 0:
            archive_gateway_datasets(
                db=db,
                archived_datasets=archived_datasets,
                previous_versions=previous_version_datasets,
            )

        if len([*new_valid_datasets, *updated_valid_datasets]) > 0:
            add_new_datasets(
                db=db, new_datasets=[*new_valid_datasets, *updated_valid_datasets]
            )
            sync_list.extend(
                create_sync_array(
                    datasets=[*new_valid_datasets, *updated_valid_datasets],
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

        if any(
            len(datasets) > 0
            for datasets in [
                archived_datasets,
                new_valid_datasets,
                updated_valid_datasets,
                invalid_datasets,
                unsupported_version_datasets,
            ]
        ):
            send_summary_mail(
                publisher=publisher,
                archived_datasets=archived_datasets,
                new_datasets=new_valid_datasets,
                updated_datasets=updated_valid_datasets,
                failed_validation=invalid_datasets,
                unsupported_version_datasets=unsupported_version_datasets,
            )

    except (CriticalError, RequestError, AuthError) as error:
        # Custom error raised, log error, send email if required, set federation.active to false
        if error.__class__.__name__ == "AuthError":
            send_auth_error_mail(publisher=publisher, url=error.__url__())

        if error.__class__.__name__ == "RequestError":
            send_datasets_error_mail(publisher=publisher, url=error.__url__())

        update_publisher(db, status=False, custodian_id=custodian_id)
        raise
