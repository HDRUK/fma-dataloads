"""
Functions for formatting and sending emails via SendGrid.
"""

import os
import sendgrid
import datetime

from sendgrid.helpers.mail import *


def send_summary_mail(
    publisher: dict = None,
    archived_datasets: list = None,
    new_datasets: list = None,
    failed_validation: list = None,
    fetch_failed_datasets: list = None,
    unsupported_version_datasets: list = None,
) -> None:
    """
    Send a formatted email to the relevant parties.
    """
    subject = (
        f"Federated Metadata Sync - {datetime.datetime.now().strftime('%d-%m-%y')}"
    )
    message = "<h2>Synchronisation results</h2>"

    if len(archived_datasets) > 0:
        message += "<br><b>The following datasets have been archived in the Gateway:</b><br><br>"
        for i in archived_datasets:
            message += f"<b>Dataset ID: </b>{i['pid']} (v {i['version']})<br>"

    if len(new_datasets) > 0:
        message += "<br><b>The following dataset versions have been added to the Gateway:</b><br><br>"
        for i in new_datasets:
            message += f"<b>Dataset ID: </b>{i['pid']} (v {i['datasetVersion']})<br>"

    if len(failed_validation) > 0:
        message += "<br><b>The following datasets have failed validation:</b><br /><br>"
        for i in failed_validation:
            message += f"<b>Dataset ID: </b>{i['identifier']} (v {i['version']})<br>"

    if len(fetch_failed_datasets) > 0:
        message += "<br><b>The script failed to retrieve the datasetv2 metadata for the following datasets:</b><br /><br>"
        for i in fetch_failed_datasets:
            message += f"<b>Dataset ID: </b>{i['identifier']} (v {i['version']})<br>"

    if len(unsupported_version_datasets) > 0:
        message += "<br><b>The following datasets had a schema version which is not supported:</b><br /><br>"
        for i in unsupported_version_datasets:
            message += f"<b>Dataset ID: </b>{i['identifier']} (v {i['version']})<br>"

    _send_mail(
        message=message,
        subject=subject,
        email_to=publisher["federation"]["notificationEmail"],
    )


def send_error_mail(publisher_name: str = "", error: str = "") -> None:
    """
    Send a message to an ops address given an error.
    """
    message = f"Fully syncing with the {publisher_name} API has failed: " + error
    subject = f"Error syncing federated datasets for {publisher_name} - {datetime.datetime.now().strftime('%d-%m-%y')}"

    _send_mail(message=message, subject=subject, email_to=os.getenv("EMAIL_ADMIN"))


def _send_mail(message: str = "", subject: str = "", email_to: str = "") -> None:
    """
    INTERNAL: send a message to a given address.
    """
    send_grid = sendgrid.SendGridAPIClient(api_key=os.environ.get("SENDGRID_API_KEY"))
    content = Content("text/html", message)
    mail = Mail(Email(os.getenv("EMAIL_SENDER")), To(email_to), subject, content)

    try:
        send_grid.client.mail.send.post(request_body=mail.get())
    except Exception as error:
        print(f"Error sending emails: {error}")
