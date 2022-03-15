import os
import sendgrid
import datetime

from sendgrid.helpers.mail import *


def send_mail(publisher={}, archived_datasets=[], new_datasets=[], failed_validation=[]):
    """
    Send a formatted email to the relevant parties.
    """
    sg = sendgrid.SendGridAPIClient(api_key=os.environ.get("SENDGRID_API_KEY"))
    from_email = Email(os.getenv("EMAIL_SENDER"))
    to_email = To(publisher["federation"]["notificationEmail"])

    subject = f"Federated Metadata Sync - {datetime.datetime.now().strftime('%d-%m-%y')}"

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
            message += f"<b>Dataset ID: </b>{i['dataset']['identifier']} (v {i['dataset']['version']})<br>"

    content = Content("text/html", message)
    mail = Mail(from_email, to_email, subject, content)

    try:
        sg.client.mail.send.post(request_body=mail.get())
    except Exception as e:
        print("Error sending email: ", e)
