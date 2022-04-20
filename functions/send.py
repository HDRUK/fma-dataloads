"""
Functions for formatting and sending emails via SendGrid.
"""

import os
import base64
import sendgrid
import datetime

from fpdf import FPDF
from sendgrid.helpers.mail import *


def send_summary_mail(
    publisher: dict = None,
    archived_datasets: list = None,
    new_datasets: list = None,
    updated_datasets: list = None,
    failed_validation: list = None,
    unsupported_version_datasets: list = None,
) -> None:
    """
    Build a formatted email for sending to the relevant parties.
    """
    attachment = None

    subject = f"Federated metadata synchronisation ({datetime.datetime.now().strftime('%d/%m/%y')})"

    message = """<div style="border: 1px solid #d0d3d4; border-radius: 15px; width: 700px; margin: 0 auto;">
                <table
                align="center"
                border="0"
                cellpadding="0"
                cellspacing="10"
                width="700"
                style="font-family: Arial, sans-serif">
                <thead>"""

    message += f"""<tr style="text-align: left;"><th>Federated metadata synchronisation summary for {datetime.datetime.now().strftime('%d/%m/%y')}</th></tr><tr></tr>"""

    if len(new_datasets) > 0:
        message += _format_html_list(
            datasets=new_datasets,
            subtitle="New datasets",
            pid_key="pid",
            version_key="datasetVersion",
        )

    if len(updated_datasets) > 0:
        message += _format_html_list(
            datasets=updated_datasets,
            subtitle="Updated datasets",
            pid_key="pid",
            version_key="datasetVersion",
        )

    if len(archived_datasets) > 0:
        message += _format_html_list(
            datasets=archived_datasets,
            subtitle="Archived datasets",
            pid_key="pid",
            version_key="version",
        )

    if len(failed_validation) > 0:
        attachment = _create_pdf(failed_validation)
        message += _format_html_list(
            datasets=failed_validation,
            subtitle="Validation failed",
            pid_key="identifier",
            version_key="version",
        )

    if len(unsupported_version_datasets) > 0:
        message += _format_html_list(
            datasets=unsupported_version_datasets,
            subtitle="Unsupported datasets",
            pid_key="identifier",
            version_key="version",
        )

    message += "</thead></table></div>"

    _send_mail(
        message=message,
        subject=subject,
        email_to=publisher["federation"]["notificationEmail"],
        attachment=attachment,
    )


def send_datasets_error_mail(publisher: dict = None, url: str = ""):
    """
    Build a formatted email for warning the custodian of a failur to connect to /datasets.
    """
    subject = f"Federated metadata synchronisation ({datetime.datetime.now().strftime('%d/%m/%y')}) - error retrieving list of datasets"

    message = """<div style="border: 1px solid #d0d3d4; border-radius: 15px; width: 700px; margin: 0 auto;">
                <table
                align="center"
                border="0"
                cellpadding="0"
                cellspacing="10"
                width="700"
                style="font-family: Arial, sans-serif">
                <thead>"""

    message += f"""<tr><th style="border: 0; font-size: 14px; text-align: left; font-weight: normal;">During the federated metadata synchronisation process, our systems encountered
                    an error when retrieving data from your organisation's "/datasets" endpoint at {url}.
                    
                    <p></p>
                    
                    This endpoint is critical to the functionality of the ingestion script. As such,
                    we have paused metadata syncing for your account. Please contact HDR UK's Data Improvement Team to resolve this issue.
                    </th></tr>"""

    _send_mail(
        message=message,
        subject=subject,
        email_to=publisher["federation"]["notificationEmail"],
    )


def send_auth_error_mail(publisher: dict = None, url: str = ""):
    """
    Build a formatted email for warning the custodian of a failur to connect to /datasets.
    """
    subject = f"Federated metadata synchronisation ({datetime.datetime.now().strftime('%d/%m/%y')}) - authorisation error"

    message = """<div style="border: 1px solid #d0d3d4; border-radius: 15px; width: 700px; margin: 0 auto;">
                <table
                align="center"
                border="0"
                cellpadding="0"
                cellspacing="10"
                width="700"
                style="font-family: Arial, sans-serif">
                <thead>"""

    message += f"""<tr><th style="border: 0; font-size: 14px; text-align: left; font-weight: normal;">During the federated metadata synchronisation process, our systems were
                    unable to authorise with the following endpoint: {url}.
                    
                    <p></p>
                    
                    For the moment, we have paused metadata syncing for your account. Please contact HDR UK's Data Improvement Team to resolve this issue.
                    </th></tr>"""

    _send_mail(
        message=message,
        subject=subject,
        email_to=publisher["federation"]["notificationEmail"],
    )


def _send_mail(
    message: str = "", subject: str = "", email_to: str = "", attachment: bytes = None
) -> None:
    """
    INTERNAL: send a message to a given address.
    """
    send_grid = sendgrid.SendGridAPIClient(api_key=os.environ.get("SENDGRID_API_KEY"))
    email_body = _get_header() + message + _get_footer()
    content = Content("text/html", email_body)
    mail = Mail(Email(os.getenv("EMAIL_SENDER")), To(email_to), subject, content)

    if attachment:
        attached_file = Attachment(
            FileContent(base64.b64encode(attachment).decode()),
            FileName(
                f"{datetime.datetime.now().strftime('%Y%m%d')}_validation_summary.pdf"
            ),
            FileType("application/pdf"),
            Disposition("attachment"),
        )

        mail.attachment = attached_file

    try:
        send_grid.client.mail.send.post(request_body=mail.get())
    except Exception as error:
        print(f"Error sending emails: {error}")


def _format_html_list(
    datasets: list = None, subtitle: str = "", pid_key: str = "", version_key: str = ""
) -> str:
    """
    INTERNAL: return a formatted list of datasets and their versions for a given subsection of the email.
    """
    html = f"""<tr><th style="border: 0; color: #29235c; font-size: 14px; text-align: left;">{subtitle} ({len(datasets)}): </th></tr>"""

    if subtitle == "Validation failed":
        html += """<tr><th style="border: 0; font-size: 12px; text-align: left;">
                    Please see the attached PDF for a full list of the validation errors encountered.</th></tr>"""

    html += "<tr><th><ul>"

    for i in datasets:
        schema_version_html = ""
        if subtitle == "Unsupported datasets":
            schema_version_html = f""", schema version: {i["@schema"]}"""

        html += f"""<li style="border: 0; font-size: 12px; font-weight: normal; color: #333333; text-align: left;">
                        {i[pid_key]} (version: {i[version_key]}{schema_version_html})
                        </li>"""

    html += "</ul></th></tr>"
    return html


def _get_header() -> str:
    """
    INTERNAL: return the pre-built email header.
    """
    return """<img src="https://storage.googleapis.com/hdruk-gateway_prod-cms/web-assets/HDRUK_logo_colour.png" 
            alt="HDR UK Logo" width="127" height="63" style="display: block; margin-left: auto; margin-right: 
            auto; margin-bottom: 24px; margin-top: 24px;"></img>"""


def _get_footer() -> str:
    """
    INTERNAL: return the pre-built email footer.
    """
    current_year = datetime.date.today().year

    return f"""<div style="margin-top: 23px; font-size:12px; text-align: center; line-height: 18px; color: #3c3c3b; width: 100%">
            <table
            align="center"
            border="0"
            cellpadding="0"
            cellspacing="16"
            style="font-family: Arial, sans-serif; 
            width:100%; 
            max-width:700px">
              <tbody>
                <tr>
                  <td align="center">
                    <a style="color: #475da7;" href="https://www.healthdatagateway.org">www.healthdatagateway.org</a>
                  </td>
                </tr>
                <tr>
                  <td align="center">
                    <span>©️HDR UK {current_year}. All rights reserved.<span/>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>"""


def _create_pdf(invalid_datasets: list = None) -> bytes:
    """
    INTERNAL: generate the PDF attachment for the validation errors (if any).
    """
    pdf = PDF()

    for i in invalid_datasets:
        pdf.add_page()
        pdf.set_font("Arial", size=12, style="B")
        pdf.cell(0, 10, txt=f'{i["summary"]["title"]} ({i["identifier"]})', ln=1)
        pdf.set_font("Arial", size=10)

        for j in i["validation_errors"]:
            pdf.cell(5, 5, txt=" - ", ln=0)
            pdf.multi_cell(
                0,
                5,
                txt=f'{"/".join(j["path"])}: {j["error"]}',
            )

    return pdf.output(dest="S").encode("latin-1")


class PDF(FPDF):
    """
    Subclass of FPDF to add footer and page number to every page.
    """

    def header(self):
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, "FMA validation errors", 0, 0, "R")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"{self.page_no()}", 0, 0, "R")
