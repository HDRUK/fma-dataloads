"""
Functions for formatting and sending emails via SendGrid.
"""

import os
import base64
import sendgrid
import datetime
import logging

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

    logging.debug(publisher)
    logging.debug(archived_datasets)
    logging.debug(new_datasets)
    logging.debug(updated_datasets)
    logging.debug(failed_validation)
    logging.debug(unsupported_version_datasets)
    # logging.debug(json.dumps(unsupported_version_datasets, ensure_ascii=True).encode("ascii", "replace"))

    subject = f"Federated metadata synchronisation ({datetime.datetime.now().strftime('%d/%m/%y')})"

    message = """<div style="border: 1px solid #d0d3d4; border-radius: 15px; width: 700px; margin: 0 auto;">
        <table
        align="center"
        border="0"
        cellpadding="0"
        cellspacing="20"
        width="700"
        style="font-family: Arial, sans-serif">
        <thead>
        """

    message += f"""<tr style="text-align: left;"><th style="font-weight: normal;">Ingestion of dataset metadata for {publisher["publisherDetails"]["name"]} from 
        {publisher["federation"]["endpoints"]["baseURL"]} ran without any critical errors. A summary of the results of the 
        action are listed below{'.' if len(failed_validation) == 0 else ', and more detailed logs are included in the attached pdf.'}
        </th></tr><tr></tr>
        """

    if len(new_datasets) > 0:
        message += """<tr><th style="border: 0; color: #29235c; font-size: 18px; text-align: left;">New dataset(s): </th></tr>"""
        message += f"""<tr><th style="border: 0; font-size: 14px; text-align: left; font-weight: normal">
            {len(new_datasets)} new dataset(s) {'was' if len(new_datasets) == 1 else 'were'} successfully ingested from your catalogue, 
            {'it is' if len(new_datasets) == 1 else 'they are'} now pending our internal review. If approved, {'it' if len(new_datasets) == 1 else 'they'} will 
            become live on the Innovation Gateway - and any further updates made in your catalogue will be fetched nightly. If {'this' if len(new_datasets) == 1 else 'any of these'} 
            dataset(s) {'is' if len(new_datasets) == 1 else 'are'} rejected in our internal review, we will pause syncing of {'this' if len(new_datasets) == 1 else 'that'} 
            dataset(s) until the issue is resolved.
                    
            <p></p>
                        
            The new dataset(s) {'is' if len(new_datasets) == 1 else 'are'} as follows:
            </th></tr>
            """
        message += _format_html_list(
            datasets=new_datasets,
            key="new",
        )

    if len(updated_datasets) > 0:
        message += """<tr><th style="border: 0; color: #29235c; font-size: 18px; text-align: left;">Updated dataset(s): </th></tr>"""
        message += f"""<tr><th style="border: 0; font-size: 14px; text-align: left; font-weight: normal">
            Updated information was found for {len(updated_datasets)} existing dataset(s) in your catalogue, 
            the {'entry' if len(updated_datasets) == 1 else 'entries'} on the Innovation Gateway {'has' if len(updated_datasets) == 1 else 'have'} 
            been updated accordingly.
                    
            <p></p>
                            
            The dataset(s) that {'has' if len(updated_datasets) == 1 else 'have'} been updated {'is' if len(updated_datasets) == 1 else 'are'} as follows:
            </th></tr>
            """
        message += _format_html_list(
            datasets=updated_datasets,
            key="updated",
        )

    if len(archived_datasets) > 0:
        message += """<tr><th style="border: 0; color: #29235c; font-size: 18px; text-align: left;">Archived dataset(s): </th></tr>"""
        message += f"""<tr><th style="border: 0; font-size: 14px; text-align: left; font-weight: normal">
            {len(archived_datasets)} dataset(s) {'was' if len(archived_datasets) == 1 else 'were'} previously ingested from your metadata catalogue, 
            but {'was' if len(archived_datasets) == 1 else 'were'} not found on this run - {'this dataset(s) has' if len(archived_datasets) == 1 else 'these dataset(s) have'}
            been archived on the Innovation Gateway and {'is' if len(archived_datasets) == 1 else 'are'} no longer discoverable to those without a direct url.
                    
            <p></p>
                        
            The dataset(s) that {'has' if len(archived_datasets) == 1 else 'have'} been archived {'is' if len(archived_datasets) == 1 else 'are'} as follows:
            </th></tr>
            """
        message += _format_html_list(
            datasets=archived_datasets,
            key="archived",
        )

    if len([*unsupported_version_datasets, *failed_validation]) > 0:
        if len(failed_validation) > 0:
            attachment = _create_pdf(failed_validation)

        message += """<tr><th style="border: 0; color: #29235c; font-size: 18px; text-align: left;">Failed validation/unsupported version: </th></tr>"""
        message += f"""<tr><th style="border: 0; font-size: 14px; text-align: left; font-weight: normal">
            {len([*unsupported_version_datasets, *failed_validation])} dataset(s) failed validation against our metadata schema, please ensure that all 
            metadata exposed through the endpoint is conformant to our schema (https://github.com/HDRUK/schemata). We support ingestion of datasets which 
            pass validation against versions 2.0.2 and 2.1 of the schema{'.' if len(failed_validation) == 0 else '. Further error logs are in the attached pdf.'}
                    
            <p></p>
                        
            The dataset(s) that {'failed' if len([*unsupported_version_datasets, *failed_validation]) ==  1 else 'have failed'} validation 
            {'is' if len([*unsupported_version_datasets, *failed_validation]) == 1 else 'are'} as follows:
            </th></tr>
            """
        message += _format_html_list(
            datasets=[*unsupported_version_datasets, *failed_validation],
            key="failed",
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
        cellspacing="20"
        width="700"
        style="font-family: Arial, sans-serif">
        <thead>
        """

    message += f"""<tr style="text-align: left;"><th style="font-weight: normal;">During the federated metadata synchronisation process, our systems encountered
        an error when retrieving data from your organisation's "/datasets" endpoint at {url}.
                    
        <p></p>
                    
        This endpoint is critical to the functionality of the ingestion script. As such,
        we have paused metadata syncing for your account. Please get in touch with us at service@healthdatagateway.com to resolve the issue.
        </th></tr>

        </thead></table></div>
        """

    _send_mail(
        message=message,
        subject=subject,
        email_to=publisher["federation"]["notificationEmail"],
    )


def send_auth_error_mail(publisher: dict = None, url: str = ""):
    """
    Build a formatted email for warning the custodian of a failur to connect to /datasets.
    """
    subject = f"Federated metadata synchronisation ({datetime.datetime.now().strftime('%d/%m/%y')}) - authentication error"

    message = """<div style="border: 1px solid #d0d3d4; border-radius: 15px; width: 700px; margin: 0 auto;">
        <table
        align="center"
        border="0"
        cellpadding="0"
        cellspacing="20"
        width="700"
        style="font-family: Arial, sans-serif">
        <thead>
        """

    message += f"""<tr style="text-align: left;"><th style="font-weight: normal;">We were unable to authorise on the following endpoint: {url}.
                    
        <p></p>
                    
        Because we were unable to authenticate, ingestion of metadata from your catalogue will be paused until this is resolved. Please get in touch 
        with us at service@healthdatagateway.com to resolve the issue.
        </th></tr>

        </thead></table></div>
        """

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
    admin_email = os.environ.get("ADMIN_TEAM_EMAIL")
    send_grid = sendgrid.SendGridAPIClient(api_key=os.environ.get("SENDGRID_API_KEY"))
    email_body = _get_header() + message + _get_footer()
    content = Content("text/html", email_body)

    logging.critical(content)

    if admin_email:
        email_to.append(admin_email)

    mail = Mail(
        Email(os.getenv("EMAIL_SENDER")),
        email_to,
        subject,
        content,
    )

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
        logging.critical(error)


def _format_html_list(datasets: list = None, key: str = "") -> str:
    """
    INTERNAL: return a formatted list of datasets and their versions for a given subsection of the email.
    """
    html = "<tr><th><ul>"

    for i in datasets:
        try:
            dataset_name = i["datasetv2"]["summary"]["title"]
        except KeyError:
            try:
                dataset_name = i["summary"]["title"]
            except KeyError:
                dataset_name = i["name"]

        try:
            version = i["datasetVersion"]
        except KeyError:
            version = i["version"]

        dataset_link = ""
        unsupported_version = ""
        if key == "updated":
            if i["activeflag"] == "active":
                dataset_link = (
                    f""" ({os.getenv("GATEWAY_ENVIRONMENT") + i["datasetid"]}"""
                )

        if key == "failed" and "@schema" in i.keys():
            unsupported_version = f""" - unsupported schema {i["@schema"]}"""

        html += f"""<li style="border: 0; font-size: 14px; font-weight: normal; color: #333333; text-align: left;">
            {dataset_name} (version {version}){dataset_link}{unsupported_version}
            </li>"""

    html += "</ul></th></tr>"
    return html


def _get_header() -> str:
    """
    INTERNAL: return the pre-built email header.
    """
    return """<img src="https://storage.googleapis.com/hdruk-gateway_prod-cms/web-assets/HDRUK_logo_colour.png" 
        alt="HDR UK Logo" width="127" height="63" style="display: block; margin-left: auto; margin-right: 
        auto; margin-bottom: 24px; margin-top: 24px;"></img>
        """


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
        </div>
        """


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
                txt=f'{"/".join([str(i) for i in j["path"]])}: {str(j["error"])}',
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
