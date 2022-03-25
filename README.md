## Federated Metadata Automation

A Python 3.8 ETL script for ingesting custodian datasets in v2 specification, validating their structure according to the HDR UK dataset v2 specifcation and uploading them to the Gateway as part of the Federated Metadata Automation data flow.

### Setup

```
From Python 3.8+

$ python3 -m venv env
$ source env/bin/activate
$ pip install -r requirements.txt

```

### .env

```
// Gateway MongoDB database credentials
MONGO_URI=<<base MongoDB URI>> ex. "mongodb+srv://user:pass@cluster" (do not include db name)
MONGO_DATABASE=<<MongoDB database name>>

// SendGrid and emails
SENDGRID_API_KEY=<<SendGrid API key>>
EMAIL_SENDER=<<email address to use as sender>>
EMAIL_ADMIN=<<email address to send error notification to>>

A path to authorised GCP service account credentials must also be in the environment (e.g., GOOGLE_APPLICATION_CREDENTIALS)
```

### Example

The script can be triggered by passing a base64 encoded metadata publisher/custodian name in a dict to the main function in main.py.

```
python -c "from main import *; main({\"data\": \"<base64 encoded publisher name>\"})"
```
