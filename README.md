## Federated Metadata Automation

A Python 3.8 ETL script written as a Google Cloud Function (scheduler > pub/sub > function) for ingesting custodian datasets in v2 specification, validating their structure according to the HDR UK dataset v2 specifcation and uploading them to the Gateway as part of the Federated Metadata Automation data flow.

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
DATABASE_USER=<<user>>
DATABASE_PASSWORD=<<password>>
DATABASE_DATABASE=<<database name>>
DATABASE_HOST=<<host>>
DATABASE_PORT=<<port>>

// SendGrid and emails
SENDGRID_API_KEY=<<SendGrid API key>>
EMAIL_SENDER=<<email address to use as sender>>
EMAIL_ADMIN=<<email address to send error notification to>>

// Dataset validation
DEFAULT_SCHEMA_URL=<<default v2 schema URL in case schema is not given by provider>>

// Google logs
LOGGING_LOG_NAME="cloudfunctions.googleapis.com%2Fcloud-functions" # another log name may be used for local development

// Local development - prints errors to stdout
ENVIRONMENT="dev"
```
