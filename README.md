## Federated Metadata Automation

A Python 3.8+ ETL package wrapped as a Flask application for ingesting custodian datasets in v2 specification, validating their structure according to the HDR UK dataset v2 specifcation and uploading them to the Gateway as part of the Federated Metadata Automation data flow.

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
ADMIN_TEAM_EMAIL=<<email address for admin team>>

// Environment - used to add links to emails
GATEWAY_ENVIRONMENT=<<base dataset path for gateway web>> ex. "http://localhost:3000/dataset/"

A path to an authorised GCP service account credentials must also be in the environment (e.g., GOOGLE_APPLICATION_CREDENTIALS) when running locally
```

### Run

The ETL ingestion procedure is triggered by a HTTP request (for example, from Cloud Scheduler). This request triggers and runs the ingestion process and returns a 200 - OK status to acknowledge completion of the request.

To run this application:

```
running locally:

$ gunicorn --workers {NUM} --threads {NUM} main:app

running in a container:

$ gunicorn --workers {NUM} --threads {NUM} --bind 0.0.0.0:8080 --timeout 90 main:app
```

The MongoDB \_id ObjectId for the relevant publisher and database environment must be given in the JSON body of a POST request:

```
POST http://[host:port]

{ data: "<BASE64 encoded _id>" }

Reponses:
    200 - ok
```
