## Federated Metadata Automation

A Python 3.8 ETL script wrapped as a Flask application for ingesting custodian datasets in v2 specification, validating their structure according to the HDR UK dataset v2 specifcation and uploading them to the Gateway as part of the Federated Metadata Automation data flow.

### Setup

```
From Python 3.8+

$ python3 -m venv env
$ source env/bin/activate
$ pip install -r requirements.txt

```

Add the entrypoint for the Flask application.

```
export FLASK_APP=main.py
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

### Run

This ETL script is triggered by a HTTP request (for example, from Cloud Scheduler). This request triggers the ingestion script and returns a 204 - no content status.

To run this application:

```
running locally:

$ flask run

running in a container:

$ flask run --host=0.0.0.0
```

To trigger the ingestion script, you need to pass the MongoDB \_id ObjectId for the relevant publisher and database environment in the JSON body of a POST request:

```
POST http://[host:port]

{ data: "<BASE64 encoded _id>" }

Reponses:
    204 - no content
```

The server will respond 204 if the HTTP trigger is successful. The request endpoint is configured as a trigger (i.e., akin to a cloud function) and will start the ingestion sctipt asynchronously and respond 204 immediately to acknowledge receipt of the request. No HTTP response is given by the actual ingestion procedure.
