## Federated Metadata Automation

Python ETL script for ingesting custodian datasets in v2 specification, validating their structure according to the HDR UK dataset v2 specifcation and uploading them to the Gateway as part of the Federated Metadata Automation data flow.

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

// More to follow...
```
