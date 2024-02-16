# Rust Delta Table Pipeline with Azure Functions Triggered by Blob Storage Events

This example demonstrates how to create Delta Table pipeline with blob triggered throughes Event Grid Azure function using Rust.

Whenever a CSV file is uploaded to a specific subfolder in container, the Azure Function is invoked to process the file. The content is then added to the Delta Table. If table does not exist, it is created. If there are any updates to a row determined by a specific column, the function will execute a merge, integrating the revised data into the existing Delta Table.

This approach is efficient for small files and eliminates the necessity for Spark.

#### Resources
- Azure Functions
- Azure Event Grid
- Azure Blob Storage

## How to setup the local dev environment

### Event Grid Event Filter
`/blobServices/default/containers/<containername>/blobs/<subfolder>/`
from this link: https://learn.microsoft.com/en-us/azure/event-grid/event-filtering

# Local development
`<ngrok_url>/runtime/webhooks/eventgrid?functionName=<FunctionName>`

```
abfss://<container_name>@<account_name>.dfs.core.windows.net/<path>/<file_name>
https://<account_name>.blob.core.windows.net/<container_name>/<path>/<file_name>
```