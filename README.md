# Rust Azure Functions Delta Table Pipeline Triggered by Blob Storage Events

This example demonstrates how to create Delta Table pipeline with blob triggered throughes Event Grid Azure function using Rust.

Whenever a CSV file is uploaded to a specific subfolder in container, the Azure Function is invoked to process the file. The content is then added to the Delta Table. If table does not exist, it is created. If there are any updates to a row determined by a specific column, the function will execute a merge, integrating the revised data into the existing Delta Table.

This approach is efficient for small files and eliminates the necessity for Spark.

#### Resources
- Azure Functions
- Azure Event Grid
- Azure Blob Storage

### How to setup the local dev environment

1. Use VS Code Azure Functions extension to install Azure Functions
2. Create a Function App with Custom Handler
3. Create a Blob Storage container and add environment variables to `local.settings.json`
4. Run ngrok to expose the function locally using this url schema below, for more details [here](https://learn.microsoft.com/en-us/azure/azure-functions/functions-event-grid-blob-trigger?tabs=isolated-process%2Cnodejs-v4&pivots=programming-language-python) <br>
```<ngrok_url>/runtime/webhooks/eventgrid?functionName=<FunctionName>```
5. Create a Event Grid topic using webhook use filtering with schema below for more details [here](https://learn.microsoft.com/en-us/azure/event-grid/event-filtering). <br>
```/blobServices/default/containers/<containername>/blobs/<subfolder>/```

#### Note:
`start.bat` can be used to start the local dev environment.

#### Note about schema of files in Azure Blob Storage:

The Hadoop Filesystem driver for Azure Data Lake Storage Gen2 is identified by its scheme identifier "abfs," which stands for Azure Blob File System. Like other Hadoop Filesystem drivers, it uses a URI format to locate files and directories in a Data Lake Storage Gen2 account. For more details [here](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-cli).

From Deltalake version `0.17.0`, can call the storage crate via: `deltalake::azure::register_handlers(None);` at the entrypoint for their code. For more information [here]([https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-cli](https://github.com/delta-io/delta-rs/releases/tag/rust-v0.17.0)).

```md
abfss://<container_name>@<account_name>.dfs.core.windows.net/<path>/<file_name>
https://<account_name>.blob.core.windows.net/<container_name>/<path>/<file_name>
```
