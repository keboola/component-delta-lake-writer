Delta Tables Extractor
=============

Component supports two access modes:

### 1. Direct Access to Delta Tables
Direct access to delta tables in your blob storage. We currently support the following providers:

- **AWS S3**: [Access Grants Credentials](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-grants-credentials.html)
- **Azure Blob Storage**: [Create SAS Tokens](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers#create-sas-tokens-in-the-azure-portal)
- **Google Cloud Storage**: [Create service account](https://help.keboola.com/components/writers/database/bigquery/#create-service-account)

In this mode, the Delta Table path is defined by specifying the bucket/container and blob location where the table data is stored.

### 2. Unity Catalog
Currently we support only Azure Blob Storage backend.

**Setup Requirements:**
- **Access Token**: [How to get access token in Databricks](https://docs.databricks.com/aws/en/dev-tools/auth/pat#databricks-personal-access-tokens-for-workspace-users)
- **External Data Access**: [Enable external data access on the metastore](https://docs.databricks.com/aws/en/external-access/admin#enable-external-data-access-on-the-metastore)
- **Permissions**: Grant EXTERNAL USE SCHEMA permission
  - Navigate to: Workspace > Permissions > Add external use schema
  
    In this mode, the user selects the catalog, schema, and table in the configuration row.

**Unity Catalog supports two table types**
- **External tables** - Writing from the component directly to the underlying blob storage, and updating metadata in Unity Catalog.
- **Native (Databricks) tables** - Data are loaded using the selected DBX Warehouse.


### Input Mapping
Component can have mapped either one input table, or one or multiple parquet files with the same schema (not supported by the Native Databricks write mode).

### Data Destination Options
- **Load Type** Append, Overwrite, Upsert (supported only for native tables, using the PK from the input table), Raise error when existing (supported only for external table).
- **Columns to partition by  [optional]** - List of columns to partition the table.
- **Warehouse** DBX Warehouse to use for loading data (only for native tables).
- **Batch size** - Bigger batch will increase speed but also can cause out-of-memory issues more likely.
- **Preserve Insertion Order** - Disabling this option may help prevent out-of-memory issues.


