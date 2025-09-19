# Delta Tables Writer

This component writes data to Delta tables stored either in your cloud storage (AWS S3, Azure Blob Storage, Google Cloud Storage) or in Unity Catalog.

Component supports partitioned tables and allows you to choose between different load types such as Append, Overwrite, and Upsert (supported only for native tables using the primary key from the input table).

Complete documentation is available in the [README](https://github.com/keboola/component-delta-lake-writer/blob/main/README.md)