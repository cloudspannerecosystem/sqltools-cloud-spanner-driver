# SQLTools Cloud Spanner Driver

[Google Cloud Spanner](https://cloud.google.com/spanner) driver for [VS Code SQLTools](https://vscode-sqltools.mteixeira.dev/).

This driver supports executing queries and DML statements on Cloud Spanner databases, as
well as browsing through the tables and views of a database.

## Installation

Install the driver from the VS Code Marketplace page.

## Supported Features
- Execute SQL queries
- Execute DML statements (INSERT / UPDATE / DELETE)
- Execute DDL statements (CREATE TABLE, DROP TABLE, CREATE INDEX, ...)
- Connecting to both Cloud Spanner databases and the local Spanner Emulator

## Limitations

- The driver supports executing multiple statements in a single script, but each statement is executed in a separate transaction. Queries use single-use read-only transactions. DML statements use read/write transactions.
- Queries may return at most 100,000 rows. Queries that would return more than 100,000 rows will return an error.

## Raising Issues

If you have any questions, find a bug, or have a feature request please [open an issue](https://github.com/cloudspannerecosystem/sqltools-cloud-spanner-driver/issues/new).
Please note that this extension is not officially supported as part of the Google Cloud Spanner product.

## License
* See [LICENSE](LICENSE)

