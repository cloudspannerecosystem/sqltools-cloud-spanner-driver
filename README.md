# SQLTools Cloud Spanner Driver

[Google Cloud Spanner](https://cloud.google.com/spanner) driver for [VS Code SQLTools](https://vscode-sqltools.mteixeira.dev/).

This driver supports executing queries and DML statements on Cloud Spanner databases, as
well as browsing through the tables and views of a database.

## Installation

Install the driver from the VS Code Marketplace page.

## Limitations

- The driver does not support DDL statements.
- The driver supports executing multiple statements in a single script, but each statement is executed in a separate transaction. Queries use single-use read-only transactions. DML statements use read/write transactions.
- Queries may return at most 100,000 rows. Queries that would return more than 100,000 rows will return an error.

## License
* See [LICENSE](LICENSE)

