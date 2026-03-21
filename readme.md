Fusetools
=========

Cloud infrastructure, database ETL, and GSuite utilities for Python.

Installation
------------

```bash
pip install fusetools
```

### Extras

Install only the dependencies you need:

```bash
# AWS (S3, DynamoDB, Lambda, SQS, CloudWatch, etc.)
pip install "fusetools[aws]"

# Firebase (Firestore, Storage)
pip install "fusetools[firebase]"

# GCP (Cloud Storage, Cloud Functions, Scheduler)
pip install "fusetools[gcp]"

# Google Suite (Sheets, Drive, Gmail, Docs)
pip install "fusetools[gsuite]"

# Database (Postgres, Oracle, Redshift, MySQL, Teradata)
pip install "fusetools[db]"

# Transfer (SSH, SFTP, HTTP downloads)
pip install "fusetools[transfer]"

# Everything
pip install "fusetools[all]"
```

Modules
-------

| Module | Description |
|---|---|
| `cloud_tools` | AWS, Firebase, and GCP infrastructure |
| `db_tools` | Database connections and ETL operations |
| `gsuite_tools` | Google Sheets, Drive, Gmail, and Docs |
| `transfer_tools` | SSH, SFTP, and local file operations |
| `logging_tools` | Stdout/stderr logging to file |

Development
-----------

```bash
uv sync --group dev --group test
make format
make lint
make test
```

Licensing
---------

Fusetools is distributed under the MIT License.
