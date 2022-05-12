# sync-beta-api-postgre
Batch data from the beta.gouv.fr API to a PostgreSQL database

## Setup

This code was developped in python3.9 (use a venv !)

To run locally :

`pip install -r requirements.txt`

Create a `.env` file with the `SCALINGO_POSTGRESQL_URL` variable set to either your local PostgreSQL instance, or the Scalingo one (if you have access).

NB : use the `postgresql://` prefix instead of the Scalingo default `postgre://`.

`python main.py`