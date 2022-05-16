import os

from dotenv import load_dotenv
import pandas as pd
import requests
from sqlalchemy import create_engine

load_dotenv()

db_url = os.getenv("SCALINGO_POSTGRESQL_URL")

# see the NB note in README > Setup
if db_url.split("://")[0] == "postgre":
    db_url = db_url.replace("postgre", "postgresql", 1)

ENGINE = create_engine(db_url)

API_VERSION = os.getenv("BETA_API_VERSION", "2.5")

def get_startups_data() -> pd.DataFrame:
    """
    Fetch and format the startups beta.gouv.fr API into a pandas dataframe
    """
    r = requests.get(f"https://beta.gouv.fr/api/v{API_VERSION}/startups.json")
    json_data = r.json()
    data = pd.DataFrame(json_data["data"])["attributes"].apply(pd.Series)
    data["current_phase"] = data.phases.apply(lambda x: x[-1]["name"])
    data["missing_stats"] = data.stats_url.isna()
    data["missing_budget"] = data.budget_url.isna()
    data["missing_os"] = data.repository.isna()
    data["missing_dashlord"] = data.dashlord_url.isna()
    data.drop(columns=["phases", "content_url_encoded_markdown", "events"], inplace = True)
    data.set_index("name", inplace=True)
    return data

def get_members_data() -> pd.DataFrame:
    """
    Fetch and format the members beta.gouv.fr API into a pandas dataframe
    """
    r = requests.get(f"https://beta.gouv.fr/api/v{API_VERSION}/authors.json")
    json_data = r.json()
    data = pd.DataFrame(json_data)
    data["date_debut"] = pd.to_datetime(data.missions.apply(lambda x: min([d["start"] for d in x])))
    data["annee_debut"] = data.date_debut.apply(lambda x: x.year)
    data["date_fin"] = pd.to_datetime(data.missions.apply(lambda x: max([d["end"] for d in x])))
    data["annee_fin"] = data.date_fin.apply(lambda x: x.year)
    data["status_admin"] = data.missions.apply(lambda x: True if "admin" == x[-1]["status"] else False)
    data.drop(columns=["missions", "previously", "startups"], inplace = True)
    data.set_index("id", inplace=True)
    return data

def write_startups_data(dataframe: pd.DataFrame):
    """
    Write startups data to a PSQL database
    """
    return dataframe.to_sql('startups', ENGINE, if_exists="replace", index=True)

def write_members_data(dataframe: pd.DataFrame):
    """
    Write startups data to a PSQL database
    """
    return dataframe.to_sql('members', ENGINE, if_exists="replace", index=True)

def synch():
    startups = get_startups_data()
    num_startups_written = write_startups_data(startups)
    members = get_members_data()
    num_members_written = write_members_data(members)
    print(f"Wrote {num_startups_written} startups and {num_members_written} members.")
    return 0

if __name__=="__main__":
    synch()
