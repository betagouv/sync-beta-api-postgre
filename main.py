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

def write_startups_data(dataframe: pd.DataFrame):
    """
    Write startups data to a PSQL database
    """
    return dataframe.to_sql('startups', ENGINE, if_exists="replace", index=True)

def synch():
    startups = get_startups_data()
    out = write_startups_data(startups)
    print(out)
    return out

if __name__=="__main__":
    synch()
