import pandas as pd
import requests

r = requests.get(f"https://beta.gouv.fr/api/v2.5/startups.json")
json_data = r.json()

# def get_incubators_data() -> pd.DataFrame:
#     """
#     Fetch and format the incubators beta.gouv.fr API into a pandas dataframe
#     """
    
#     data = pd.DataFrame(json_data["data"])["attributes"].apply(pd.Series)
#     data["current_phase"] = data.phases.apply(lambda x: x[-1]["name"])
#     data["date_current_phase"] = pd.to_datetime(data.phases.apply(lambda x: x[-1]["start"]))
#     data["date_impact_national"] = pd.to_datetime(data.events.apply(_extract_national_impact))
#     data["missing_stats"] = data.stats_url.isna()
#     data["missing_budget"] = data.budget_url.isna()
#     data["missing_os"] = data.repository.isna()
#     data["missing_dashlord"] = data.dashlord_url.isna()
#     data.drop(columns=["phases", "content_url_encoded_markdown", "events"], inplace = True)
#     data.set_index("name", inplace=True)
#     return data