import numpy as np
import pandas as pd

# headers = pd.read_csv('vaccination-data.csv', index_col=0,
#                       nrows=0).columns.tolist()
df = pd.read_csv("origin_data/vaccination-data.csv")
# print("df:",headers)
country_vaccines_used = pd.DataFrame(
    columns=["Country", "PERSONS_FULLY_VACCINATED_PER100", "VACCINES_USED"]
)

for index, x in df.iterrows():
    row = {"Country": "", "PERSONS_FULLY_VACCINATED_PER100": 0, "VACCINES_USED": 0}
    if pd.notna(x["VACCINES_USED"]):
        vaccines = x["VACCINES_USED"].split(",")
        for vaccine in vaccines:
            row["Country"] = x["COUNTRY"]
            row["PERSONS_FULLY_VACCINATED_PER100"] = x[
                "PERSONS_FULLY_VACCINATED_PER100"
            ]
            row["VACCINES_USED"] = vaccine
            rowFrame = pd.DataFrame([row])
            country_vaccines_used = pd.concat([country_vaccines_used, rowFrame])
print("country_vaccines_used:", country_vaccines_used)
# country_vaccines_used.to_csv("country_vaccines_used.csv", index=False)


globalDf = pd.read_csv("origin_data/WHO-COVID-19-global-data.csv")
# print("df:",headers)

globalDf["Year"]=pd.to_datetime(globalDf["Date_reported"]).dt.year
globalDf["Month"]=pd.to_datetime(globalDf["Date_reported"]).dt.month
globalDf["YearMonth"]=pd.to_datetime(globalDf["Date_reported"]).dt.strftime('%Y-%m')
print("globalDf:", globalDf)
globalDf.to_csv("global_covid_cases_deaths.csv", index=False)

for index, x in df.iterrows():
    row = {"Country": "", "PERSONS_FULLY_VACCINATED_PER100": 0, "VACCINES_USED": 0}
    if pd.notna(x["VACCINES_USED"]):
        vaccines = x["VACCINES_USED"].split(",")
        for vaccine in vaccines:
            row["Country"] = x["COUNTRY"]
            row["PERSONS_FULLY_VACCINATED_PER100"] = x[
                "PERSONS_FULLY_VACCINATED_PER100"
            ]
            row["VACCINES_USED"] = vaccine
            rowFrame = pd.DataFrame([row])
            country_vaccines_used = pd.concat([country_vaccines_used, rowFrame])
print("country_vaccines_used:", country_vaccines_used)
country_vaccines_used.to_csv("country_vaccines_used.csv", index=False)
# stringency index
covid_data = pd.read_csv("origin_data/owid-covid-data.csv")
covid_data["date"] = pd.to_datetime(covid_data["date"])
df_us = covid_data.loc[covid_data["iso_code"] == "USA"]
covid_data["date"] = pd.to_datetime(covid_data["date"])
# face covering policy
face_covering_policy = pd.read_csv(
    "origin_data/face-covering-policies-covid.csv", low_memory=False
)
face_covering_policy["Day"] = pd.to_datetime(face_covering_policy["Day"])
# public events
public_events_policy = pd.read_csv(
    "origin_data/public-events-covid.csv", low_memory=False
)
public_events_policy["Day"] = pd.to_datetime(face_covering_policy["Day"])
public_gathering_policy = pd.read_csv(
    "origin_data/public-gathering-rules-covid.csv", low_memory=False
)
public_gathering_policy["Day"] = pd.to_datetime(public_gathering_policy["Day"])
# internal movements policy
internal_movement_policy = pd.read_csv(
    "origin_data/internal-movement-covid.csv", low_memory=False
)
internal_movement_policy["Day"] = pd.to_datetime(internal_movement_policy["Day"])
# international movements policy
international_travel_policy = pd.read_csv(
    "origin_data/international-travel-covid.csv", low_memory=False
)
international_travel_policy["Day"] = pd.to_datetime(international_travel_policy["Day"])
# testing policy
testing_policy = pd.read_csv(
    "origin_data/covid-19-testing-policy.csv", low_memory=False
)
testing_policy["Day"] = pd.to_datetime(testing_policy["Day"])

policies = [
    face_covering_policy,
    public_events_policy,
    public_gathering_policy,
    internal_movement_policy,
    international_travel_policy,
    testing_policy,
]
policy_df = policies[0]
for i in range(len(policies)):
    if i == len(policies) - 1:
        break
    policy_df = policy_df.merge(
        policies[i + 1], on=["Code", "Day", "Entity"], how="inner"
    )
policy_df = policy_df.loc[policy_df["Entity"].isin(["United States", "Singapore"])]
policy_df = policy_df.drop(columns="Entity")
policy_df = policy_df.rename({"Code": "iso_code", "Day": "date"}, axis=1)

covid_data = covid_data.loc[covid_data["location"].isin(["United States", "Singapore"])]
covid_data = covid_data.drop(
    columns=[
        "new_tests",
        "gdp_per_capita",
        "extreme_poverty",
        "cardiovasc_death_rate",
        "diabetes_prevalence",
        "female_smokers",
        "male_smokers",
        "life_expectancy",
        "human_development_index",
        "new_tests_per_thousand",
        "weekly_hosp_admissions_per_million",
        "weekly_hosp_admissions",
        "hosp_patients_per_million",
        "hosp_patients",
        "icu_patients_per_million",
        "icu_patients",
        "excess_mortality_cumulative_absolute",
        "excess_mortality_cumulative",
        "excess_mortality",
        "excess_mortality_cumulative_per_million",
        "handwashing_facilities",
        "weekly_icu_admissions_per_million",
        "weekly_icu_admissions",
        "continent",
    ]
)
covid_data = covid_data.merge(policy_df, on=["iso_code", "date"], how="inner")
covid_data.to_csv("covid_data.csv", index=False)
print("df_raw_covid_data:", covid_data)
