# /tmp/transform_covid.py
import pandas as pd

df = pd.read_csv("/tmp/covid.csv")
df_clean = df[["location", "new_cases"]]
df_clean.to_csv("/tmp/covid_clean.csv", index=False)

print("CSV transformé créé : /tmp/covid_clean.csv")
