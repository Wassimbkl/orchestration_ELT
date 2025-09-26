import requests

def extract_csv():
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv"
    r = requests.get(url)
    with open("/tmp/covid.csv", "wb") as f:
        f.write(r.content)