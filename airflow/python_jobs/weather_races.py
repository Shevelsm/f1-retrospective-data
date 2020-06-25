import requests

url = "https://iddogino-global-weather-history-v1.p.rapidapi.com/weather"

querystring = {
    "date": "2018-01-11",
    "latitude": "33.9463626",
    "longitude": "-118.4010845",
}

headers = {
    "x-rapidapi-host": "iddogino-global-weather-history-v1.p.rapidapi.com",
    "x-rapidapi-key": "cd5737116fmsh0fb8a9815a47d64p12ab7cjsnc83fa15cc3a1",
}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
