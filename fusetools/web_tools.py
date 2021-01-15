import requests
import json


def bitly_url_shortener(long_url, api_token, domain="bit.ly"):
    HEADERS = {'Authorization': f'Bearer {api_token}',
               'Content-Type': 'application/json'
               }

    data = {
        "long_url": long_url,
        "domain": domain
    }

    url = "https://api-ssl.bitly.com/v4/shorten"

    r = requests.post(
        url=url,
        json=data,
        headers=HEADERS
    )
    return json.loads(r.content)


