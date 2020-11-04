"""
Marketplace services.

|pic1|
    .. |pic1| image:: ../images_source/commerce_tools/discogs1.png
        :width: 50%
"""

import oauth2 as oauth
import json
import os
import time
from selenium import webdriver


class Discogs:
    """
    Discogs' API infrastructure.

    .. image:: ../images_source/commerce_tools/discogs1.png

    """

    @classmethod
    def get_authentication_token(cls, usr, pwd, sav_dir, chromedriver_path, key, secret):
        """
        Creates a JSON authentication token to use for API calls.

        :param usr: Discogs username.
        :param pwd: Discogs password.
        :param sav_dir: Local filepath to save auth token.
        :param chromedriver_path: Path to Chromedriver instance for Selenium.
        :param key: Discogs API key.
        :param secret: Discogs API secret.
        :return: JSON authentication token.
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--auto-open-devtools-for-tabs")
        chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument('--disable-dev-shm-usage')
        browser = webdriver.Chrome(chromedriver_path, options=chrome_options)

        request_token_url = "https://api.discogs.com/oauth/request_token"
        user_agent = 'discogs_api_example/1.0'

        consumer = oauth.Consumer(key, secret)
        client = oauth.Client(consumer)

        # pass in your consumer key and secret to the token request URL. Discogs returns
        # an ouath_request_token as well as an oauth request_token secret.
        resp, content = client.request(request_token_url, 'POST', headers={'User-Agent': user_agent})

        token = content.decode("utf-8").split("&")[0].split("=")[1]
        secret = content.decode("utf-8").split("&")[1].split("=")[1]

        auth_url = f"https://www.discogs.com/oauth/authorize?oauth_token={token}"
        browser.get(auth_url)
        time.sleep(3)
        # enter creds
        browser.find_element_by_name("username").send_keys(usr)
        browser.find_element_by_name("password").send_keys(pwd)
        browser.find_element_by_name("submit").click()
        time.sleep(3)

        # click authorize button
        (browser
         .find_element_by_xpath('//*[@id="oauth_form_block"]/fieldset/button/i')
         .click())
        time.sleep(3)

        # get auth code
        auth_code = browser.find_element_by_class_name("auth_success_verify_code").text
        oauth_verifier = auth_code

        token2 = oauth.Token(token, secret)
        token2.set_verifier(oauth_verifier)
        client = oauth.Client(consumer, token2)

        access_token_url = 'https://api.discogs.com/oauth/access_token'
        resp, content = client.request(access_token_url, 'POST', headers={'User-Agent': user_agent})

        token = content.decode("utf-8").split("&")[0].split("=")[1]
        secret = content.decode("utf-8").split("&")[1].split("=")[1]
        os.chdir(sav_dir)
        print("Saving json with creds..")
        with open('auth.json', 'w') as f:
            json.dump({
                "token": token,
                "secret": secret
            }, f)

        browser.close()

    @classmethod
    def api_request(cls, key, secret, token_path,
                    request_type,
                    inventory_bytes=False,
                    search_cat=False,
                    search_string=False):
        """
        Function to perform generic Discogs API requests.

        :param key: Discogs API key.
        :param secret: Discogs API secret.
        :param token_path: Authenticated API token.
        :param request_type: API request type.
        :param inventory_bytes: Encoded bytes array for inventory to list.
        :param search_cat: Search category.
        :param search_string: Search string.
        :return: API JSON response.
        """
        with open(token_path,
                  encoding='utf-8',
                  errors='ignore') as json_data:
            auth = json.load(json_data, strict=False)

        user_agent = 'discogs_api_example/1.0'
        consumer = oauth.Consumer(key, secret)
        token = oauth.Token(key=auth['token'],
                            secret=auth['secret'])
        client = oauth.Client(consumer, token)

        if request_type == "general_search":
            query_string = f'https://api.discogs.com/database/search?{search_cat}={search_string}'

        elif request_type == "inventory_upload":
            query_string = f'https://api.discogs.com/inventory/upload/add'
            resp, content = client.request(
                uri=query_string,
                method="POST",
                headers={'User-Agent': user_agent},
                body=inventory_bytes
            )
            content2 = json.loads(content)
            results = [content2.get("results")]
            return results
        elif request_type == "category_search":
            pass

        # first query
        resp, content = client.request(query_string, headers={'User-Agent': user_agent})
        content2 = json.loads(content)
        results = [content2.get("results")]

        return results
