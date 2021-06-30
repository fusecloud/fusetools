"""
Accounting tasks and applications.

|pic1| |pic2|
    .. |pic1| image:: ../images_source/accounting_tools/logo_quickbooks.png
        :width: 50%
"""
import webbrowser
from typing import Optional

import pandas as pd
from intuitlib.client import AuthClient
from quickbooks import QuickBooks
from intuitlib.enums import Scopes
from quickbooks.objects import Purchase, Attachable


# pip install python-quickbooks

class Quickbooks:
    """

    """

    @classmethod
    def get_auth_token(cls, client_id: str, client_secret: str, environment: str = "sandbox",
                       redirect_uri: str = "'https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl'",
                       open_browser=False):
        """
        Returns the link complete authentication process and retrieve refresh token.

        :param client_id:
        :param client_secret:
        :param environment:
        :param redirect_uri:
        :param open_browser:
        :return:
        """
        auth_client = AuthClient(
            client_id=client_id,
            client_secret=client_secret,
            environment=environment,
            redirect_uri=redirect_uri,
        )

        scopes = [Scopes.ACCOUNTING]

        # // Get authorization URL
        auth_url = auth_client.get_authorization_url(scopes)
        if open_browser:
            webbrowser.open(auth_url)

        cls.auth_client = auth_client
        return auth_client, auth_url

    @classmethod
    def get_client(cls, refresh_token: str, company_id: str, auth_client=False):
        """

        :param refresh_token:
        :param company_id:
        :param auth_client:
        :return:
        """
        client = QuickBooks(
            auth_client=auth_client if auth_client else cls.auth_client,
            refresh_token=refresh_token,
            company_id=company_id,
        )

        cls.client = client
        return client

    @classmethod
    def get_payments(cls, client=False) -> pd.DataFrame:
        """

        :param client:
        :return:
        """
        purchases = Purchase.all(qb=client if client else cls.client)
        purchase_df = pd.concat([pd.DataFrame(x.__dict__.values()).T for x in purchases])
        purchase_df.columns = purchases[0].__dict__.keys()
        return purchase_df

    @classmethod
    def upload_attachment(cls, obj_type: str, obj_id: int, ext_type: str, filepath: str, filename: Optional[str] = None,
                          client=False):
        """

        :param obj_type:
        :param obj_id:
        :param ext_type:
        :param filepath:
        :param filename:
        :param client:
        :return:
        """
        attachment = Attachable()
        attachment.AttachableRef = [{'EntityRef': {'type': obj_type, 'value': obj_id}}]
        attachment.FileName = filename if filename else filepath.split("/")[-1]
        attachment._FilePath = filepath
        attachment.ContentType = ext_type
        attachment.save(qb=client)
