"""
Home Automation Tools.

|pic1|
    .. |pic1| image:: ../images_source/home_tools/home_automation.jpeg
        :width: 45%
"""

import requests
import webbrowser


class Nest:

    @classmethod
    def get_auth_code(cls, device_project_id: str, client_id: str):
        webbrowser.open(
            (
                f'''
            https://nestservices.google.com/partnerconnections/
            {device_project_id}/auth?
            redirect_uri=https://www.google.com&access_type=offline
            &prompt=consent
            &client_id={client_id}
            &response_type=code
            &scope=https://www.googleapis.com/auth/sdm.service'''
                    .replace("\n", "")
                    .replace(" ", "")
                    .strip()
            )
        )

    @classmethod
    def get_access_refresh_tokens(cls, client_id: str, client_secret: str, auth_code: str):
        r = requests.post(
            (
                f'''
            https://www.googleapis.com/oauth2/v4/token?
            client_id={client_id}
            &client_secret={client_secret}
            &code={auth_code}
            &grant_type=authorization_code
            &redirect_uri=https://www.google.com'''
                    .replace("\n", "")
                    .replace(" ", "")
                    .strip()
            )
        )
        return r

    @classmethod
    def refresh_access_token(cls, client_id: str, client_secret: str, refresh_token: str):
        r = requests.post(
            (
                f'''
            https://www.googleapis.com/oauth2/v4/token?
            client_id={client_id}
            &client_secret={client_secret}
            &refresh_token={refresh_token}
            &grant_type=refresh_token'''
                    .replace("\n", "")
                    .replace(" ", "")
                    .strip()
            )
        )
        return r

    @classmethod
    def get_devices(cls, project_id: str, access_token: str):
        r = requests.get(
            f'''https://smartdevicemanagement.googleapis.com/v1/enterprises/{project_id}/devices''',
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
        )

        return r

    @classmethod
    def get_structures(cls, project_id: str, access_token: str):
        r = requests.get(
            f'''https://smartdevicemanagement.googleapis.com/v1/enterprises/{project_id}/structures''',
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
        )

        return r

    @classmethod
    def get_device(cls, project_id: str, access_token: str, device_id: str):
        r = requests.get(
            f'''https://smartdevicemanagement.googleapis.com/v1/enterprises/{project_id}/devices/{device_id}''',
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
        )

        return r

    @classmethod
    def execute_device_command(cls, project_id: str, access_token: str, device_id: str, command_dict: dict):
        r = requests.post(
            f'''https://smartdevicemanagement.googleapis.com/v1/enterprises/{project_id}/devices/{device_id}:executeCommand''',
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            },
            json=command_dict
        )

        return r
