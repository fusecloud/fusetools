"""
Communication services.

|pic1| |pic2| |pic3|
    .. |pic1| image:: ../images_source/comm_tools/twilio1.png
        :width: 30%
    .. |pic2| image:: ../images_source/comm_tools/whatsapp1.png
        :width: 20%
    .. |pic3| image:: ../images_source/comm_tools/sendgrid1.png
        :width: 30%
"""

from twilio.rest import Client
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


class Twilio:
    """
    Twilio's API infrastructure.

    .. image:: ../images_source/comm_tools/twilio1.png
    """

    @classmethod
    def send_message(cls, body, from_number, to_number, account_sid, auth_token, media_url=False):
        """
        Sends an SMS/MMS message.

        :param body: Message body (text).
        :param from_number: Phone number to send the message from.
        :param to_number: Phone number to send the message to.
        :param account_sid: Twilio developer account ID.
        :param auth_token: Twilio developer authorization token.
        :param media_url: Media URL to pass for MMS messages.
        :return: API message sent ID.
        """
        client = Client(account_sid, auth_token)

        if media_url:
            message = (
                client
                    .messages
                    .create(
                    body=body,
                    from_=from_number,
                    media_url=media_url,
                    to=to_number
                )
            )
        else:
            message = (
                client
                    .messages
                    .create(
                    body=body,
                    from_=from_number,
                    to=to_number
                )
            )

        return message.sid

    @classmethod
    def get_messages(cls, account_sid, auth_token):
        """
        Retrieves a list of message attributes for a given Twilio developer account.

        :param account_sid: Twilio developer account ID.
        :param auth_token: Twilio developer authorization token.
        :return: Pandas DataFrame of message attributes for a given Twilio developer account.
        """
        client = Client(account_sid, auth_token)
        messages = client.messages.list()

        sent_time_list = []
        sid_list = []
        direction_list = []
        msg_list = []
        img_list = []
        for record in messages:
            sid_list.append(record.sid)
            direction_list.append(record.direction)
            msg_list.append(record.body)
            sent_time_list.append(record.date_sent.strftime('%Y%m%d%H24%M%S'))
            try:
                for img in record.media.list():
                    img_list.append(img.fetch())
            except:
                img_list.append(None)

        msg_df = \
            pd.DataFrame({
                "sent_time_list": sent_time_list,
                "sid_list": sid_list,
                "direction_list": direction_list,
                "msg_list": msg_list,
                # "img_list":img_list
            })
        return msg_df


class WhatsApp:
    """
    Twilio's WhatsApp API infrastructure.

    .. image:: ../images_source/comm_tools/whatsapp1.png
    """

    @classmethod
    def send_message(cls, body, from_number, to_number, account_sid, auth_token, media_url=False):
        """
        Sends a WhatsApp message.

        :param body: Message body (text).
        :param from_number: Phone number to send the message from.
        :param to_number: Phone number to send the message to.
        :param account_sid: Twilio developer account ID.
        :param auth_token: Twilio developer authorization token.
        :param media_url: Media URL to pass for MMS messages.
        :return: API message sent ID.
        """
        client = Client(account_sid, auth_token)
        if media_url:
            message = client.messages.create(
                from_=from_number,
                to=to_number,
                body=body,
                media_url=media_url
            )
        else:
            message = client.messages.create(
                from_=from_number,
                to=to_number,
                body=body
            )

        return message.sid


class SendGrid:
    """
    SendGrid's API infrastructure.

    .. image:: ../images/sendgrid1.png
    """

    @classmethod
    def send_email(cls, api_key, from_email, to_emails, subject, html_content):
        """
        Sends an email via SendGrid API.

        :param api_key: SendGrid API Key.
        :param from_email: Email to send the message from.
        :param to_emails: Email to send the message to.
        :param subject: Email subject line.
        :param html_content: HTML content to send in email.
        :return: JSON API call response.
        """
        message = Mail(
            from_email=from_email,
            to_emails=to_emails,
            subject=subject,
            html_content=html_content)
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)

        return response
