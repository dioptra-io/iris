from email.message import EmailMessage

import aiosmtplib


class Mail(object):
    def __init__(self, settings) -> None:
        self.settings = settings

    async def send(self, receiver: str) -> None:
        """Send an email."""
        if not self.settings.MAIL_ENABLE:
            return

        message = EmailMessage()
        message["From"] = self.settings.MAIL_SENDER
        message["To"] = receiver
        message["Subject"] = self.settings.MAIL_SUBJECT
        message.set_content(self.settings.MAIL_BODY)

        await aiosmtplib.send(
            message, hostname=self.settings.MAIL_SERVER, port=self.settings.MAIL_PORT
        )
