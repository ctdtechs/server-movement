#!/usr/bin/env python3
"""
smtp_outlook_test.py

Simple script to test sending an email via Outlook / Office365 SMTP.

Requirements:
    Uses only the Python standard library (smtplib, email) -- no pip install needed.

Notes:
  - Outlook/Office365 SMTP: smtp.office365.com, port 587, STARTTLS.
  - If your account has MFA enabled, you'll likely need an "App Password"
    instead of your normal login password (regular password will fail auth).
  - If your organization uses OAuth2-only mail (common in enterprise M365
    tenants), basic SMTP AUTH may be disabled entirely -- ask your admin,
    or you'll need OAuth2 device-code auth instead of a plain password.
"""

import smtplib
import ssl
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ------------------------------------------------------------------------- #
# CONFIG -- edit these
# ------------------------------------------------------------------------- #
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587

SENDER_EMAIL = "vn@ctdtechs.com"
SENDER_PASSWORD_B64 = "VmlnbmVzaEAwNzI2="

RECEIVER_EMAIL = "vn@ctdtechs.com"

SUBJECT = "SMTP Test Email"
BODY = "This is a test email sent via Python using Outlook SMTP."


def get_password() -> str:
    try:
        return base64.b64decode(SENDER_PASSWORD_B64).decode("utf-8")
    except Exception as e:
        raise ValueError(
            f"SENDER_PASSWORD_B64 is not valid base64: {e}. "
            "Generate it with: python3 -c \"import base64; "
            "print(base64.b64encode(b'your-password').decode())\""
        )


def send_test_email():
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg["Subject"] = SUBJECT
    msg.attach(MIMEText(BODY, "plain"))

    try:
        password = get_password()
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SENDER_EMAIL, password)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        print(f"Email sent successfully to {RECEIVER_EMAIL}")

    except ValueError as e:
        print(f"Config error: {e}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"Authentication failed: {e}")
        print("-> Check username/password. If MFA is enabled, use an App Password.")
    except smtplib.SMTPConnectError as e:
        print(f"Could not connect to {SMTP_SERVER}:{SMTP_PORT}: {e}")
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    send_test_email()