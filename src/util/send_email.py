import base64
from email.message import EmailMessage
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

# Gmail API connnection (see email_auth_setup.py script for first-time run)
creds = Credentials.from_authorized_user_file(
    "token.json", ["https://www.googleapis.com/auth/gmail.send"]
)
service = build("gmail", "v1", credentials=creds)

# load env variables
load_dotenv()
EMAIL_DISABLED = os.getenv("EMAIL_DISABLED", "true").lower() == "true"
RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "")


def send_zoning_update_email(recent_changes):
    if EMAIL_DISABLED:
        print("Email sending disabled.")
    else:
        # generate email
        msg = EmailMessage()
        recipient_list = [email.strip() for email in RECIPIENTS.split(",") if email]
        msg["To"] = ", ".join(recipient_list)
        msg["Subject"] = "Recent Avondale Zoning Changes"
        msg.set_content("Please open with an email client that supports HTML.")
        email_html = _recent_changes_to_html(recent_changes)
        msg.add_alternative(email_html, subtype="html")

        # send email
        encoded = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": encoded}).execute()


def _recent_changes_to_html(recent_changes):
    """
    Convert a list of zoning change records into an HTML unordered list.
    Each address is a hyperlink to its record link.
    """
    if not recent_changes:
        return "<p>No recent Avondale zoning changes.</p>"

    html_items = []
    for record in recent_changes:
        status = record["subStatus"]
        if "Passed" not in status:
            date = record["introductionDate"]
            date = date.split("T")[0]
            status = f"Introduced {date}"

        html_items.append(
            f'<li><a href="{record["url"]}">{record["billAddress"]}</a> ({record["ward"]}, {record["community"]}) - {status}</li>'
        )

    html = "<html><body><ul>\n" + "\n".join(html_items) + "\n</ul></body></html>"
    return html
