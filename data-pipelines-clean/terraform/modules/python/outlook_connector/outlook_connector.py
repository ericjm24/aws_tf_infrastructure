import boto3
from O365 import Account
from datetime import datetime as dt
import os
from json import loads
import base64
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Retrieving environment variables
OUTLOOK_CREDENTIALS_SECRET = os.environ.get("OUTLOOK_CREDENTIALS_SECRET", "outlook_creds")
S3_BUCKET = os.environ.get("S3_LANDING_BUCKET", "CLIENTNAME-ds-landing-bucket")
ENV = os.environ.get("ENVIRONMENT", "dev")

# Get Outlook Credentials
def get_secret(secret_name):
    secretmanager = boto3.client('secretsmanager')
    secret = secretmanager.get_secret_value(SecretId=secret_name)
    try:
        out = loads(secret['SecretString'])
    except Exception:
        out = {}
    return out



# Helper function to write outlook attachments to s3
s3 = boto3.client('s3')
def save_attachment_to_s3(att, ts):
    if (not att.name) or (not att.content):
        return None
    key = f"{ENV}/outlook/{ts.strftime('%Y/%m/%d')}/{att.name}"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=base64.b64decode(att.content)
        )
    except:
        return None
    else:
        return key


def _is_in(a,b):
    if type(b) == list:
        return a in b
    else:
        return a==b

def save_all_attachments_to_s3(start_date, end_date, attachment_type=None, sender_address=None, subject_line=None):
    outlook_creds = get_secret(OUTLOOK_CREDENTIALS_SECRET)
    credentials = (outlook_creds['OUTLOOK_CLIENT_ID'], outlook_creds['OUTLOOK_SECRET_KEY'])
    account = Account(credentials, auth_flow_type='credentials', tenant_id=outlook_creds['OUTLOOK_TENANT_ID'])

    if account.authenticate():
        logger.info('Outlook login authenticated')

    mailbox = account.mailbox(outlook_creds['OUTLOOK_EMAIL_ADDR'])

    query = mailbox.new_query().on_attribute('created_date_time').greater(start_date)
    query = query.chain('and').on_attribute('created_date_time').less(end_date)

    if sender_address:
        query = query.chain('and').on_attribute('from').contains(sender_address)
    if subject_line:
        query = query.chain('and').on_attribute('subject').contains(subject_line)

    if not attachment_type:
        filetype_filter = (".csv", ".tsv", ".xls", ".xlsx", ".txt", ".zip")
    elif type(attachment_type) == list:
        filetype_filter = tuple(attachment_type)
    else:
        filetype_filter = (attachment_type,)
    
    logger.info(f"Email query: {query}")
    attachment_keys = []
    for message in mailbox.get_messages(
        query=query,
        download_attachments=True,
        limit = 10000000,
        batch = 15
        ):
        if message.has_attachments:
            logger.info(f"Retreiving attachments for email from {message.sender} with subject '{message.subject}' sent on {message.sent}.")
            for att in message.attachments:
                if att.name.endswith(filetype_filter):
                    key = save_attachment_to_s3(att, message.created or message.received or dt.now())
                    if key:
                       logger.info(f"Successfully saved {att.name} to s3://{S3_BUCKET}/{key}")
                       attachment_keys.append(f"{S3_BUCKET}/{key}")
                    else:
                       logger.info(f"Failed to save {att.name}")
    return attachment_keys

def lambda_handler(event, context):
    os.chdir("/tmp")
    start_date = dt.fromisoformat(event.get('start_date', '1900-01-01'))
    end_date = dt.fromisoformat(event.get('end_date', '9999-12-31'))
    sender_address = event.get('sender', None)
    subject_line = event.get("subject", None)
    attachment_type = event.get("attachment_type", None)
    attachment_keys = save_all_attachments_to_s3(
        start_date=start_date,
        end_date=end_date,
        attachment_type=attachment_type,
        sender_address=sender_address,
        subject_line=subject_line
        )
    return list(set(attachment_keys))
                