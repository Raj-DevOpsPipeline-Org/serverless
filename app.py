import base64
import json
import logging
import os
import zipfile
from io import BytesIO

import boto3
import requests
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv()

gcp_key_encoded = os.getenv("GCP_KEY")
gcp_key_decoded = base64.b64decode(gcp_key_encoded).decode("utf-8")
bucket_base_url = "https://storage.cloud.google.com"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.getenv("DYNAMODB_TABLE_NAME"))


def upload_blob(bucket_name, source_content, destination_path):
    try:
        blob = bucket_name.blob(destination_path)
        blob.upload_from_string(source_content)
        logger.info("Uploaded blob to bucket")
    except Exception as e:
        logger.error(f"Error uploading blob to bucket: {e}")
        raise e


def update_email_tracking(email, status, assignment_id, submission_count):
    try:
        table.put_item(
            Item={
                "id": f"{email}__{assignment_id}__{submission_count}",
                "email": email,
                "status": status,
                "assignment_id": assignment_id,
                "submission_count": submission_count,
            }
        )
        logger.info("Updated email tracking in DynamoDB")
    except Exception as e:
        logger.error(f"Error updating email tracking: {e}")
        raise e


def send_email(
    to_email,
    user_first_name,
    user_last_name,
    subject,
    content,
    assignment_id,
    submission_count,
):
    try:
        personalized_content = (
            f"Hello {user_first_name} {user_last_name},<br><br>{content}<br><br>Thanks"
        )
        message = Mail(
            from_email="noreply@demo.rajss.me",
            to_emails=to_email,
            subject=subject,
            html_content=personalized_content,
        )
        sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
        response = sg.send(message)
        logger.info(f"Email sent to {to_email} with status code {response.status_code}")
        update_email_tracking(to_email, "Sent", assignment_id, submission_count)
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        update_email_tracking(to_email, "Failed", assignment_id, submission_count)


def lambda_handler(event, context):
    message_str = event["Records"][0]["Sns"]["Message"]
    message = json.loads(message_str)
    submission_url = message["submission_url"]
    user_email = message["user_email"]
    user_first_name = message["user_first_name"]
    user_last_name = message["user_last_name"]
    assignment_id = message["assignment_id"]
    submission_count = message["submission_count"]

    try:
        response = requests.get(submission_url)
        file_name = os.path.basename(submission_url)
        if response.status_code == 200:
            file = BytesIO(response.content)
            if zipfile.is_zipfile(file):
                credentials_json = json.loads(gcp_key_decoded)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_json
                )
                storage_client = storage.Client(
                    credentials=credentials, project=credentials.project_id
                )
                bucket_name = storage_client.bucket(os.getenv("GCP_BUCKET_NAME"))
                upload_blob(
                    bucket_name,
                    response.content,
                    f"{user_first_name}_{user_last_name}/{assignment_id}/attempt_{submission_count}/{file_name}",
                )
                file_url = f"{bucket_base_url}/{bucket_name.name}/{user_first_name}_{user_last_name}/{assignment_id}/attempt_{submission_count}/{file_name}"

                # Send success email
                send_email(
                    user_email,
                    user_first_name,
                    user_last_name,
                    f"Assignment {assignment_id} Submission Successful",
                    f"Your submission for Assignment {assignment_id} has been successfully uploaded. <br>You can download your submission at: {file_url}",
                    assignment_id,
                    submission_count,
                )
            else:
                logger.error("Downloaded file is not a zip file")
                send_email(
                    user_email,
                    user_first_name,
                    user_last_name,
                    f"Assignment {assignment_id} Submission Failed",
                    f"Your submission for Assignment {assignment_id} failed because the submitted file ('{file_name}') is not a .zip file. Please submit the file in .zip format.",
                    assignment_id,
                    submission_count,
                )
                update_email_tracking(
                    user_email, "Failed", assignment_id, submission_count
                )

        else:
            logger.error(f"Failed to download file: HTTP {response.status_code}")
            send_email(
                user_email,
                user_first_name,
                user_last_name,
                f"Assignment {assignment_id} Submission Failed",
                f"There was an error downloading the Assignment {assignment_id} and processing it.",
                assignment_id,
                submission_count,
            )
            update_email_tracking(user_email, "Failed", assignment_id, submission_count)
    except Exception as e:
        logger.error(f"Error processing submission: {e}")
        send_email(
            user_email,
            user_first_name,
            user_last_name,
            f"Assignment {assignment_id} Submission Failed",
            "There was an error processing your submission.",
            assignment_id,
            submission_count,
        )
        update_email_tracking(user_email, "Failed", assignment_id, submission_count)
    return message
