import json, requests
from dotenv import load_dotenv

load_dotenv()

def lambda_handler(event, context):

    message = event['Records'][0]['Sns']['Message']
    print(message)
    submission_url = message['submission_url']
    user_email = message['user_email']
    try:
        data = requests.get(submission_url)
        print(data)
        print(type(data))
    except Exception as e:
        pass
    return message