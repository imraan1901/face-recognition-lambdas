import datetime
import boto3
import pymysql
import json
import io
import base64
import os


RESPONSE_200 = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                    "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "PUT",
                    },
        "body": {
            "Authenticated": False,
            "message": "Placeholder"
        }
    }

RDS_ENDPOINT = os.environ['RDS_ENDPOINT']
BUCKET = os.environ['BUCKET']
PORT = int(os.environ['PORT'])
USER = os.environ['USER']
PASSWORD = os.environ['PASSWORD']
REGION = os.environ['REGION']
DBNAME = os.environ['DBNAME']
COLLECTIONID = os.environ['COLLECTIONID']

session = boto3.Session()
rekognition_client = session.client('rekognition', REGION)
s3_client = boto3.client('s3', region_name='us-west-1')
conn = pymysql.connect(host=RDS_ENDPOINT,
                       user=USER,
                       passwd=PASSWORD,
                       port=PORT,
                       database=DBNAME)


def form_json(resp, message="Placeholder", auth=False):
    resp['body']['Authenticated'] = auth
    resp['body']['message'] = message
    json_body = json.dumps(resp['body'])
    resp['body'] = json_body


def lambda_handler(event, context):
    # In function since it would be cached otherwise
    # In function since it would be cached otherwise
    RESPONSE_500 = {
        "isBase64Encoded": False,
        "statusCode": 500,
        "headers": {"Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                    },
        "body": {
            "Authenticated": False,
            "message": "Placeholder"
        }
    }

    RESPONSE_200 = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                    },
        "body": {
            "Authenticated": False,
            "message": "Placeholder"
        }
    }
    data = event['body']
    decoded_data = base64.b64decode(data)
    image_filelike = io.BytesIO(decoded_data)

    key = event['queryStringParameters']['filename']

    try:
        # Create date folder and ignore if already created
        current_date = str(datetime.date.today())
        s3_client.put_object(Bucket=BUCKET, Key=(current_date + '/'))
        # Upload Image to S3
        s3_client.upload_fileobj(Bucket=BUCKET,
                                 Key='%s/%s' % (current_date, key),
                                 Fileobj=image_filelike)

    except Exception as e:
        print(e)
        message = e.__str__()
        form_json(RESPONSE_200, message=message)
        return RESPONSE_200

    # Get the user id of the image
    try:
        response = rekognition_client.search_faces_by_image(
            CollectionId=COLLECTIONID,
            Image={
                'S3Object': {
                    'Bucket': BUCKET,
                    'Name': '%s/%s' % (current_date, key),
                }
            },
            MaxFaces=1,
            QualityFilter='AUTO'
        )

        faces = response['FaceMatches']
        # Send Response here
        if faces == []:
            print('no faces found')
            message = "User Not Found"
            form_json(RESPONSE_200, message=message)
            return RESPONSE_200

        user_id = faces[0]['Face']['FaceId']

    except Exception as e:
        print(e)
        message = e.__str__()
        form_json(RESPONSE_500, message=message)
        return RESPONSE_500

    # If facematch found, search for user in DB
    try:
        cur = conn.cursor()
        query = f'SELECT FirstName,LastName FROM `USERS` WHERE USER_ID=\'{user_id}\';'
        cur.execute(query)
        conn.commit()
        result = cur.fetchone()

        form_json(RESPONSE_200, message=result, auth=True)
        return RESPONSE_200

    except Exception as e:
        print(e)
        message = e.__str__()
        form_json(RESPONSE_500, message=message)
        return RESPONSE_500