import os
import urllib.parse
import boto3
import pymysql

RDS_ENDPOINT = os.environ['RDS_ENDPOINT']
PORT = int(os.environ['PORT'])
USER = os.environ['USER']
PASSWORD = os.environ['PASSWORD']
REGION = os.environ['REGION']
DBNAME = os.environ['DBNAME']
COLLECTIONID = os.environ['COLLECTIONID']

session = boto3.Session()
rekognition_client = session.client('rekognition', REGION)
conn = pymysql.connect(host=RDS_ENDPOINT,
                       user=USER,
                       passwd=PASSWORD,
                       port=PORT,
                       database=DBNAME)


def lambda_handler(event, context):
    # Get trigger bucket and key
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Get the user first name and last name... ex. imraan_iqbal.jpg
    name = key.split('.')[0].split('_')
    first_name, last_name = name[0].lower().capitalize(), name[1].lower().capitalize()

    # Index the new user
    try:
        response = rekognition_client.index_faces(
            CollectionId=COLLECTIONID,
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key,
                }
            },
            MaxFaces=1,
            QualityFilter='AUTO'
        )
        user_id = response['FaceRecords'][0]['Face']['FaceId']

    except Exception as e:
        print(e)
        raise e

    # Add user to db
    try:
        # token = rds_client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)
        cur = conn.cursor()
        query = f'INSERT INTO USERS VALUES (\'{user_id}\', \'{last_name}\', \'{first_name}\');'
        cur.execute(query)
        conn.commit()

    except Exception as e:
        print(e)
        raise e