from flask import Flask, request, Response, send_file
import boto3
import datetime
import hashlib
import platform
import os
import io

app = Flask(__name__)

# Set resource names
s3_bucket = 'zeleski-file-storage'
dynamodb_table = 'zeleski-file-storage-index'

# Get the service clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table(dynamodb_table)


def get_datetime():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%dT%H:%M")


@app.route('/v1.0/upload_file', methods=['POST'])
def upload_file():

    if 'file' in request.files and 'name' in request.form and 'cust_id' in request.form:
        # Get required data
        file = request.files['file']
        name = request.form['name']
        cust_id = int(request.form['cust_id'])
        # Set current time
        time_now = get_datetime()
        # Get hash of file
        md5 = hashlib.md5(file.read()).hexdigest()
        # Get file size
        file.seek(0, os.SEEK_END)
        size = file.tell()
        # Set key name
        key_name = md5[:4] + "/" + time_now + "/" + name

        try:
            # Seek to front of file for upload
            file.seek(0)
            # Upload file to S3, write data to Dynamo for metadata
            s3.upload_fileobj(file, s3_bucket, key_name)
            table.put_item(
                Item={
                    'cust_id': cust_id,
                    'date_time': time_now,
                    'file_size': size,
                    'key': key_name,
                    'file_name': name,
                    'server_id': platform.node(),
                }
            )
        except Exception as e:
            return e

        return Response('{"Created": true}', status=201,
                        mimetype='application/json')

    else:
        return Response('{"BadData": true}', status=400,
                        mimetype='application/json')


@app.route('/v1.0/download_file', methods=['POST'])
def download_file():
    json_data = request.get_json()

    if 'file' in json_data:
        # Get required data
        key = json_data['file']
        # Create file IO object
        file = io.BytesIO()

        try:
            # Download requested file
            s3.download_fileobj(s3_bucket, key, file)
        except Exception as e:
            return e

        # Seek to beginning of file to send
        file.seek(0)

        return send_file(file, mimetype='application/octet-stream')
    else:
        return Response('{"BadData": true}', status=400,
                        mimetype='application/json')


@app.route('/v1.0/delete_file', methods=['DELETE'])
def delete_file():
    json_data = request.get_json()

    s3_resource = boto3.resource('s3')

    if 'file' in json_data and 'cust_id' in json_data:
        # Get required data
        key = json_data['file']
        cust_id = json_data['cust_id']

        # s3_obj = s3_resource.Object(s3_bucket, key)
        # s3_obj.delete()
        key_split = key.split('/')

        table.delete_item(
            Key={
                'date_time': {
                    'S': key_split[1],
                },
                'file_name': {
                    'S': key_split[2],
                },
                'cust_id': {
                    'N': cust_id
                }
            }
        )
        return Response('{"Deleted": true}', status=202,
                        mimetype='application/json')
    else:
        return Response('{"BadData": true}', status=400,
                        mimetype='application/json')


@app.route('/v1.0/get_metadata', methods=['POST'])
def get_metadata():
    data = request.data['query']

    if data:
        pass
    else:
        pass


@app.route('/v1.0/health', methods=['GET'])
def get_health():
    pass
