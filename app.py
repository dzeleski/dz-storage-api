from flask import Flask, request, Response
import boto3
import datetime
import hashlib
import platform
import os

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

    if 'file' in request.files and 'name' in request.form:
        file = request.files['file']

        name = request.form['name']
        time_now = get_datetime()
        md5 = hashlib.md5(file.read()).hexdigest()
        file.seek(0, os.SEEK_END)
        size = file.tell()
        key_name = md5[:4] + "/" + time_now + "/" + name

        try:
            file.seek(0)
            s3.upload_fileobj(file, s3_bucket, key_name)
            table.put_item(
                Item={
                    'cust_id': 1,
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
    data = request.data['file']

    if data:
        pass
    else:
        pass


@app.route('/v1.0/delete_file', methods=['DELETE'])
def delete_file():
    data = request.data['file']

    if data:
        pass
    else:
        pass


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