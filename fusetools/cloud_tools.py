"""
Cloud services.

|pic1| |pic2|
    .. |pic1| image:: ../images_source/cloud_tools/aws1.png
        :width: 25%
    .. |pic2| image:: ../images_source/cloud_tools/firebase1.png
        :width: 45%

"""

import json
import io
import os
import sys
from datetime import datetime
from typing import List, Optional
from botocore.config import Config
import aiobotocore
import asyncio
import boto3
from aiobotocore.session import get_session

import pandas as pd
from io import StringIO
import time

import firebase_admin
from botocore.exceptions import ClientError
from firebase_admin import credentials, firestore, db, storage
from gcloud import storage as storage_gcp
from oauth2client.service_account import ServiceAccountCredentials

# rsa signer deps
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from botocore.signers import CloudFrontSigner


class Firebase:
    """
    Functions for interacting with Firebase infrastructure, including: Firestore, Storage

    .. image:: ../images_source/cloud_tools/firebase1.png
        :width: 45%

    """

    @classmethod
    def initapp(cls, db_url, storage_url, token_path):
        """
        Initializes an application instance for your Firebase project.

        :param db_url: Firestore instance URL for your project.
        :param storage_url: Storage instance URL for your project.
        :param token_path: Local path of your Firebase credentials token.
        :return: Authenticated application instance for your Firebase project.
        """
        cred = credentials.Certificate(token_path)
        default_app = firebase_admin.initialize_app(
            cred,
            {
                'databaseURL': db_url,
                'storageBucket': storage_url
            }
        )
        return default_app

    @classmethod
    def load_delete_firestore(
            cls,
            obj_types,
            obj_names,
            method,
            d=False,
            constraint_key=False,
            constraint_val=False,
            delete_object_type=False,
            upsert_doc_name=False,
            document_delete_batch_size=False

    ):

        """
        Executes an Insert, Update or Delete operation on a Firestore database object.

        :param parent_object_type: Type of object containing operation target.
        :param parent_collection_name: Name of parent collection.
        :param method: Operation type: "insert", "update", "delete".
        :param d: Data object (required for insert, update)
        :param constraint_key: Name of field to constraint updates and deletes to.
        :param constraint_val: Value of field to constraint updates and deletes to.
        :param parent_sub_collection_name: Name of parent sub collection if object lives inside sub collection.
        :param parent_document_name: Name of parent document if object lives inside a document (sub collection).
        :param upsert: Specifies whether to insert target data if no data is found to update.
        :param document_delete_batch_size: Limit of documents to delete per function call to avoid Memory errors.
        :return: JSON response for API call.
        """

        database = firestore.client()

        # build object reference
        obj = database
        for t, n in (zip(obj_types, obj_names)):
            print(t)
            if t == "collection":
                obj = obj.collection(n)
            elif t == "document":
                obj = obj.document(n)

        if method == "insert":
            ret = obj.set(d)

            return ret

        elif method == "update":
            if constraint_key and constraint_val:
                print("update with key constraint...")

                results = obj.where(constraint_key, '==', constraint_val).get()

                if len(results) > 0:
                    print(f"updating {len(results)} documents that met key constraint...")

                    ret = []
                    for idx, item in enumerate(results):
                        print(f"updating document index: {idx}")
                        doc = obj.document(item.id)
                        ret_ = doc.set(d)
                        ret.append(ret_)

                    return ret

                else:

                    print("no key constraint results found...")
                    if upsert_doc_name:
                        print("upsert triggered...inserting document...")
                        ret = (obj.add(upsert_doc_name).set(d))
                        return ret

            else:

                print("no key constraint, running straight update...")
                ret = (obj.set(d))

                return ret

        elif method == "merge":
            obj.update(d)

        elif method == "delete":

            if delete_object_type == "document":

                print(f"deleting document...")
                ret = obj.delete()
                return ret

            elif delete_object_type == "field":

                print(f"deleting document field: {constraint_key}...")

                ret = obj.update({constraint_key: firestore.DELETE_FIELD})
                return ret

            elif delete_object_type in ['collection', 'sub_collection']:

                if constraint_key and constraint_val:
                    print("deleting documents with key constraint...")

                    results = obj.where(constraint_key, '==', constraint_val).get()
                    if len(results) > 0:
                        docs = []
                        for idx, item in enumerate(results):
                            doc = obj.document(item.id)
                            docs.append(doc)
                    else:
                        print("No documents with key constraint found...exiting...")
                        return

                else:
                    if document_delete_batch_size:
                        results = obj.limit(document_delete_batch_size).get()
                    else:
                        results = obj.get()

                if len(results) == 0:
                    print("No documents found...exiting...")
                    return

                docs = []
                for idx, item in enumerate(results):
                    doc = obj.document(item.id)
                    docs.append(doc)

                deleted = 0
                for doc in docs:
                    print(f'Deleting doc: {doc.id}')
                    doc.delete()
                    deleted += 1

                # https://firebase.google.com/docs/firestore/manage-data/delete-data#python_2
                if document_delete_batch_size and \
                        deleted >= document_delete_batch_size:
                    return Firebase.load_delete_firestore(
                        # parent_object_type=parent_object_type,
                        # parent_collection_name=parent_collection_name,
                        method=method,
                        d=d,
                        constraint_key=constraint_key,
                        constraint_val=constraint_val,
                        # parent_sub_collection_name=parent_sub_collection_name,
                        # parent_document_name=parent_document_name,
                        delete_object_type=False,
                        upsert_doc_name=upsert_doc_name,
                        document_delete_batch_size=document_delete_batch_size
                    )

    @classmethod
    def file_to_bucket(cls, blob_name, file_path, content_type=None, bucket_name=False, metadata_d=False):
        """
        Sends a local file to a storage bucket.

        :param blob_name: Name for file to have after being loaded to storage bucket.
        :param file_path: Filepath of file to load.
        :param content_type: Type of file, such as 'image/png'.
        :param bucket_name: Name of bucket to send file to.
        :param metadata_d: Metadata dictionary to attach to a file (optional).
        :return:
        """

        # https://stackoverflow.com/questions/52883534/firebase-storage-upload-file-python
        # https://stackoverflow.com/questions/60080133/firebase-storage-error-with-uploading-png-image-via-python-google-cloud-storag

        # Create new dictionary with the metadata
        metadata = metadata_d

        # Set metadata to blob
        if bucket_name:
            bucket = storage.bucket(bucket_name)
        else:
            bucket = storage.bucket()

        blob = bucket.blob(blob_name)
        blob.metadata = metadata

        with open(file_path, 'rb') as my_file:
            blob.upload_from_file(my_file, content_type=content_type)

    @classmethod
    def list_bucket_objects(cls, bucket_name=False):
        """
        Lists objects in a bucket.

        :param bucket_name: Name of bucket.
        :return: List of bucket objects.
        """
        if bucket_name:
            bucket = storage.bucket(bucket_name)
        else:
            bucket = storage.bucket()
        blobs = list(bucket.list_blobs())

        return blobs

    @classmethod
    def delete_bucket_object(cls, blob_name, bucket_name=False):
        """
        Deletes a bucket object.

        :param blob_name: Name of bucket object.
        :param bucket_name: Name of bucket.
        :return: Confirmation of bucket object that was deleted.
        """
        if bucket_name:
            bucket = storage.bucket(bucket_name)
        else:
            bucket = storage.bucket()
        blob = bucket.blob(blob_name)
        blob.delete()

        print("Blob {} deleted.".format(blob_name))


class AWS:
    """
    Functions for interacting with AWS infrastructure, including: S3, Redshift, DynamoDB

    .. image:: ../images_source/cloud_tools/aws1.png
        :width: 45%

    """
    @classmethod
    def get_secret_manager(cls, secret_name: str, pub: str, sec: str, region_name: str) -> str:
        # Create a Secrets Manager client
        session = boto3.session.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        return get_secret_value_response['SecretString']


    @classmethod
    def list_cloudwatch_rules(cls, pub, sec, region_name):
        client = boto3.client(
            'events',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name
        )

        rules = client.list_rules()

        # extract rule names
        rule_names = [x.get("Name") for x in rules.get("Rules")]

        # find targets for rules
        for idx, rule in enumerate(rule_names):
            try:
                ret = client.list_targets_by_rule(Rule=rule)
            except Exception as e:
                print(str(e))
                time.sleep(3)
                print("Trying again...")
                try:
                    ret = client.list_targets_by_rule(Rule=rule)
                except Exception as e:
                    print(str(e))
                    pass
            # update the rule_names object with the ret json
            rules.get('Rules')[idx].update(ret)

        return rules

    @classmethod
    def list_lambda_functions(cls, pub, sec, region_name):

        client = boto3.client(
            'lambda',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
        )

        function_list = \
            client.list_functions(
                # MasterRegion='string',
                FunctionVersion='ALL',
                # Marker='string',
                MaxItems=123
            )

        # extract rule names
        function_names = [x.get("FunctionName") for x in function_list.get("Functions")]

        # find targets for rules
        for idx, function in enumerate(function_names):
            try:
                ret = client.get_function(FunctionName=function)
            except Exception as e:
                print(str(e))
                time.sleep(3)
                print("Trying again...")
                try:
                    ret = client.get_function(FunctionName=function)
                except Exception as e:
                    print(str(e))
                    pass
            # update the rule_names object with the ret json
            function_list.get('Functions')[idx].update(ret)

        return function_list

    @classmethod
    def query_cloudwatch_logs(cls, pub, sec, region_name, log_group_name, start_datetime, end_datetime, query):
        client = boto3.client(
            'logs',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
        )

        start_query_response = client.start_query(
            logGroupName=log_group_name,
            startTime=start_datetime,
            endTime=end_datetime,
            queryString=query,
        )

        query_id = start_query_response['queryId']

        response = None

        while response == None or response['status'] == 'Running':
            print('Waiting for query to complete ...')
            time.sleep(1)
            response = client.get_query_results(
                queryId=query_id
            )

        if len(response.get("results")) == 0:
            return

        for idx, row in enumerate(response.get("results")):

            df = pd.DataFrame(row).T.reset_index()
            df.columns = list(df.iloc[0].values)
            df = df.iloc[1:].reset_index(drop=True)

            if idx == 0:
                df_all = df.copy()
            else:
                df_all = pd.concat([df_all, df])

        return df_all.reset_index(drop=True)

    @classmethod
    def create_s3_bucket(cls, pub, sec, bucket_name):
        """
        Creates an S3 bucket.

        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param bucket_name: Name of new S3 bucket.
        :return: JSON response for API call.
        """
        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')
        response = s3.create_bucket(Bucket=bucket_name)
        return response

    @classmethod
    def create_s3_presigned_url(cls, pub, sec, region_name,
                                bucket_name,
                                key_name,
                                client_method="get_object",
                                expry_seconds=3600
                                ):
        """
        Creates a pre-signed URL for an S3 bucket object.

        :param pub:
        :param sec:
        :param region_name:
        :param bucket_name:
        :param key_name:
        :param client_method:
        :param expry_seconds:
        :return:
        """
        s3_client = boto3.client(
            's3',
            region_name=region_name,
            config=Config(signature_version='s3v4'),
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        url = s3_client.generate_presigned_url(
            client_method,
            Params={
                'Bucket': bucket_name,
                'Key': key_name
            },
            ExpiresIn=expry_seconds)

        return url

    @classmethod
    def s3_to_s3(cls, pub, sec, bucket_from, bucket_to, from_key, to_key):
        """
        Transfers an S3 data object to another S3 bucket.

        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param bucket_from: S3 bucket to transfer object from.
        :param bucket_to: S3 bucket to transfer object to.
        :param from_key: Path of S3 data object to transfer.
        :param to_key: Destination path of transferred S3 data object.
        :return: JSON response for API call.
        """
        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')
        copy_source = {
            'Bucket': bucket_from,
            'Key': from_key
        }
        bucket = s3.Bucket(bucket_to)
        response = bucket.copy(copy_source, to_key)
        return response

    @classmethod
    def delete_s3_object(cls, pub, sec, bucket, obj_list=False, delete_all=False):
        """
        Deletes a list of S3 bucket objects.

        :param delete_all:
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param bucket: S3 bucket name.
        :param obj_list: List of bucket objects to be deleted, in the following format: [{"Key": "your_file_name_key"}].
        :return: JSON response for API call.
        """

        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')
        bucket = s3.Bucket(bucket)
        # can only delete 1k objects at a time so do loop
        if obj_list:
            print("Deleting objects")
            min_idx = 0
            max_idx = 1000
            while len(obj_list[min_idx:max_idx]) > 0:
                response = bucket.delete_objects(Delete={'Objects': [{"Key": x} for x in obj_list[min_idx:max_idx]]})
                print(response)
                min_idx += 1000
                max_idx += 1000
        elif delete_all:
            bucket.objects.all().delete()

    @classmethod
    def df_to_s3(cls, df, object_name, bucket, pub, sec, sep, header=False):
        """
        Sends a Pandas DataFrame to S3.

        :param df: Pandas DataFrame.
        :param object_name: S3 bucket object name to save DataFrame to.
        :param bucket: S3 bucket name.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param sep: Delimiter for Pandas DataFrame.
        :param header: Indicates if Pandas DataFrame should be sent with a header.
        :return: JSON response for API call.
        """

        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')
        csv_buffer = StringIO()

        df.to_csv(csv_buffer
                  , index=False
                  , header=header
                  , encoding='utf-8'
                  , sep=sep)

        response = s3.Object(bucket, object_name).put(Body=csv_buffer.getvalue())
        return response

    @classmethod
    def s3_to_df(cls, object_name, bucket, pub, sec, sep, header=False):
        """
        Saves data from S3 to a Pandas DataFrame.

        :param object_name: S3 data object to save to Pandas.
        :param bucket: S3 Bucket name.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param sep: Delimiter for S3 data object to convert to a DataFrame.
        :param header: Indicates if the S3 data object contains a header to use for the DataFrame.
        :return: Pandas DataFrame of S3 data object.
        """

        client = boto3.client(
            's3',
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        obj = client.get_object(Bucket=bucket, Key=object_name)
        body = obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_string), sep=sep, header=header)

        return df

    @classmethod
    def s3_to_rs(cls, cursor, tbl_name, bucket_path, pub, sec, delimiter, updates_off=False, exec=True):
        """
        Transfers an S3 data object into a Redshift relational database table.

        :param cursor: Redshift cursor object.
        :param tbl_name: Redshift table name.
        :param bucket_path: Bucket path of S3 data object to copy.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param delimiter: Delimiter for S3 data object to convert to a Redshift table.
        :param updates_off: Redshift Insert/Upsert performance tuning parameter.
        :param exec: Specifies whether to execute the command if True or return the command's text if false.
        :return: Log of data object transfer (from bucket object to table).
        """
        sql = \
            f'''copy {tbl_name} 
            from '{bucket_path}' 
            credentials 'aws_access_key_id={pub};aws_secret_access_key={sec}' 
            delimiter '{delimiter}' 
            IGNOREHEADER 1'''.replace("\n", " ")

        # https://www.flydata.com/blog/how-to-improve-performance-upsert-amazon-redshift/
        if updates_off:
            sql = sql + "COMPUPDATE OFF STATUPDATE OFF"

        if exec:
            cursor.execute(sql)
            cursor.execute("commit;")
            print(f'''loaded data from {bucket_path} to {tbl_name}''')

        else:
            return sql

    @classmethod
    def rs_to_s3(cls, cursor, sql, bucket_path, delimiter, pub, sec, exec=True, zip_file=False):
        # todo: test zip functionality
        """
        Transfers a Redshift query result to a data object on S3.

        :param cursor: Redshift cursor object.
        :param sql: Redshift query's SQL statement.
        :param bucket_path: Bucket path of S3 data object to save data to.
        :param delimiter: Delimiter for S3 data object when converted from Redshift query result.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param exec: Specifies whether to execute the command if True or return the command's text if false.
        :return: Log of data object transfer (from query to bucket object).
        """

        sql_exec = f'''unload ('{sql}') 
                    to '{bucket_path}{".giz" if zip_file else ""}' 
                    credentials 'aws_access_key_id={pub};aws_secret_access_key={sec}' 
                    delimiter '{delimiter}' 
                    header 
                    {"GZIP" if zip_file else ""}
                    allowoverwrite 
                    parallel off
                    '''

        if exec:
            cursor.execute(sql_exec)
            cursor.execute("commit")
            print(f'''loaded data to {bucket_path} from query: {sql}''')
        else:
            return sql_exec

    @classmethod
    def s3_to_file(cls, bucket, object_name, folder_file, pub, sec):
        """
        Saves an S3 object to a local filepath.

        :param bucket: Name of S3 bucket.
        :param object_name: Bucket path of S3 object to save.
        :param folder_file: Name of local filepath.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :return: Log of data object transfer (from bucket object to filepath).
        """

        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')
        response = s3.Bucket(bucket).download_file(object_name, folder_file)
        print(f'''loaded data to {folder_file} from: {object_name}''')
        return response

    @classmethod
    def file_to_s3(cls, folder_file, bucket, object_name, pub, sec,
                   metadata_d=None,
                   public_file=None,
                   content_type=None):
        """
        Uploads a local file to an S3 bucket.

        :param content_type:
        :param folder_file: Filepath of local file.
        :param bucket: Name of S3 bucket.
        :param object_name: Bucket path of S3 object to upload to.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param metadata_d: Dictionary of metadata to add to uploaded S3 bucket object.
        :param public_file: Switch to make S3 bucket object open to public access.
        :return: Log of data object transfer (from filepath to bucket object).
        """
        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')

        extra_args_d = {}

        if metadata_d:
            extra_args_d.update({"Metadata": metadata_d})

        if content_type:
            extra_args_d.update({"ContentType": content_type})

        if public_file:
            extra_args_d.update({"ACL": 'public-read'})

        response = \
            (
                s3.Bucket(bucket)
                    .upload_file(
                    folder_file,
                    object_name,
                    ExtraArgs=extra_args_d
                )
            )

        print(f'''loaded data to {object_name} from: {folder_file}''')
        return response

    @classmethod
    def bytes_to_s3(cls, binary_data, bucket, object_name, pub, sec,
                    metadata_d=None,
                    public_file=None,
                    content_type=None):
        """
        Uploads a local file to an S3 bucket.

        :param binary_data:
        :param content_type:
        :param folder_file: Filepath of local file.
        :param bucket: Name of S3 bucket.
        :param object_name: Bucket path of S3 object to upload to.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param metadata_d: Dictionary of metadata to add to uploaded S3 bucket object.
        :param public_file: Switch to make S3 bucket object open to public access.
        :return: Log of data object transfer (from filepath to bucket object).
        """
        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')

        extra_args_d = {}

        if metadata_d:
            extra_args_d.update({"Metadata": metadata_d})

        if content_type:
            extra_args_d.update({"ContentType": content_type})

        if public_file:
            extra_args_d.update({"ACL": 'public-read'})

        response = s3.Bucket(bucket).upload_fileobj(
            io.BytesIO(binary_data), object_name,
            ExtraArgs=extra_args_d)

        print(f'''loaded data to {object_name} from bytes in memory''')
        return response

    @classmethod
    def bytes_to_s3_2(cls, binary_data, bucket, object_name, pub, sec,
                    metadata_d=None,
                    public_file=None,
                    content_type=None):
        """
        Uploads a local file to an S3 bucket.

        :param binary_data:
        :param content_type:
        :param folder_file: Filepath of local file.
        :param bucket: Name of S3 bucket.
        :param object_name: Bucket path of S3 object to upload to.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param metadata_d: Dictionary of metadata to add to uploaded S3 bucket object.
        :param public_file: Switch to make S3 bucket object open to public access.
        :return: Log of data object transfer (from filepath to bucket object).
        """
        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')

        extra_args_d = {}

        if metadata_d:
            extra_args_d.update({"Metadata": metadata_d})

        if content_type:
            extra_args_d.update({"ContentType": content_type})

        if public_file:
            extra_args_d.update({"ACL": 'public-read'})

        boto_test_bucket = s3.Bucket(bucket)
        import io
        buf = io.BytesIO()
        buf.write(binary_data)
        buf.seek(0)

        response = boto_test_bucket.upload_fileobj(buf, object_name, ExtraArgs=extra_args_d)

        print(f'''loaded data to {object_name} from bytes in memory''')
        return response

    @classmethod
    def s3_list_files(cls, bucket, pub, sec, search_str=False):
        """
        Returns a list of objects in an S3 bucket.

        :param bucket: Name of S3 bucket.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param search_str: Search string to limit results by.
        :return: Dictionary of bucket object file names, creation times and metadata.
        """

        session = boto3.Session(
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        s3 = session.resource('s3')
        s3c = session.client('s3')
        bucket = s3.Bucket(name=bucket)

        files = []
        times = []
        metadata = []
        for my_bucket_object in bucket.objects.all():
            print(my_bucket_object.key)
            if search_str:
                if search_str in my_bucket_object.key:
                    files.append(str(my_bucket_object.key))
                    times.append(str(my_bucket_object.last_modified))
                    metadata.append(
                        s3c.head_object(Bucket=bucket.name,
                                        Key=my_bucket_object.key).get("Metadata"))

            else:
                files.append(str(my_bucket_object.key))
                times.append(str(my_bucket_object.last_modified))
                metadata.append(s3c.head_object(Bucket=bucket.name,
                                                Key=my_bucket_object.key).get("Metadata"))

        df = pd.DataFrame({
            "files": files,
            "times": times,
            "metadata": metadata
        })
        return df

    @classmethod
    def describe_dynamo_tbl(cls, pub, sec, region_name, tbl_name, endpoint_url=None):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param tbl_name:
        :param endpoint_url:
        :return:
        """
        dynamodb = boto3.client(
            'dynamodb',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        res = dynamodb.describe_table(TableName=tbl_name)

        return res

    @classmethod
    def delete_dynamo_tbl(cls, pub, sec, region_name, tbl_name, endpoint_url=None):
        """
        Deletes a DynamoDB table.

        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param region_name: Region name where the Dynamo table is.
        :param tbl_name: Name of Dynamo table.
        :return: JSON response for API call.
        """

        dynamodb = boto3.client(
            'dynamodb',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        table = dynamodb.delete_table(TableName=tbl_name)

        return table

    @classmethod
    def dynamo_delete_items(cls, pub, sec, region_name, tbl_name, key_name,
                            delete_field_name: Optional[str] = None,
                            delete_field_val: Optional[str] = None):

        ddb = boto3.resource(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec
        )

        try:
            flag = False
            table = ddb.Table(tbl_name)
            scan = table.scan()
            while not flag:
                with table.batch_writer() as batch:
                    for each in scan['Items']:

                        if delete_field_name and delete_field_val:
                            if each[delete_field_name] == delete_field_val:
                                batch.delete_item(
                                    Key={
                                        key_name: each[key_name]
                                    }
                                )

                        else:
                            batch.delete_item(
                                Key={
                                    key_name: each[key_name]
                                }
                            )

                    flag = True
        except Exception as e:
            print(e)

    @classmethod
    def make_dynamo_tbl(cls, pub: str, sec: str, region_name: str, tbl_name: str,
                        key_schema, attribute_definitions,
                        provisioned_throughput, endpoint_url=None):
        """
        Creates a Dynamo table.

        :param endpoint_url: Endpoint if not on AWS (defaults to None)
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param region_name: Region name where the Dynamo table is.
        :param tbl_name: Name of Dynamo table.
        :param key_schema: List of table key objects containing attribute names and key types.
        :param attribute_definitions: List of attribute definition objects for the keys, including attribute names and data types.
        :param provisioned_throughput: Read & Write capacity for table.
        :return: JSON response for API call.
        """

        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        table = dynamodb.create_table(
            TableName=tbl_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput
        )

        return table

    @classmethod
    def load_dynamo(cls, pub, sec, region_name, tbl_name, d, load_type=False, endpoint_url=None,
                    condition_expression=None):
        """
        Inserts either a Dictionary or Pandas DataFrame into DynamoDB.

        :param condition_expression:
        :param endpoint_url: Endpoint if not on AWS (defaults to None)
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param region_name: Region name for DynamoDB table.
        :param tbl_name: Name of DynamoDB table.
        :param d: Dictionary or Pandas DataFrame to be loaded.
        :return: JSON response for API call.
        """

        # print(f'''Loading a {type(d)} to {tbl_name}''')

        dynamodb = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            endpoint_url=endpoint_url
        )

        if isinstance(d, dict):

            # dictionary format should follow this example:
            # {'CountryId': {'S': '2'}, 'Name': {'S': 'Australia'}, 'test': {'S': 'test'}}
            # where countryID is the table partition key
            if condition_expression:
                response = dynamodb.put_item(
                    TableName=tbl_name,
                    Item=d,
                    ConditionExpression=condition_expression
                    # 'attribute_not_exists(foo) AND attribute_not_exists(bar)'
                )
            else:
                response = dynamodb.put_item(
                    TableName=tbl_name,
                    Item=d
                )


        elif isinstance(d, pd.DataFrame):

            request_items = False
            if load_type == "bulk":
                response = dynamodb.batch_write_item(
                    RequestItems=request_items
                )

            else:

                for col in d:
                    print(str(d[col].dtypes))
                    if str(d[col].dtypes) != "object":
                        d[col] = d[col].astype(str)

                for idx, row in d.iterrows():
                    print(f'''Loading df record: {idx}''')
                    record = pd.DataFrame(row).reset_index()
                    record.columns = ["index", "val"]
                    record = record.query("index != 'name'").reset_index(drop=True)
                    item_d = {}

                    for idxxx, roww in record.iterrows():
                        field = roww['index'].split("(")[0].strip()
                        type = roww['index'].split("(")[1].replace(")", "").strip()

                        val = roww["val"]

                        item_d.update({
                            field: {type: val}
                        })

                    response = dynamodb.put_item(
                        TableName=tbl_name,
                        Item=item_d
                    )

        return response

    @classmethod
    def bulk_load_dynamo(cls, pub, sec, region_name,
                         tbl_name,
                         request_items: List,
                         endpoint_url=None):
        client = boto3.client(
            service_name='dynamodb',
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            endpoint_url=endpoint_url
        )

        print(f"Writing {len(request_items)} to dynamodb")

        start = 0
        while True:
            # Loop adding 25 items to dynamo at a time
            # request_items = create_batch_write_structure(tbl_name, start, 25)
            print(f"Load size: {len(request_items[start:start + 25])}")

            if len(request_items[start:start + 25]) == 0:
                print(f"Nothing to insert, stopping")
                break

            response = client.batch_write_item(
                RequestItems={tbl_name: request_items[start:start + 25]}
            )
            if len(response['UnprocessedItems']) == 0:
                print(f'Wrote {len(request_items[start:start + 25])} items to dynamo')
            else:
                # Hit the provisioned write limit
                print('Hit write limit, backing off then retrying')
                time.sleep(5)

                # Items left over that haven't been inserted
                unprocessed_items = response['UnprocessedItems']
                print('Resubmitting items')
                # Loop until unprocessed items are written
                while len(unprocessed_items) > 0:
                    response = client.batch_write_item(
                        RequestItems=unprocessed_items
                    )
                    # If any items are still left over, add them to the
                    # list to be written
                    unprocessed_items = response['UnprocessedItems']

                    # If there are items left over, we could do with
                    # sleeping some more
                    if len(unprocessed_items) > 0:
                        print('Backing off for 5 seconds')
                        time.sleep(5)

                # Inserted all the unprocessed items, exit loop
                print('Unprocessed items successfully inserted')
                break

            start += 25
            print(f"Curr position: {start}")

    @classmethod
    async def async_bulk_load_dynamo(cls, pub, sec, region_name,
                                     tbl_name,
                                     request_items: List,
                                     endpoint_url=None):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param tbl_name:
        :param request_items:
        :param endpoint_url:
        :return:
        """
        session = get_session()
        async with session.create_client(
                service_name='dynamodb',
                region_name=region_name,
                aws_access_key_id=pub,
                aws_secret_access_key=sec,
                endpoint_url=endpoint_url
        ) as client:

            print(f"Writing {len(request_items)} to dynamodb")

            start = 0
            while True:
                # Loop adding 25 items to dynamo at a time
                # request_items = create_batch_write_structure(tbl_name, start, 25)
                print(f"Load size: {len(request_items[start:start + 25])}")

                if len(request_items[start:start + 25]) == 0:
                    print(f"Nothing to insert, stopping")
                    break

                response = await client.batch_write_item(
                    RequestItems={tbl_name: request_items[start:start + 25]}
                )
                if len(response['UnprocessedItems']) == 0:
                    print(f'Wrote {len(request_items[start:start + 25])} items to dynamo')
                else:
                    # Hit the provisioned write limit
                    print('Hit write limit, backing off then retrying')
                    await asyncio.sleep(5)

                    # Items left over that haven't been inserted
                    unprocessed_items = response['UnprocessedItems']
                    print('Resubmitting items')
                    # Loop until unprocessed items are written
                    while len(unprocessed_items) > 0:
                        response = await client.batch_write_item(
                            RequestItems=unprocessed_items
                        )
                        # If any items are still left over, add them to the
                        # list to be written
                        unprocessed_items = response['UnprocessedItems']

                        # If there are items left over, we could do with
                        # sleeping some more
                        if len(unprocessed_items) > 0:
                            print('Backing off for 5 seconds')
                            await asyncio.sleep(5)

                    # Inserted all the unprocessed items, exit loop
                    print('Unprocessed items successfully inserted')
                    break

                start += 25
                print(f"Curr position: {start}")

    @classmethod
    def update_dynamo_tbl(cls, pub, sec, region_name, tbl_name, attr_definitions=None, add_index=None,
                          endpoint_url=None):

        client = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            endpoint_url=endpoint_url
        )

        response = client.update_table(
            TableName=tbl_name,
            AttributeDefinitions=attr_definitions if attr_definitions else None,
            GlobalSecondaryIndexUpdates=add_index if add_index else None,
        )

        return response

    @classmethod
    def update_dynamo_item(cls, pub, sec, region_name, tbl_name,
                           update_obj, update_expression,
                           update_attr_names, update_attr_vals,
                           endpoint_url=None,
                           ):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param tbl_name:
        :param endpoint_url:
        :param update_obj:
        :param update_expression:
        :param update_attr_names:
        :param update_attr_vals:
        :return:
        """

        client = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            endpoint_url=endpoint_url
        )

        response = client.update_item(
            Key=update_obj,
            TableName=tbl_name,
            UpdateExpression=update_expression,
            ExpressionAttributeNames=update_attr_names,
            ExpressionAttributeValues=update_attr_vals
        )

        return response

    @classmethod
    async def async_update_dynamo_item(cls, pub, sec, region_name, tbl_name,
                                       update_obj, update_expression,
                                       update_attr_names, update_attr_vals,
                                       endpoint_url=None):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param tbl_name:
        :param update_obj:
        :param update_expression:
        :param update_attr_names:
        :param update_attr_vals:
        :param endpoint_url:
        :return:
        """
        # session = aiobotocore.get_session()
        session = get_session()
        async with session.create_client(
                service_name='dynamodb',
                region_name=region_name,
                aws_access_key_id=pub,
                aws_secret_access_key=sec,
                endpoint_url=endpoint_url
        ) as client:
            response = await client.update_item(
                Key=update_obj,
                TableName=tbl_name,
                UpdateExpression=update_expression,
                ExpressionAttributeNames=update_attr_names,
                ExpressionAttributeValues=update_attr_vals
            )

            return response

    @classmethod
    def bulk_update_dynamo(cls, pub, sec, region_name,
                           update_obj_list,
                           endpoint_url=None):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param update_obj_list:
        :param endpoint_url:
        :return:
        """

        client = boto3.client(
            'dynamodb',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        response = client.transact_write_items(
            TransactItems=update_obj_list
        )

        return response

    @classmethod
    async def async_bulk_update_dynamo(cls, pub, sec, region_name,
                                       update_obj_list,
                                       endpoint_url=None):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param update_obj_list:
        :param endpoint_url:
        :return:
        """

        # session = aiobotocore.get_session()
        session = get_session()
        async with session.create_client(
                service_name='dynamodb',
                region_name=region_name,
                aws_access_key_id=pub,
                aws_secret_access_key=sec,
                endpoint_url=endpoint_url
        ) as client:
            response = await client.transact_write_items(
                TransactItems=update_obj_list
            )

            return response

    @classmethod
    def wait_dynamo_operation(cls, tbl_name, region, pub, secret, op_type, op_type_val, update_wait_time):
        """

        :param tbl_name:
        :param region:
        :param pub:
        :param secret:
        :param op_type:
        :param op_type_val:
        :param update_wait_time:
        :return:
        """
        ddb = boto3.resource(
            "dynamodb",
            # endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=pub,
            aws_secret_access_key=secret
        )
        while True:
            r = (
                ddb.
                    Table(tbl_name).
                    meta.client.
                    describe_table(TableName=tbl_name)
            )

            # todo: add table creation

            if op_type == "index_creation":
                if r.get("Table").get('GlobalSecondaryIndexes'):
                    index_created = \
                        [x for x in (
                            r
                                .get("Table")
                                .get('GlobalSecondaryIndexes')
                        )
                         if x.get('IndexName') == op_type_val
                         ]  # [0]

                    if len(index_created) == 0:
                        print(f"No GSI {op_type_val} exists on the table! Exiting.")
                        break

                    index_created_status = index_created[0].get("IndexStatus")

                    if index_created_status == 'CREATING':
                        print(f"GSI {op_type_val} is being created...waiting {update_wait_time} seconds")
                        time.sleep(update_wait_time)

                    else:
                        print(f"GSI {op_type_val} is created! Exiting.")
                        break
                else:
                    print(f"No GSIs exist on table. Exiting.")
                    break

    @classmethod
    def get_dynamo_fields(cls, tbl_name, pub, sec, region_name, endpoint_url=None):
        """
        Retrieves a list of fields for a given DynamoDB table.

        :param endpoint_url:
        :param tbl_name: Name of Dynamo table.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param region_name: Region name where the Dynamo table is.
        :return: JSON response for API call.
        """

        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        table = dynamodb.Table(tbl_name)
        response = table.scan()

        fields = [list(x.keys()) for x in response['Items']]
        fields_all = list(set(item for items in fields for item in items))

        return fields_all

    @classmethod
    def query_dynamo(cls, pub, sec, region_name, tbl_name,
                     query_type='scan',
                     endpoint_url=None,
                     query_search_obj=None,
                     filter_expression=None,
                     expression_attr_vals=None,
                     expression_attr_names=None,
                     key_condition_expr=None,
                     index_name=None):
        """
        Downloads data from a DynamoDB table into a Pandas DataFrame.

        :param index_name:
        :param key_condition_expr:
        :param expression_attr_names:
        :param expression_attr_vals:
        :param filter_expression:
        :param endpoint_url:
        :param query_search_obj:
        :param query_type: The type of query to perform.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param region_name: Region name for DynamoDB table.
        :param tbl_name: Name of DynamoDB table.
        :return: Pandas DataFrame of DynamoDB data.
        """

        client = boto3.client(
            service_name='dynamodb',
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            endpoint_url=endpoint_url
        )

        if query_type == "scan":

            results = []
            last_evaluated_key = None
            while True:
                if last_evaluated_key:
                    response = client.scan(
                        TableName=tbl_name,
                        ExclusiveStartKey=last_evaluated_key
                    )
                else:
                    response = client.scan(TableName=tbl_name)
                last_evaluated_key = response.get('LastEvaluatedKey')

                results.extend(response['Items'])

                if not last_evaluated_key:
                    break
            return results

        elif query_type == "get_item":
            response = client.get_item(
                TableName=tbl_name,
                Key=query_search_obj
            )

            return response

        elif query_type == "filtered_scan":
            results = []
            last_evaluated_key = None
            while True:
                if last_evaluated_key:
                    response = client.scan(
                        TableName=tbl_name,
                        ExclusiveStartKey=last_evaluated_key,
                        FilterExpression=filter_expression,
                        ExpressionAttributeValues=expression_attr_vals
                    )
                else:
                    response = client.scan(
                        TableName=tbl_name,
                        FilterExpression=filter_expression,
                        ExpressionAttributeValues=expression_attr_vals
                    )
                last_evaluated_key = response.get('LastEvaluatedKey')

                results.extend(response['Items'])

                if not last_evaluated_key:
                    break
            return results

        elif query_type == "query_on_keys":
            results = []
            last_evaluated_key = None
            while True:
                if last_evaluated_key:
                    response = client.query(
                        TableName=tbl_name,
                        ExclusiveStartKey=last_evaluated_key,
                        ExpressionAttributeNames=expression_attr_names,
                        ExpressionAttributeValues=expression_attr_vals,
                        KeyConditionExpression=key_condition_expr
                    )
                else:
                    response = client.query(
                        TableName=tbl_name,
                        ExpressionAttributeNames=expression_attr_names,
                        ExpressionAttributeValues=expression_attr_vals,
                        KeyConditionExpression=key_condition_expr,

                    )
                last_evaluated_key = response.get('LastEvaluatedKey')

                results.extend(response['Items'])

                if not last_evaluated_key:
                    break
            return results

        elif query_type == "query_on_index":
            results = []
            last_evaluated_key = None
            while True:
                if last_evaluated_key:
                    response = client.query(
                        TableName=tbl_name,
                        ExclusiveStartKey=last_evaluated_key,
                        IndexName=index_name,
                        ExpressionAttributeValues=expression_attr_vals,
                        ExpressionAttributeNames=expression_attr_names,
                        KeyConditionExpression=key_condition_expr
                    )
                else:
                    response = client.query(
                        TableName=tbl_name,
                        IndexName=index_name,
                        ExpressionAttributeValues=expression_attr_vals,
                        ExpressionAttributeNames=expression_attr_names,
                        KeyConditionExpression=key_condition_expr
                    )
                last_evaluated_key = response.get('LastEvaluatedKey')

                results.extend(response['Items'])

                if not last_evaluated_key:
                    break
            return results
        else:
            print("query type must be one of: get_item, scan, filtered_scan, query_on_keys, query_on_index")

    @classmethod
    async def async_query_dynamo(cls, pub, sec, region_name, tbl_name,
                                 query_type='scan',
                                 endpoint_url=None,
                                 query_search_obj=None,
                                 filter_expression=None,
                                 expression_attr_vals=None,
                                 expression_attr_names=None,
                                 key_condition_expr=None,
                                 index_name=None):
        """
        Downloads data from a DynamoDB table into a Pandas DataFrame.

        :param index_name:
        :param key_condition_expr:
        :param expression_attr_names:
        :param expression_attr_vals:
        :param filter_expression:
        :param endpoint_url:
        :param query_search_obj:
        :param query_type: The type of query to perform.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param region_name: Region name for DynamoDB table.
        :param tbl_name: Name of DynamoDB table.
        :return: Pandas DataFrame of DynamoDB data.
        """

        # session = aiobotocore.get_session()
        session = get_session()

        async with session.create_client(
                service_name='dynamodb',
                region_name=region_name,
                aws_access_key_id=pub,
                aws_secret_access_key=sec,
                endpoint_url=endpoint_url
        ) as client:

            if query_type == "scan":

                results = []
                last_evaluated_key = None
                while True:
                    if last_evaluated_key:
                        response = await client.scan(
                            TableName=tbl_name,
                            ExclusiveStartKey=last_evaluated_key
                        )
                    else:
                        response = await client.scan(TableName=tbl_name)
                    last_evaluated_key = response.get('LastEvaluatedKey')

                    results.extend(response['Items'])

                    if not last_evaluated_key:
                        break
                return results

            elif query_type == "get_item":
                response = await client.get_item(
                    TableName=tbl_name,
                    Key=query_search_obj
                )

                return response

            elif query_type == "filtered_scan":
                results = []
                last_evaluated_key = None
                while True:
                    if last_evaluated_key:
                        response = await client.scan(
                            TableName=tbl_name,
                            ExclusiveStartKey=last_evaluated_key,
                            FilterExpression=filter_expression,
                            ExpressionAttributeValues=expression_attr_vals
                        )
                    else:
                        response = await client.scan(
                            TableName=tbl_name,
                            FilterExpression=filter_expression,
                            ExpressionAttributeValues=expression_attr_vals
                        )
                    last_evaluated_key = response.get('LastEvaluatedKey')

                    results.extend(response['Items'])

                    if not last_evaluated_key:
                        break
                return results

            elif query_type == "query_on_keys":
                results = []
                last_evaluated_key = None
                while True:
                    if last_evaluated_key:
                        response = await client.query(
                            TableName=tbl_name,
                            ExclusiveStartKey=last_evaluated_key,
                            ExpressionAttributeNames=expression_attr_names,
                            ExpressionAttributeValues=expression_attr_vals,
                            KeyConditionExpression=key_condition_expr
                        )
                    else:
                        response = await client.query(
                            TableName=tbl_name,
                            ExpressionAttributeNames=expression_attr_names,
                            ExpressionAttributeValues=expression_attr_vals,
                            KeyConditionExpression=key_condition_expr,

                        )
                    last_evaluated_key = response.get('LastEvaluatedKey')

                    results.extend(response['Items'])

                    if not last_evaluated_key:
                        break
                return results

            elif query_type == "query_on_index":
                results = []
                last_evaluated_key = None
                while True:
                    if last_evaluated_key:
                        response = await client.query(
                            TableName=tbl_name,
                            ExclusiveStartKey=last_evaluated_key,
                            IndexName=index_name,
                            ExpressionAttributeValues=expression_attr_vals,
                            ExpressionAttributeNames=expression_attr_names,
                            KeyConditionExpression=key_condition_expr
                        )
                    else:
                        response = await client.query(
                            TableName=tbl_name,
                            IndexName=index_name,
                            ExpressionAttributeValues=expression_attr_vals,
                            ExpressionAttributeNames=expression_attr_names,
                            KeyConditionExpression=key_condition_expr
                        )
                    last_evaluated_key = response.get('LastEvaluatedKey')

                    results.extend(response['Items'])

                    if not last_evaluated_key:
                        break
                return results
            else:
                print("query type must be one of: get_item, scan, filtered_scan, query_on_keys, query_on_index")

    @classmethod
    def dynamo_results_to_df(cls, data: List, fields: List):
        """
        Converts the results of a Dynamo query into a Pandas DatFrame
        :param data: Results of a Dynamo query (List)
        :param fields: Fields to bring in from results
        :return: Pandas DatFrame
        """
        df = pd.DataFrame()
        f_dat_all = []
        for idx, f in enumerate(fields):
            f_dat = []
            for idxx, doc in enumerate(data):
                if doc.get(f):
                    f_dat.append(
                        # doc.get(f)
                        list(doc.get(f).values())[0]
                    )
                else:
                    f_dat.append(doc.get(f))

            df[f] = pd.Series(f_dat)
            f_dat_all.append({f: f_dat})

        return df

    @classmethod
    def df_list_prep_dynamo(cls, df_list: List):
        """

        :param df_list: List of pandas DataFrames of same format (columns, datatypes)
        :return:
        """
        res_data_all_comb = []
        for idx_, res_ in enumerate(df_list):
            res_data_str = \
                [
                    # dynamo needs everything to be a string
                    {w: {'S': str(x)}} for w, x
                    in zip(res_.columns.to_list(), res_.values.tolist()[0])
                    if type(x) == str
                ]

            res_data_map = \
                [
                    {w: {'M': x}} for w, x
                    in zip(res_.columns.to_list(), res_.values.tolist()[0])
                    if type(x) == dict
                ]

            res_data_list = \
                [
                    {w: {'L': x}} for w, x
                    in zip(res_.columns.to_list(), res_.values.tolist()[0])
                    if type(x) == list
                ]

            res_data_map_new = []
            for i, map_ in enumerate(res_data_map):
                # declare param types for map elements
                res_data_map_keys = list(list(res_data_map[i].values())[0].get("M").keys())
                res_data_map_vals = list(list(res_data_map[i].values())[0].get("M").values())

                res_data_map_vals_str = [{'S': str(x)} for x in res_data_map_vals if type(x) == str]
                res_data_map_vals_int = [{'N': str(x)} for x in res_data_map_vals if type(x) == int]

                res_data_map_vals_all = res_data_map_vals_str + res_data_map_vals_int

                res_data_map_new.append({
                    list(map_)[0]: {
                        "M": dict(
                            zip(
                                res_data_map_keys,
                                res_data_map_vals_all
                            )
                        )
                    }
                })

            res_data_list_new = []
            for i, list_ in enumerate(res_data_list):
                vals = list(res_data_list[i].values())[0].get("L")
                val_types = [type(x) for x in list(res_data_list[i].values())[0].get("L")]

                res_data_list_new.append(
                    {
                        list(res_data_list[i].keys())[0]: {
                            'L': [{'N' if x == int else 'S': str(y)} for x, y in zip(val_types, vals)]}
                    }
                )

            res_data_int = \
                [
                    {w: {'N': str(x)}} for w, x
                    in zip(res_.columns.to_list(), res_.values.tolist()[0])
                    if type(x) == int
                ]

            res_data_all = res_data_str + res_data_int + res_data_map_new + res_data_list_new

            res_data_all_dict = \
                dict(
                    zip(
                        [list(x)[0] for x in res_data_all],
                        [list(x.values())[0] for x in res_data_all]
                    )
                )

            res_data_all_comb.append(res_data_all_dict)

        load_list = [{"PutRequest": {"Item": x}} for x in res_data_all_comb]

        return load_list, res_data_all_comb

    @classmethod
    def dynamo_to_redshift(cls, cursor, pub, sec, tbl_name_rs, tbl_name_dynamo, readratio=50, fields=False):
        """
        Copies contents of a DynamoDB table to a Redshift table.

        :param cursor: Redshift database cursor object.
        :param pub: AWS account public key.
        :param sec: AWS account secret key.
        :param tbl_name_rs: Name of Redshift table.
        :param tbl_name_dynamo: Name of Dynamo table.
        :param readratio: The percentage of the DynamoDB table's provisioned throughput to use for the data load
        :param fields: List of fields to pull from Dynamo table.
        :return: Completed Dynamo to Redshift ETL operation.
        """
        sql_exec = \
            f'''
            copy {tbl_name_rs} {str(fields)
                .replace("[", "(")
                .replace("]", ")")
                .replace("'", "")
            if fields else ""} 
                from 'dynamodb://{tbl_name_dynamo}'
            credentials 'aws_access_key_id={pub};aws_secret_access_key={sec}'
            readratio {readratio}; 
            '''

        cursor.execute(sql_exec)
        cursor.execute("commit")

    @classmethod
    def create_iam_role_for_lambda(cls, iam_resource, iam_role_name):
        """
        Creates an IAM role.
        :param iam_resource: The Boto3 IAM resource object.
        :param iam_role_name: The name of the role to create.
        :return: The newly created role.
        """
        lambda_assume_role_policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'lambda.amazonaws.com'
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        }
        policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'

        try:
            role = iam_resource.create_role(
                RoleName=iam_role_name,
                AssumeRolePolicyDocument=json.dumps(lambda_assume_role_policy))
            iam_resource.meta.client.get_waiter('role_exists').wait(RoleName=iam_role_name)
            print("Created role %s.", role.name)

            role.attach_policy(PolicyArn=policy_arn)
            print("Attached basic execution policy to role %s.", role.name)
        except ClientError as error:
            if error.response['Error']['Code'] == 'EntityAlreadyExists':
                role = iam_resource.Role(iam_role_name)
                print("The role %s already exists. Using it.", iam_role_name)
            else:
                print(
                    "Couldn't create role %s or attach policy %s.",
                    iam_role_name, policy_arn)
                raise

        return role

    @classmethod
    def create_iam_role_for_lambda(cls, iam_resource, iam_role_name):
        """
        Creates an AWS Identity and Access Management (IAM) role that grants the
        AWS Lambda function basic permission to run. If a role with the specified
        name already exists, it is used for the demo.
        :param iam_resource: The Boto3 IAM resource object.
        :param iam_role_name: The name of the role to create.
        :return: The newly created role.
        """
        lambda_assume_role_policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'lambda.amazonaws.com'
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        }
        policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'

        try:
            role = iam_resource.create_role(
                RoleName=iam_role_name,
                AssumeRolePolicyDocument=json.dumps(lambda_assume_role_policy))
            iam_resource.meta.client.get_waiter('role_exists').wait(RoleName=iam_role_name)
            print(f"Created role '{role.name}")

            role.attach_policy(PolicyArn=policy_arn)
            print(f"Attached basic execution policy to role '{role.name}'")
        except ClientError as error:
            if error.response['Error']['Code'] == 'EntityAlreadyExists':
                role = iam_resource.Role(iam_role_name)
                print(f"The role '{iam_role_name}' already exists. Using it.")
            else:
                print(f"Couldn't create role '{iam_role_name}' or attach policy '{policy_arn}'", )
                raise

        return role

    @classmethod
    def deploy_lambda_function(cls,
                               lambda_client,
                               function_name,
                               handler_name,
                               iam_role,
                               deployment_package,
                               function_runtime="python3.8",
                               timeout_seconds=900,
                               memory_size=128,
                               env_vars=False,
                               tags=False,
                               function_description=None):
        """
        Deploys the AWS Lambda function.
        :param lambda_client: The Boto3 AWS Lambda client object.
        :param function_name: The name of the AWS Lambda function.
        :param handler_name: The fully qualified name of the handler function. This
                             must include the file name and the function name.
        :param iam_role: The IAM role to use for the function.
        :param deployment_package: The deployment package that contains the function
                                   code in ZIP format.
        :return: The Amazon Resource Name (ARN) of the newly created function.
        """
        try:
            response = lambda_client.create_function(
                FunctionName=function_name,
                Description=function_description if function_description else "",
                Runtime=function_runtime,
                Role=iam_role.arn,
                Handler=handler_name,
                Code=deployment_package,
                Timeout=timeout_seconds,
                Tags=tags if tags else {},
                MemorySize=memory_size,
                Environment={'Variables': env_vars} if env_vars else False,
                Publish=True)
            function_arn = response['FunctionArn']
            print(f"Created function '{function_name}' with ARN: {function_arn}")
        except ClientError:
            print(f"Couldn't create function '{function_name}'")
            raise
        else:
            return function_arn

    @classmethod
    def delete_lambda_function(cls, lambda_client, function_name):
        """
        Deletes an AWS Lambda function.
        :param lambda_client: The Boto3 AWS Lambda client object.
        :param function_name: The name of the function to delete.
        """
        try:
            lambda_client.delete_function(FunctionName=function_name)
        except ClientError:
            print(f"Couldn't delete function '{function_name}'")
            raise

    @classmethod
    def invoke_lambda_function(cls, lambda_client, function_name, function_params):
        """
        Invokes an AWS Lambda function.
        :param lambda_client: The Boto3 AWS Lambda client object.
        :param function_name: The name of the function to invoke.
        :param function_params: The parameters of the function as a dict. This dict
                                is serialized to JSON before it is sent to AWS Lambda.
        :return: The response from the function invocation.
        """
        try:
            response = lambda_client.invoke(
                FunctionName=function_name,
                Payload=json.dumps(function_params).encode())
            print(f"Invoked function '{function_name}'")
        except ClientError:
            print(f"Couldn't invoke function '{function_name}'")
            raise
        return response

    @classmethod
    def list_sqs_queues(cls, pub: str, sec: str, region_name: str, max_results: Optional[int]):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param max_results:
        :return:
        """
        # create the queue
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
        )

        queues = sqs_client.list_queues(MaxResults=max_results)

        return queues

    @classmethod
    def create_sqs_queue(cls, pub: str, sec: str, region_name: str, queue_name: str, attr_dict: dict):
        """

        :param pub:
        :param sec:
        :param region_name:
        :return:
        """
        # create the queue
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
        )

        try:
            response = sqs_client.create_queue(
                QueueName=queue_name,
                Attributes=attr_dict
            )
        except Exception as e:
            response = str(e)

        return response

    @classmethod
    def get_sqs_messages(cls, pub: str, sec: str, region_name: str, queue_name: str,
                         max_messages: Optional[int] = 10,
                         wait_time_seconds: Optional[int] = 5
                         ):
        """

        :param wait_time_seconds:
        :param pub:
        :param sec:
        :param region_name:
        :param queue_name:
        :param max_messages:
        :return:
        """
        sqs_resource = boto3.resource(
            'sqs',
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
        )

        queue = sqs_resource.get_queue_by_name(QueueName=queue_name)

        messages_all = []
        while True:
            messages = queue.receive_messages(
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds
            )
            if len(messages) == 0:
                break
            for message in messages:
                messages_all.append(message.body)

        return messages_all

    @classmethod
    def get_sqs_messages2(cls, pub: str, sec: str, region_name: str, queue_url: str,
                          max_messages: Optional[int] = 10,
                          # wait_time_seconds: Optional[int] = 5
                          ):

        # sqs_client = boto3.client('sqs')
        sqs_client = boto3.client(
            'sqs',
            region_name=region_name,
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
        )

        messages = []

        while True:
            resp = sqs_client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=max_messages
            )

            try:
                messages.extend(resp['Messages'])
            except KeyError:
                break

        return messages

    @classmethod
    def send_sqs_messages(cls, pub: str, sec: str, region_name: str, messages: List, queue_url: str,
                          batch_mode: Optional[bool] = False):
        """

        :param pub:
        :param sec:
        :param region_name:
        :param messages:
        :param queue_url:
        :param batch_mode:
        :return:
        """
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=pub,
            aws_secret_access_key=sec,
            region_name=region_name,
        )

        response = None
        msg_list_id = []
        msg_list_body = []
        response_list = []
        for idx, message in enumerate(messages):
            if batch_mode:
                msg_list_id.append(idx)
                msg_list_body.append(message)
            else:
                response = sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=message,
                )

                response_list.append(response)

        if batch_mode:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=[
                    {"Id": str(x), "MessageBody": str(y)} for x, y in zip(msg_list_id, msg_list_body)
                ]
            )

        return response

    @classmethod
    def create_cloudfront_s3_link(cls, public_sign_key: str, s3_object_name: str,
                                  cf_distribution_addr: str, expiration_date: datetime,
                                  pem_path: str, protocol_schema="http"
                                  ):

        def rsa_signer(message):
            with open(pem_path, 'rb') as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend()
                )
            return private_key.sign(message, padding.PKCS1v15(), hashes.SHA1())

        url = f'{protocol_schema}://{cf_distribution_addr}/{s3_object_name}'

        cloudfront_signer = CloudFrontSigner(public_sign_key, rsa_signer)

        signed_url = \
            cloudfront_signer.generate_presigned_url(
                url=url,
                date_less_than=expiration_date
            )

        return signed_url


class GCP:
    """
    Functions for interacting with GCP infrastructure.

    .. image:: ../images_source/cloud_tools/gcp3.png
        :width: 45%

    """

    @classmethod
    def make_credentials(cls, cred_method, gcloud_project_name,
                         gcloud_token_dict=None, gcloud_token_path=None):
        """
        Creates an authenticated GCP client object.

        :param cred_method: Credentials object, JSON file from disk or JSON object in memory.
        :param gcloud_project_name: Name of GCP project.
        :param gcloud_token_dict: Name of JSON GCP token if in memory.
        :param gcloud_token_path: Name of JSON GCP token if on disk.
        :return: Authenticated GCP client.
        """
        if cred_method == "file":
            credentials = (
                ServiceAccountCredentials
                    .from_json_keyfile_name(gcloud_token_path)
            )
        elif cred_method == "dict":
            credentials = (
                ServiceAccountCredentials
                    .from_json_keyfile_dict(gcloud_token_dict)
            )

        client = storage_gcp.Client(
            credentials=credentials,
            project=gcloud_project_name)

        return client

    @classmethod
    def get_bucket(cls, client, bucket_name):
        """
        Returns a GCP storage bucket object.

        :param client: Authenticated GCP client object
        :param bucket_name: Name of GCP bucket.
        :return: GCP storage bucket object.
        """
        bucket = client.get_bucket(bucket_name)
        return bucket

    @classmethod
    def get_bucket_objects(cls, bucket_obj, prefix=False):
        """
        Returns a list of bucket objects from a GCP storage object.

        :param bucket_obj: GCP storage bucket object to pull list of contained objects from.
        :param prefix: Optional filter to include to filter bucket object paths by.
        :return: List of GCP storage bucket objects.
        """
        if prefix:
            return [x for x in bucket_obj.list_blobs(prefix=prefix)]
        else:
            return [x for x in bucket_obj.list_blobs()]

    @classmethod
    def load_bucket_objects(cls, bucket, file_list, sav_dir):
        """
        Loads a list of objects to a GCP storage bucket.

        :param bucket: Name of GCP storage bucket.
        :param file_list: List of local names to load.
        :param sav_dir: Local directory containing files to load.
        :return: Nothing
        """
        for file in file_list:
            blob = bucket.blob(file)
            blob.upload_from_filename(sav_dir + file)

    @classmethod
    def download_bucket_objects(cls, obj_list, sav_dir):
        """
        Saves a list of GCP storage objects to a local directory.

        :param obj_list: List of GCP bucket storage objects.
        :param sav_dir: Local directory to save objects to.
        :return: Nothing
        """
        for file in obj_list:
            # Download the file to a destination
            file.download_to_filename(
                sav_dir + (
                    str(file)
                        .split(",")[-1]
                        .replace(">", "")
                        .strip()
                )
            )

    @classmethod
    def delete_bucket_objects(cls, obj_list):
        """
        Deletes objects from a GCP storage bucket.

        :param obj_list: List of GCP storage objects to delete.
        :return: Nothing
        """
        for obj in obj_list:
            obj.delete()

    @classmethod
    def publish_app(cls, app_config_file, project_id):
        # cmd = f'''gcloud app deploy app.yaml --project [project-id]'''
        cmd = f'''gcloud app deploy {app_config_file} --project {project_id}'''
        sys.command(cmd)

    # todo: create cloud function
    @classmethod
    def create_cloud_function(cls, function_name, topic_name, python_runtime, timeout_seconds):
        cmd = f'''gcloud functions deploy {function_name}
        --entry-point main 
        --runtime {python_runtime} 
        --trigger-resource {topic_name} 
        --trigger-event google.pubsub.topic.publish 
        --timeout {timeout_seconds}s'''.replace("\n", "")

        return cmd

    # todo: create scheduled job
    @classmethod
    def create_scheduled_job(cls):
        cmd = f'''gcloud scheduler jobs create pubsub [JOB_NAME] 
        --schedule [SCHEDULE] 
        --topic [TOPIC_NAME] 
        --message-body [MESSAGE_BODY]
        '''

        from google.cloud import scheduler

        project_id = "XXXX"
        client = scheduler_v1.CloudSchedulerClient.from_service_account_json(
            r"./xxxx.json")

        parent = client.location_path(project_id, 'us-central1')

        job = {"name": "projects/your-project/locations/app-engine-location/jobs/traing_for_model",
               "description": "this is for testing training model",
               "http_target": {
                   "uri": "https://us-central1-gerald-automl-test.cloudfunctions.net/automl-trainmodel-1-test-for-cron-job"},
               "schedule": "0 10 * * *",
               "time_zone": "Australia/Perth",
               }

        training_job = client.create_job(parent, job)

        return cmd
