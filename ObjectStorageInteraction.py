# ************************************************************************************************************************
# * @License Starts
# *
# * Copyright © 2015 - present. MongoExpUser
# *
# *  License: MIT - See: https://opensource.org/licenses/MIT
# *
# * @License Ends
# *
# ************************************************************************************************************************
#
#  ...Ecotert's ObjectStorageInteraction.py  (released as open-source under MIT License) implements:
#
#
#    ObjectStorageInteraction() class for interacting with public clouds' Object Storage using boto3 and s3f3 library.
#
#     The following public clouds' block storages are covered:
#
#     (1) Amazon S3 (aws_s3)
#
#     (2) Linode Object Storage (linode_objs)
#
#     (3) Backblaze Cloud Storage (b2_cs)
#
#     (4) Google Cloud Storage (gcp_cs)
#
#     (5) Add others in the future
#
# ************************************************************************************************************************
# ************************************************************************************************************************

try:
    """  import commonly used modules and check for import error
    """
    #import
    import sys, boto3, s3fs
    from pprint import pprint
    from boto3.session import Session
    from botocore.client import Config
    #check for error
except(ImportError) as err:
  print(str(err))


class ObjectStorageInteraction():
    """
    A class that implements methods for interacting with public clouds' Object Storage using boto3 and s3f3 library.
    The following public clouds' block storages are covered:
    (1) Amazon S3 (aws_s3)
    (2) Linode Object Storage (linode_objs)
    (3) Backblaze Cloud Storage (b2_cs)
    (4) Google Cloud Storage (gcp_cs)
    (5) Add others in the future

    References:
    ==========
    1) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    2) https://s3fs.readthedocs.io/en/latest/

  """

    def __init__(self):
        print()
        print("Initiating Object Storage Interaction Engine.")
        print("=============================================")
        print(ObjectStorageInteraction.__doc__)
    # End  __init__() method

    def object_storage_interaction_using_boto3(self, ACCESS_KEY=None, SECRET_KEY=None, REGION_NAME=None, bucket_name=None, provider=None):
        endpoint_url = None
        provider = provider.lower()

        if provider == "aws":
            endpoint_url = "{}{}{}".format("https://s3.", REGION_NAME, ".amazonaws.com")
        elif provider == "linode":
            endpoint_url = "{}{}{}".format("https://", REGION_NAME, ".linodeobjects.com")
        elif provider == "backblaze":
            endpoint_url = "{}{}{}".format("https://s3.", REGION_NAME, ".backblazeb2.com")
        elif provider == "gcp":
            endpoint_url = "https://storage.googleapis.com/"

        session = Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION_NAME)
        s3 = session.resource('s3', endpoint_url=endpoint_url, config=Config(signature_version='s3v4'))
        client = session.client(service_name='s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION_NAME, endpoint_url=endpoint_url)
        bucket = s3.Bucket(bucket_name)
        
        # from here on, you can interact with the "client" and "bucket" (object storage) as desired:
        # see:
        # 1. boto3-S3 documentation -> https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
        # 2. print examples below: uncomment and test
        
        # for object in bucket.objects.all():
            # print(object.key)
        # print(client.list_buckets())
        
        # 3. note: for BIG DAT ANALYTICS: the "client.select_object_content()" is useful for querying CSV, JSON and PARQUET files directly with SQL
        #    expressions on AWS S3, essentially turning "data-lake" into serverless database - no need to move data to DBRMS
        #    for refereneces, see:
        #    1) AWS Storage Blog Article: https://aws.amazon.com/blogs/storage/querying-data-without-servers-or-databases-using-amazon-s3-select/
        #    2) S3-Select SQL Reference: https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html
        #    3) S3-Select AWS SDK for Python (Boto3) - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content
        #    4) S3-Select AWS SDK for JavaScript - https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#selectObjectContent-property
        
        # 4. alternatively, on AWS S3 and other S3-compatible object storage systems; provided by linode, backblaze, GCP, etc; 
        #    PySpark can also be used to load CSV, JSON and PARQUET files as DataFrames and "PySpark SQL" can then be used 
        #    to issue SQL expressions against the DataFrames just like "client.select_object_content()".
        #    see - https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
        
        return {"bucket": bucket, "client": client}
    # End object_storage_interaction_using_boto3() method

    def object_storage_interaction_using_s3fs(self, ACCESS_KEY=None, SECRET_KEY=None, REGION_NAME=None, bucket_name=None, provider=None):
        endpoint_url = None
        provider = provider.lower()

        if provider == "aws":
            endpoint_url = "{}{}{}".format("https://s3.", REGION_NAME, ".amazonaws.com")
        elif provider == "linode":
            endpoint_url = "{}{}{}".format("https://", REGION_NAME, ".linodeobjects.com")
        elif provider == "backblaze":
            endpoint_url = "{}{}{}".format("https://s3.", REGION_NAME, ".backblazeb2.com")
        elif provider == "gcp":
            endpoint_url = "https://storage.googleapis.com/"

        bucket_name_path =  "{}{}".format(bucket_name, "/")
        fs = s3fs.S3FileSystem(anon=False, key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={'endpoint_url': endpoint_url})
        # from here on, you can interact with the bucket (object storage) like a file system, as desired, see:
        # 1. https://s3fs.readthedocs.io/en/latest/
        # 2. print examples below: uncomment and test
        # print("-------------------------------------------------------------------------")
        # print("List of objects in bucket: ", fs.ls(bucket_name_path))
        # print("Space used by all buckets: ", fs.du(bucket_name_path))
        # print("-------------------------------------------------------------------------")
        return {"fs": fs,  "bucket_name_path": bucket_name_path}
    # End object_storage_interaction_using_s3fs()method

    def check_object_attributes(self, obj=None):
        for attr in dir(obj):
            print(attr)
    # End check_object_attributes() method
    
    def serialization_option(self, input_serialization_option=None)
        input_srl = None
        output_srl = {"JSON": {"RecordDelimiter": '\n'}}
        if input_serialization_option == "parquet":
            input_srl = {"Parquet": {}}
        elif input_serialization_option == "compressed_csv":
            input_srl = {"CSV": {"FileHeaderInfo": "Use"}, "CompressionType": "GZIP"}
        elif input_serialization_option == "compressed_json":
            input_srl = {"JSON": {"Type": "DOCUMENT"}, "CompressionType": "GZIP"}
        return {"input_serialization": input_srl, "output_serialization": output_srl}
    # End serialization_option 
        
    def separator(self):
        print("------------------------------------")
    # End separator() method
# End ObjectStorageInteraction() class
