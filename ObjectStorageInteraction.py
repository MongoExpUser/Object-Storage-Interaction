# ************************************************************************************************************************
# * @License Starts                                                                                                      *
# *                                                                                                                      *
# * Copyright © 2015 - present. MongoExpUser                                                                             *
# *                                                                                                                      *
# *  License: MIT - See: https://opensource.org/licenses/MIT                                                             *
# *                                                                                                                      *
# * @License Ends                                                                                                        *
# *                                                                                                                      *
# ************************************************************************************************************************
#                                                                                                                        *
#  ...Ecotert's ObjectStorageInteraction.py  (released as open-source under MIT License) implements:                     *
#                                                                                                                        *
#                                                                                                                        *                                                                                                                       *
#    ObjectStorageInteraction() class for interacting with public clouds' Object Storage using boto3 and s3f3 library.   *
#                                                                                                                        *
#     The following public clouds' block storages are covered:                                                           *
#                                                                                                                        *
#     (1) Amazon S3 (aws_s3)                                                                                             *
#                                                                                                                        *                                                                                                                      *
#     (2) Linode Object Storage (linode_objs)                                                                            *
#                                                                                                                        *
#     (3) Backblaze Cloud Storage (b2_cs)                                                                                *
#                                                                                                                        *                                                                                                                       *
#     (4) Google Cloud Storage (gcp_cs)                                                                                  *
#                                                                                                                        *
#     (5) Add others in the future                                                                                       *
#                                                                                                                        *
# ************************************************************************************************************************
# ************************************************************************************************************************

try:
    """  
    import commonly used modules and check for import error
    """
    #import
    import sys, boto3, s3fs
    from pprint import pprint
    from pandasql import sqldf
    import dask.dataframe as dd
    from pandas import read_csv
    from boto3.session import Session
    from botocore.client import Config
    from dask_sql import Context as context
    #check for error
except(ImportError) as err:
  print(str(err))


class ObjectStorageInteraction():
    """
    A class that implements methods for interacting with public clouds' Object Storage using boto3 and s3f3 library.
    The following public clouds' block storages are covered:
    (1) Amazon S3 (aws_s3)
    (2) Linode Object Storage - linode_objs
    (3) Backblaze Cloud Storage  - b2_cs
    (4) Google Cloud Storage - gcp_cs
    (5) Add others in the future

    References:
    ==========
    1) https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    2) https://s3fs.readthedocs.io/en/latest/
    3) https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html
    4) https://aws.amazon.com/comprehend/  - Amazon natural language processing (NLP) AIML service.
    """

    def __init__(self):
        print()
        print("Initiating Object Storage Interaction Engine.")
        print("=============================================")
        print(ObjectStorageInteraction.__doc__)


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
        
        # 3. sample_query() method within this class - see line 172 below.
        #    self.sample_query(file=None, input_serialization_option=None, client=None, bucket_name=None, file_name=None, sql_query_string=None, sample_one=True)
        
        # 4. note: for BIG DAT ANALYTICS:
        #   a) the "client.select_object_content()" is useful for querying CSV, JSON and PARQUET files directly with SQL
        #    expressions on AWS S3, essentially turning "data-lake" into serverless database - no need to move data to a RDBMS
        #    for refereneces, see:
        #    i)   AWS Storage Blog Article: https://aws.amazon.com/blogs/storage/querying-data-without-servers-or-databases-using-amazon-s3-select/
        #    ii)  S3-Select SQL Reference: https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html
        #    iii) S3-Select AWS SDK for Python (Boto3) - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content
        #    iv)  S3-Select AWS SDK for JavaScript - https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#selectObjectContent-property
        #
        #    b) alternatively, on AWS S3 and other S3-compatible object storage systems; provided by linode, backblaze, GCP, etc;
        #    PySpark can also be used to load CSV, JSON and PARQUET files as DataFrames and "PySpark SQL" can then be used
        #    to issue SQL expressions or queries against the DataFrames just like "client.select_object_content()".
        #    see - https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
        
        return {"bucket": bucket, "client": client}


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
        # 1. https://s3fs.readthedocs.io/en/latest/api.html
        # 2. print examples below: uncomment and test
        # print("-------------------------------------------------------------------------")
        # print("List of objects in bucket: ", fs.ls(bucket_name_path))
        # print("Space used by bucket: ", fs.du(bucket_name_path))
        # print("List of objects in bucket, with details/attributes: ", fs.ls(bucket_name_path, detail=True))
        # print("-------------------------------------------------------------------------")
        return {"fs": fs,  "bucket_name_path": bucket_name_path}


    def check_object_attributes(self, obj=None):
        for attr in dir(obj):
            print(attr)

    
    def serialization_options(self, input_serialization_option=None):
        input_srl = None
        output_srl = {"JSON": {"RecordDelimiter": '\n'}}
        if input_serialization_option == "parquet":
            input_srl = {"Parquet": {}}
        elif input_serialization_option == "compressed_csv":
            input_srl = {"CSV": {"FileHeaderInfo": "Use"}, "CompressionType": "GZIP"}
        elif input_serialization_option == "compressed_json":
            input_srl = {"JSON": {"Type": "DOCUMENT"}, "CompressionType": "GZIP"}
        return {"input_serialization": input_srl, "output_serialization": output_srl}

    
    def print_or_view_sql_query_result(self, sql_query_result=None, file_name=None):
        for result in sql_query_result['Payload']:
            if 'Records' in result:
                print()
                print("Result of", file_name)
                print( result['Records']['Payload'].decode('utf-8') )
                print()

        
    def sample_query(self, file=None, input_serialization_option=None, client=None, bucket_name=None, file_name=None, sql_query_string=None, sample_one=True):
        serialization =  self.serialization_options(input_serialization_option=input_serialization_option)
        
        # note 1 : file_name is a compressed json.file (GZIP) output file of Amazon Comprehend's sentiment analysis.
        # note 2 : Amazon Comprehend is a natural language processing (NLP) AIML service - see https://aws.amazon.com/comprehend/
        
        if sample_one:
            sql_query_string = "SELECT obj.Sentiment, obj.File FROM s3object obj  WHERE obj.Sentiment = 'NEUTRAL'"
        else:
            sql_query_string = "SELECT obj.SentimentScore.Neutral, obj.SentimentScore.Negative FROM s3object obj"
        
        if sql_query_string:
            sql_query_result = client.select_object_content(Bucket=bucket_name, Key=file_name,
                                                            ExpressionType='SQL', Expression= sql_query_string,
                                                            InputSerialization=serialization.get("input_serialization"),
                                                            OutputSerialization=serialization.get("output_serialization")
                                                           )
            self.print_or_view_sql_query_result(sql_query_result=sql_query_result, file_name=file_name)

        
    def s3_to_pandas_df_s3fs(self, key=None, secret=None, s3_file_key=None, bucket_name=None, read_csv=None, sqldf=None):
        """
        Read CSV file from S3 into a pandas data frame (df) and run SQL Query against the df. 
        Reading CSV file uses s3fs-supported pandas APIs under the hood.
        """

        confirm = None
        if (key and secret and s3_file_key and bucket_name and read_csv and sqldf):
            confirm = True
        pprint({ "Confirm?" : confirm })

        if confirm:
            # read file to data frame
            full_path = "{}{}{}{}".format("s3://", bucket_name, "/", s3_file_key)
            storage_options = { "key": key, "secret": secret }
            df = read_csv(full_path, storage_options=storage_options)
            pprint(df)

            # run query and print result
            query = "SELECT * FROM df LIMIT 5;"
            query_result = sqldf(query)
            print()
            print("====================== Query Result Begins ======================")
            pprint(query_result)
            print("====================== Query Result Ends ========================")
            print()


    def s3_to_dask_df_s3fs(self, key=None, secret=None, s3_file_key=None, bucket_name=None, read_csv=None, context=None):
        """
        - Read CSV file from S3 into a "dask" data frame (df). Note that "parquet" file is also supported by dask.
        - Create a TABLE from the df and run SQL Query against the TABLE, with "dask-sqL". 
        - Reading data with "dask" dataframe and running SQL query with "dask-sql" ensure that:
            a) multi-cores are used (like spark/pyspark) on multi-cores system, speeding up computation
        - Dask Ref 1: https://docs.dask.org/en/latest/ (Main Page)
        - Dask Ref 2: https://dask-sql.readthedocs.io/en/latest/api.html#dask_sql.Context.create_table
        - Dask Ref 3: https://dask-sql.readthedocs.io/en/latest/data_input.html
        - Dask Ref 4: https://dask-sql.readthedocs.io/en/latest/machine_learning.html
        """
    
        confirm = None
        if (key and secret and s3_file_key and bucket_name and read_csv and sqldf):
            confirm = True
        pprint({ "Confirm?" : confirm })
    
        if confirm:
            # read file to data frame
            read_csv = dd.read_csv
            full_path = "{}{}{}{}".format("s3://", bucket_name, "/", s3_file_key)
            storage_options = { "key": key, "secret": secret }
            df = read_csv(full_path, storage_options=storage_options) # from s3
            pprint(df.head())

            # create table
            c = context()
            # by default (persist=False), the data is lazily loaded
            # persist value of "True" loads the file's data directly into memory
            c.create_table("sample_table", df, persist=True) 
    
            # run query against table and print result
            query = "SELECT * FROM sample_table LIMIT 5;"
            query_result = c.sql(query)
            print()
            print("====================== Query Result Begins ======================")
            pprint(query_result.head())
            print("====================== Query Result Ends ========================")
            print()

    
    def separator(self):
        print("------------------------------------")

