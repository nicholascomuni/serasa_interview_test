#import findspark
#findspark.init()
#from pyspark.sql import SparkSession
import tweepy
from datetime import datetime, timedelta
import json
import tempfile

from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
    
api_key = "EdAoEqTNwz10vEPGmCvYLhjYK"
api_key_secret = "R2vwF9bkuBpAE9BbGbwdK8PJ96bjVUUwKxdFqAFG083ZdIeGHy"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAFrdeQEAAAAATuSXukaBLVML29SvFP%2Br1k5UYTo%3DKehDqZUH5KJmQjFPCLpvKCBmsQXF9pVvuKfBak1YJ2dzI4qYLX"
access_token =  "1544068011828584449-j9jg3Lawmf6cjcKH3IEPiGQqjVJZbm"
access_token_secret = "b8KwBePO4YmLw7YeRThYJqvJzKcHdABSrlKoZzuJUq1RD"

def request_twitter_data():
    client = tweepy.Client(bearer_token = bearer_token)
    hashtag = "#australia"
    response = client.search_recent_tweets(query=hashtag,
                                       tweet_fields=['created_at','lang'],
                                       start_time=datetime.now()-timedelta(days=1),
                                       end_time = datetime.now())

    json_data = json.dumps([tweet.data for tweet in response.data])
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmpfile_path = f"{tmp_dir}/{hashtag.replace('#','')}.json"
        print(tmpfile_path)

        with open(tmpfile_path,"w") as tmpfile:
            tmpfile.write(json_data)

        filepath = f"hashtags/{hashtag.replace('#','')}/{datetime.now().year}/{datetime.now().month}/{hashtag.replace('#','')}_{str(datetime.now()).replace(':','_')}.json"

        hook = WasbHook(wasb_conn_id="wasbconn")
        hook.load_file(tmpfile_path, container_name="landingzone",blob_name=filepath)


def transform_function():
    pass