from utils.constants import SECRET,CLIENT_ID,OUTPUT_PATH
from etls.reddit_etl import connect_reddit, load_data_to_csv, transform_data
from etls.reddit_etl import extract_posts
import pandas as pd

def reddit_pipeline(file_name:str,subreddit:str,time_filter='day',limit=None):
    #Connecting to Reddit Instance
    instance = connect_reddit(CLIENT_ID,SECRET,'Airscholar Agent')
    #Extraction
    posts = extract_posts(instance,subreddit,time_filter,limit)
    post_df = pd.DataFrame(posts)
    
    #Tranformation
    post_df = transform_data(post_df)
    
    #Loading to Csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path )
    
    return file_path