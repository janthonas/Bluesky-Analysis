#!/usr/bin/env python
import pandas as pd
import os

import re
import emoji

import langdetect
from langdetect import detect, LangDetectException

import zlib
import base64

from collections import Counter

import time
from datetime import datetime, timezone

from google.cloud import bigquery

import os
from dotenv import load_dotenv

import numpy as np

load_dotenv()

master = pd.read_csv(os.getenv("MASTER_LOCAL_DIR"))

try:
    #os.remove("master = pd.read_csv(os.getenv("MASTER_LOCAL_DIR"))")
    print("master.csv file deleted")
except FileNotFoundError:
    print('File does not exist')
    
def master_csv_build(post):
    
    def remove_non_standard_char(p):
        return re.sub(r'[^\x00-\x7F:.,!?\'\"()\[\]{}@#&%$\-\w\s]', '', p)
    
    post_is_english = True
    def remove_non_english(p):
        nonlocal post_is_english
        try:
            if detect(p) == 'en':
                return p
            else:
                post_is_english = False
                return np.nan
        except LangDetectException:
            post_is_english = False
            return np.nan

    def emoji_to_string(p):
        return emoji.demojize(p, delimiters=(':', ':'))
    
    post = str(post)
    
    post = remove_non_standard_char(post)
    post = remove_non_english(post)
    
    if(post_is_english):
        post = emoji_to_string(post)
    
    return post

def posts_text_build(post):
    min_post_length = 100
    
    def remove_short_post(p):
        if len(p) < min_post_length:
            return np.nan
        else:
            return p
        
    def compress_post(p):
        compressed = zlib.compress(p.encode('utf-8'))
        return base64.b64encode(compressed).decode('utf-8')
    
    post = remove_short_post(post)
    if pd.isna(post):
        return np.nan
    
    post = compress_post(post)
    
    return post
    
def post_csv_build(df):
    
    def add_master_post_id(df):
        return df['did'] + '-' + df['post_id']
        
    def format_timestamp(df):
        return pd.to_datetime(df['timestamp'].astype(float), unit='s').dt.round('us')
        
    def add_weekstart(df):
        return df['timestamp'].dt.to_period('W-SUN').apply(lambda r: r.start_time)
    
    df['master_post_id'] = add_master_post_id(df)
    df['timestamp'] = format_timestamp(df)
    df['week_start'] = add_weekstart(df)
    df = df.drop(columns=['did', 'post_id'])
        
    return df

def meta_csv_build(df, df_master_total=None):
    mention_re = r'@[\w\.]+'
    hashtag_re = r'#\w+'
    emoji_re = r':[a-zA-Z0-9_]+:'

    def extract_items(text):
        if not isinstance(text, str):
            return []
        return re.findall(mention_re, text) + \
               re.findall(hashtag_re, text) + \
               re.findall(emoji_re, text)
               
    def count_items(df_with_items, df_master_total):
        # Flatten items (mentions, hashtags, emojis)
        flat_items = [item for sublist in df_with_items['items'] for item in sublist]

        # Add labels
        label_items = df_with_items['labels'].dropna().tolist()
        label_items = [label for label in label_items if isinstance(label, str)]

        # Combine all
        all_items = flat_items + label_items

        # Count frequencies
        counter = Counter(all_items)

        # Add total_posts
        counter['total_posts'] = len(df_master_total)

        # Add unique_users
        unique_users = df_with_items['did'].nunique()
        counter['unique_users'] = unique_users

        # Create DataFrame
        result_df = pd.DataFrame(counter.items(), columns=['item', 'count'])

        # Add date column (Unix timestamp at 00:00 UTC)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        result_df['timestamp'] = today

        return result_df

    df = df.copy()
    df['items'] = df['posts'].apply(extract_items)
    df = count_items(df, df_master_total)

    return df

def network_csv_build(df):
    # Drop rows without a valid reply target
    df = df[df['reply_to_did'] != 'none']

    # Count interactions between did and reply_to_did
    counted_df = (
        df.groupby(['did', 'reply_to_did'])
          .size()
          .reset_index(name='count')
    )

    # Add timestamps
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    counted_df['timestamp'] = today
    counted_df['week_start'] = counted_df['timestamp'].dt.to_period('W-SUN').dt.start_time.dt.date

    return counted_df


# Doing master pre-processing
print('----- master pre-processing -----')
master['posts'] = master['posts'].apply(master_csv_build)
master = master.dropna(subset=['posts']).reset_index(drop=True)

# Build the posts store
print('----- posts store -----')
posts = master[['timestamp', 'posts', 'did', 'post_id']]
posts['posts'] = posts['posts'].apply(posts_text_build)
posts = post_csv_build(posts)
posts.to_csv(os.getenv('POSTS_LOCAL_DIR'), index=False)
print('Complete!')

# Build metadata store
print('----- metadata store -----')
meta = meta_csv_build(master, df_master_total=master)
meta.to_csv(os.getenv('META_LOCAL_DIR'), index=False)
print('Complete!')

# Build Network Store
print('----- network store -----')
network = network_csv_build(master[['did', 'reply_to_did']])
network.to_csv(os.getenv('NETWORK_LOCAL_DIR'), index=False)
print('Complete!')

# Initialize client (make sure GOOGLE_APPLICATION_CREDENTIALS is set)
#client = bigquery.Client()

# Define table references
project_id = "bsky-store"
dataset_id = "bsky_data"

posts_table = f"{project_id}.{dataset_id}.posts"

network_table = f"{project_id}.{dataset_id}.network"
network_temp_table = f"{project_id}.{dataset_id}.network_temp"

# 1. Upload the new meta DataFrame to a temporary table
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
#client.load_table_from_dataframe(network, network_temp_table, job_config=job_config).result()

# 2. Run a MERGE to upsert into the Network table
network_merge_query = f"""
MERGE `{network_table}` AS target
USING `{network_temp_table}` AS source
ON target.timestamp = source.timestamp
   AND target.did = source.did
   AND target.reply_to_did = source.reply_to_did
WHEN MATCHED THEN
  UPDATE SET target.count = target.count + source.count
WHEN NOT MATCHED THEN
  INSERT (did, reply_to_did, count, timestamp, week_start)
  VALUES (source.did, source.reply_to_did, source.count, source.timestamp, source.week_start)
"""

meta_table = f"{project_id}.{dataset_id}.meta"
meta_temp_table = f"{project_id}.{dataset_id}.meta_temp"

# 1. Upload the new meta DataFrame to a temporary table
#client.load_table_from_dataframe(meta, meta_temp_table, job_config=job_config).result()

# 2. Run a MERGE to upsert into the main Meta table
meta_merge_query = f"""
MERGE `{meta_table}` AS target
USING `{meta_temp_table}` AS source
ON target.timestamp = source.timestamp AND target.item = source.item
WHEN MATCHED THEN
  UPDATE SET target.count = target.count + source.count
WHEN NOT MATCHED THEN
  INSERT (item, count, timestamp, week_start)
  VALUES (source.item, source.count, source.timestamp, source.week_start)
"""

# Example: write pandas DataFrame to Posts table
print('----- Uploading to BQ! -----')
#client.load_table_from_dataframe(posts, posts_table).result()
#client.query(network_merge_query).result()
#client.delete_table(network_temp_table, not_found_ok=True)
#client.query(meta_merge_query).result()
#client.delete_table(meta_temp_table, not_found_ok=True)
print('Complete!')

