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

master = pd.read_csv('master.csv')

try:
    os.remove("master.csv")
except FileNotFoundError:
    print('File does not exist')

# Post editing
# remove all non-english sentences
# remove all posts less than 150 characters
# convert all emojis to emoji strings
# remove all non standard characters

def drop_nan_from_column(df, column_name):
    return df.dropna(subset=[column_name])

def remove_short_posts(df, column='posts', min_length=100):
    return df[df[column].str.len() >= min_length]

def remove_non_english(post):
    try:
        return post if detect(post) == 'en' else ''
    except LangDetectException:
        return ''

def emoji_to_string(post):
    return emoji.demojize(post, delimiters=(':', ':'))

def remove_non_standard(post):
    return re.sub(r'[^\x00-\x7F:.,!?\'\"()\[\]{}@#&%$\-\w\s]', '', post)

def compress_post_column(df, column='posts'):
    def compress(text):
        if not isinstance(text, str):
            return ''
        compressed = zlib.compress(text.encode('utf-8'))
        return base64.b64encode(compressed).decode('utf-8')
    
    df[column] = df[column].apply(compress)
    return df

def extract_mentions_column(df, source_col='posts', target_col='mentions'):
    mention_pattern = r'@[\w\.]+'
    
    df[target_col] = df[source_col].apply(
        lambda text: re.findall(mention_pattern, text) if isinstance(text, str) else []
    )
    
    return df

def extract_hashtags_column(df, source_col='posts', target_col='hashtags'):
    hashtag_pattern = r'#\w+'

    df[target_col] = df[source_col].apply(
        lambda text: re.findall(hashtag_pattern, text) if isinstance(text, str) else []
    )
    return df

def extract_emojis_column(df, source_col='posts', target_col='emojis'):
    emoji_pattern = r':[a-zA-Z0-9_]+:'

    df[target_col] = df[source_col].apply(
        lambda text: re.findall(emoji_pattern, text) if isinstance(text, str) else []
    )
    return df

def extract_all_items(df):
    mention_re = r'@[\w\.]+'
    hashtag_re = r'#\w+'
    emoji_re = r':[a-zA-Z0-9_]+:'

    def extract(text):
        if not isinstance(text, str):
            return []
        return re.findall(mention_re, text) + \
               re.findall(hashtag_re, text) + \
               re.findall(emoji_re, text)

    df['items'] = df['posts'].apply(extract)
    return df

def add_timestamp(df):
    # Function that adds a timsemtap 
    today = datetime.now(df['timestamp'])
    df['timestamp'] = today
    
    return df

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

def count_did_pairs(df):
    # Step 1: Filter out rows with 'none' reply_to_did
    df = df[df['reply_to_did'] != 'none']

    # Step 2: Group by (did, reply_to_did) and count
    counted_df = df.groupby(['did', 'reply_to_did']).size().reset_index(name='count')

    # Step 3: Add today's timestamp (UTC midnight)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    counted_df['timestamp'] = today 

    return counted_df

# Doing master pre-processing
print('----- master pre-processing -----')
master = drop_nan_from_column(master, 'posts')
master['posts'] = master['posts'].apply(remove_non_english)
master['posts'] = master['posts'].apply(emoji_to_string)
print('turned emojis into strings in metadata!')
master['posts'] = master['posts'].apply(remove_non_standard)
print('removed non standard characters in metadata!')
master = drop_nan_from_column(master, 'posts')

# Build the posts store
print('----- posts store -----')
posts = master[['timestamp', 'posts', 'did', 'post_id']]
posts = remove_short_posts(posts, column='posts')
posts = compress_post_column(posts, column='posts')
posts['master_post_id'] = posts['did'] + '-' + posts['post_id']
posts['timestamp'] = pd.to_datetime(posts['timestamp'], unit='s').dt.round('us')
posts = posts.drop(columns=['did', 'post_id'])
posts.to_csv('posts.csv', index=False)
print('Complete!')

# Build metadata store
print('----- metadata store -----')
meta = extract_all_items(master)
meta = count_items(meta, master)
meta.to_csv('meta.csv', index=False)
print('Complete!')

# Build Network Store
print('----- network store -----')
network = master[['did', 'reply_to_did']]
network = count_did_pairs(network)
network.to_csv('network.csv', index=False)
print('Complete!')

# Initialize client (make sure GOOGLE_APPLICATION_CREDENTIALS is set)
client = bigquery.Client()

# Define table references
project_id = "bsky-store"
dataset_id = "bsky_data"

posts_table = f"{project_id}.{dataset_id}.Posts"

network_table = f"{project_id}.{dataset_id}.Network"
network_temp_table = f"{project_id}.{dataset_id}.network_temp"

# 1. Upload the new meta DataFrame to a temporary table
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(network, network_temp_table, job_config=job_config).result()

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
  INSERT (did, reply_to_did, count, timestamp)
  VALUES (source.did, source.reply_to_did, source.count, source.timestamp)
"""

meta_table = f"{project_id}.{dataset_id}.Meta"
meta_temp_table = f"{project_id}.{dataset_id}.meta_temp"

# 1. Upload the new meta DataFrame to a temporary table
client.load_table_from_dataframe(meta, meta_temp_table, job_config=job_config).result()

# 2. Run a MERGE to upsert into the main Meta table
meta_merge_query = f"""
MERGE `{meta_table}` AS target
USING `{meta_temp_table}` AS source
ON target.timestamp = source.timestamp AND target.item = source.item
WHEN MATCHED THEN
  UPDATE SET target.count = target.count + source.count
WHEN NOT MATCHED THEN
  INSERT (item, count, timestamp)
  VALUES (source.item, source.count, source.timestamp)
"""

# Example: write pandas DataFrame to Posts table
print('----- Uploading to BQ! -----')
client.load_table_from_dataframe(posts, posts_table).result()
client.query(network_merge_query).result()
client.delete_table(network_temp_table, not_found_ok=True)
client.query(meta_merge_query).result()
client.delete_table(meta_temp_table, not_found_ok=True)
print('Complete!')

# Build the metadata store
#meta = drop_nan_from_column(master, 'posts')
#meta['posts'] = meta['posts'].apply(remove_non_english)
#print('dropped nans in metadata!')
#print('removed non english in metadata!')
#meta['posts'] = meta['posts'].apply(emoji_to_string)
#print('turned emojis into strings in metadata!')
#meta['posts'] = meta['posts'].apply(remove_non_standard)
#print('removed non standard characters in metadata!')
#meta = extract_mentions_column(meta, source_col='posts', target_col='mentions')
#print('created mentions column in metadata!')
#meta = extract_hashtags_column(meta, source_col='posts', target_col='hashtags')
#print('created hashtags column in metadata!')
#meta = extract_emojis_column(meta, source_col='posts', target_col='emojis')
#print('created emojis column in metadata!')
#meta['master_post_id'] = meta['did'] + '-' + meta['post_id']
#print('created master post in metadata!')
#meta = meta[['master_post_id', 'timestamp', 'facets', 'labels', 'did', 'post_id', 'reply_to_did', 'reply_to_post_id']]
#meta.to_csv('meta.csv', index=False)