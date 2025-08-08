# Define table references
project_id = "bsky-store"
dataset_id = "bsky_data"

posts_table = f"{project_id}.{dataset_id}.posts"

network_table = f"{project_id}.{dataset_id}.network"
network_temp_table = f"{project_id}.{dataset_id}.network_temp"

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