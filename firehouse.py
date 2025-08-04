from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR
import ssl
import certifi
import websockets.sync.client
from langdetect import detect

import pandas as pd
import time

# 🔧 Patch websockets to use proper SSL context (certifi fixes the cert error)
original_connect = websockets.sync.client.connect
ssl_context = ssl.create_default_context(cafile=certifi.where())

def patched_connect(*args, **kwargs):
    kwargs['ssl'] = ssl_context
    return original_connect(*args, **kwargs)

websockets.sync.client.connect = patched_connect


bsky_posts = {
    'posts': [],
    'timestamp': [],
    'langs': [],
    'facets': [],
    'reply': [],
    'labels': [],
    'did': [],
    'post_id': [],
    'reply_to_did': [],
    'reply_to_post_id': []
}

# 🧵 Firehose handler
def on_message_handler(message):
    commit = parse_subscribe_repos_message(message)
    
    if len(bsky_posts['posts']) >= 10000:
        post_df = pd.DataFrame(bsky_posts)
        post_df.to_csv('output/master.csv', mode='a', index=False)
        
        for key in list(bsky_posts.keys()):
            bsky_posts[key].clear()

    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    if not commit.blocks:
        return

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        try:
       	    if op.action == "create" and op.cid:
                data = car.blocks.get(op.cid)

                if data and data.get('$type') == 'app.bsky.feed.post':
                    text = data.get('text', '')

                    try:
                        langs = data.get('langs', '')[0]
                    except:
                        langs = 'xx'

                    facets = bool(data.get('facets', ''))
                    reply = bool(data.get('reply', ''))

                    try:
                        labels = data.get('labels', '')['values'][0]['val']
                    except:
                        labels = 'none'

                    # Post author and ID
                    post_did = commit.repo.replace('did:plc:', '')
                    post_id = op.path.split('/')[-1]

                    # Default values for reply target
                    reply_to_did = 'none'
                    reply_to_post_id = 'none'

                    try:
                        reply_uri = data['reply']['parent']['uri']
                        reply_to_did = reply_uri.split('/')[2].replace('did:plc:', '')
                        reply_to_post_id = reply_uri.split('/')[-1]
                    except:
                        pass

                    createdAt = time.time()

                    bsky_posts['posts'].append(text)
                    bsky_posts['timestamp'].append(createdAt)
                    bsky_posts['langs'].append(langs)
                    bsky_posts['facets'].append(facets)
                    bsky_posts['reply'].append(reply)
                    bsky_posts['labels'].append(labels)
                    bsky_posts['did'].append(post_did)
                    bsky_posts['post_id'].append(post_id)
                    bsky_posts['reply_to_did'].append(reply_to_did)
                    bsky_posts['reply_to_post_id'].append(reply_to_post_id)

                    print(text)
                    print(createdAt)
                    print("-------------------------------")

        except Exception as e:
            print("Skipped op due to error")
            continue

# 🔌 Start client
# Allows the client to stop while saving the state using a keyboard interrupt
client = FirehoseSubscribeReposClient()

while True:
    try:
        print("Starting Bluesky Firehouse")
        client.start(on_message_handler)
    except KeyboardInterrupt:
        print("Stopping...")
        break
    except Exception as e:
        print(f"Firehouse stalled: {e}")
        print("Reconnecting after 5 seconds")
        time.sleep(5)

print("Process has ended!")










