from elasticsearch import Elasticsearch
import json

# Assuming you have an Elasticsearch client configured
es = Elasticsearch(['your-elasticsearch-host:9200'])

# Get all indices in the frozen tier
frozen_indices = es.cat.indices(h=("index", "status", "store.size", "pri.store.size"), s="index", format="json", params={"expand_wildcards": "all", "bytes": "b"})

# Filter indices in the frozen tier with size less than 1G (1,000,000,000 bytes)
small_frozen_indices = [idx for idx in frozen_indices if idx['status'] == 'open' and int(idx['pri.store.size']) < 1000000000 and idx['index'].startswith('your_data_stream_prefix-')]

#-------- 2
# Sort by index name (assuming index names include timestamps for easy sorting)
sorted_indices = sorted(small_frozen_indices, key=lambda x: x['index'])

if not sorted_indices:
    print("No small frozen indices found.")
else:
    oldest_index = sorted_indices[0]['index']
    print(f"The oldest small index is: {oldest_index}")


#------- 3
# Define the target size (50G in bytes)
target_size = 50 * 1024 * 1024 * 1024  # 50 GB

# Track current size of the oldest index
current_size = int([idx for idx in frozen_indices if idx['index'] == oldest_index][0]['pri.store.size'])

# Reindex from newer to oldest
for idx in sorted_indices[1:]:  # Skip the oldest which we're merging into
    if current_size < target_size:
        # Reindex operation
        reindex_body = {
            "source": {
                "index": idx['index']
            },
            "dest": {
                "index": oldest_index
            }
        }
        es.reindex(body=reindex_body, wait_for_completion=False)
        
        # Update current size (this step might need adjustment for precise size calculation after reindex)
        current_size += int(idx['pri.store.size'])
        
        # Delete the source index if reindex is successful (you might want to verify reindex operation first)
        es.indices.delete(index=idx['index'])
        
        print(f"Reindexed {idx['index']} into {oldest_index}. Current size: {current_size}")
    else:
        break

print(f"Finished reindexing. Final size of {oldest_index}: {current_size} bytes.")
