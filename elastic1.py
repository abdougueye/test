from elasticsearch import Elasticsearch
import time

# Connect to Elasticsearch cluster
es = Elasticsearch(["http://localhost:9200"])

# Parameters
MAX_SIZE_GB = 50  # Max size of the target index
THRESHOLD_SIZE_GB = 1  # Size threshold to reindex (in GB)

def get_frozen_indices():
    """Get a list of indices in the frozen tier."""
    response = es.cat.indices(index='*', h=['index', 'status', 'store.size'], format='json')
    frozen_indices = [index for index in response if 'frozen' in index['status'].lower()]
    return frozen_indices

def get_index_size(index_name):
    """Return the size of an index in GB."""
    stats = es.indices.stats(index=index_name)
    size_bytes = stats['_all']['total']['store']['size_in_bytes']
    size_gb = size_bytes / (1024 ** 3)  # Convert bytes to GB
    return size_gb

def get_oldest_index(frozen_indices):
    """Return the oldest index (based on creation date)."""
    frozen_indices.sort(key=lambda x: x['index'], reverse=False)  # Assuming index names contain creation dates
    return frozen_indices[0]

def reindex_data(source_index, target_index):
    """Reindex data from the source index to the target index."""
    reindex_body = {
        "source": {
            "index": source_index
        },
        "dest": {
            "index": target_index
        }
    }
    es.reindex(body=reindex_body)
    print(f"Reindexed data from {source_index} to {target_index}")

def main():
    frozen_indices = get_frozen_indices()
    small_indices = [index for index in frozen_indices if get_index_size(index['index']) < THRESHOLD_SIZE_GB]
    
    if not small_indices:
        print("No frozen indices smaller than 1GB found.")
        return
    
    # Find the oldest index
    oldest_index = get_oldest_index(small_indices)
    print(f"Oldest index in frozen tier: {oldest_index['index']}")

    # Check the size of the oldest index
    oldest_index_size = get_index_size(oldest_index['index'])
    
    while oldest_index_size < MAX_SIZE_GB:
        # Find the newest small index
        newer_indices = [index for index in frozen_indices if get_index_size(index['index']) < MAX_SIZE_GB]
        newer_indices.sort(key=lambda x: x['index'], reverse=True)
        
        if not newer_indices:
            print("No more small indices to reindex.")
            break
        
        newest_index = newer_indices[0]
        print(f"Reindexing from {newest_index['index']} to {oldest_index['index']}...")

        reindex_data(newest_index['index'], oldest_index['index'])
        
        # Wait for a few seconds to ensure reindexing has completed (optional)
        time.sleep(10)
        
        # Update the size of the oldest index
        oldest_index_size = get_index_size(oldest_index['index'])
        
        if oldest_index_size >= MAX_SIZE_GB:
            print(f"Oldest index has reached {MAX_SIZE_GB}GB.")
            break

if __name__ == "__main__":
    main()
