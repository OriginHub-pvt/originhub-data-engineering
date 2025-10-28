"""
Simple Weaviate Test
Quick test to verify Weaviate is working correctly.
"""

import weaviate
from weaviate.classes.config import Configure, Property, DataType

print("Connecting to Weaviate...")
client = weaviate.connect_to_local(
    host="localhost",
    port=8081,
    grpc_port=50051
)

try:
    print(f"✓ Connected: {client.is_ready()}")
    
    # Create collection
    print("\nCreating test collection...")
    if client.collections.exists("SimpleTest"):
        client.collections.delete("SimpleTest")
    
    collection = client.collections.create(
        name="SimpleTest",
        vector_config=Configure.Vectors.text2vec_huggingface(
            model="sentence-transformers/all-MiniLM-L6-v2"
        ),
        properties=[
            Property(name="title", data_type=DataType.TEXT),
            Property(name="content", data_type=DataType.TEXT),
        ]
    )
    print(f"✓ Collection created: {collection.name}")
    
    # Insert data
    print("\nInserting article (this may take 10-30 seconds for first insert)...")
    article = {
        "title": "Test Article",
        "content": "This is a test of the Weaviate system"
    }
    
    uuid = collection.data.insert(properties=article)
    print(f"✓ Inserted article with UUID: {uuid}")
    
    # Query data
    print("\nQuerying data...")
    response = collection.query.fetch_objects(limit=10)
    print(f"✓ Found {len(response.objects)} articles")
    
    for obj in response.objects:
        print(f"  - {obj.properties['title']}")
    
    # Semantic search
    print("\nTesting semantic search...")
    response = collection.query.near_text(
        query="test system",
        limit=1
    )
    print(f"✓ Semantic search found {len(response.objects)} results")
    
    # Cleanup
    print("\nCleaning up...")
    client.collections.delete("SimpleTest")
    print("✓ Test collection deleted")
    
    print("\n" + "="*60)
    print("ALL TESTS PASSED!")
    print("="*60)
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    client.close()
    print("\nConnection closed")

