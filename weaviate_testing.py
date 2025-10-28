"""
Weaviate Testing - Incremental Development
Test each section by uncommenting code step by step.
"""

import weaviate
from weaviate.classes.config import Configure, Property, DataType
from datetime import datetime

from weaviate.collections import collection

def create_collection(name: str, model: str):
    return client.collections.create(
        name = name,
        vector_config=Configure.Vectors.text2vec_huggingface(
            model=model 
        )
    )
    
def insert_article(collection_name: str, article_data: dict) -> str:
    articles = client.collections.get(collection_name)
    uuid = articles.data.insert(properties=article_data)
    return str(uuid)

# Connect to Weaviate
client = weaviate.connect_to_local(
    host="localhost",
    port=8081,
    grpc_port=50051
)

try:
    testing = create_collection("testing","sentence-transformers/all-MiniLM-L6-v2")
    
    print(testing.name)
    
    sample_article = {
    "title": "Introduction to Machine Learning",
    "content": "Machine learning is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed. It focuses on developing computer programs that can access data and use it to learn for themselves.",
    "url": "https://example.com/ml-intro",
    "source": "TechBlog"
    }
    
    id =  insert_article("testing", sample_article)
    
    testing = client.collections.get("testing")
    
    if testing.query.fetch_object_by_id(id):
        print("Balle balle")
    
    obj = testing.query.fetch_object_by_id(id, include_vector=True)

    print(obj.vector['default'])
    
    client.collections.delete("testing")
    
    print("deleted collection")
    
    print(client.collections.list_all())
    
finally:
    client.close()
    print("\nConnection closed")
