from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

client = QdrantClient(
    host="qdrant",
    port=6333
)

client.create_collection(
    collection_name="real_estate",
    vectors_config=VectorParams(size=384, distance=Distance.COSINE),
)