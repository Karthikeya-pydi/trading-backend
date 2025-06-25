import redis
from app.core.config import settings
from typing import Dict, Any

class MockRedis:
    """Mock Redis implementation for development"""
    def __init__(self):
        self._data: Dict[str, Any] = {}
    
    def get(self, key: str):
        return self._data.get(key)
    
    def set(self, key: str, value: Any, ex: int = None):
        self._data[key] = value
        return True
    
    def delete(self, key: str):
        return self._data.pop(key, None) is not None
    
    def exists(self, key: str):
        return key in self._data

try:
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    # Test connection
    redis_client.ping()
except:
    # Fall back to mock Redis if real Redis is not available
    redis_client = MockRedis()

def get_redis():
    return redis_client
