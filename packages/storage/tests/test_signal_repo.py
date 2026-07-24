import pytest
from unittest.mock import AsyncMock, MagicMock

from storage.identifiers import RedisKey, redis_signal_pending_key
from storage.repositories.signal_repo import SignalRedisRepository

@pytest.fixture
def redis_client():
    client = MagicMock()
    
    pipe = MagicMock()
    pipe.hset = MagicMock()
    pipe.zadd = MagicMock()
    pipe.zrem = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock()
    
    client.client.pipeline.return_value.__aenter__.return_value = pipe
    client.client.hgetall = AsyncMock()
    client.client.zrevrange = AsyncMock()
    client.client.zrange = AsyncMock()
    client.client.exists = AsyncMock()
    client.client.zrem = AsyncMock()
    
    return client

@pytest.fixture
def repo(redis_client):
    return SignalRedisRepository(redis=redis_client, default_ttl_sec=1800)

class TestSignalRepo:
    @pytest.mark.asyncio
    async def test_save_pending(self, repo, redis_client):
        signal = {"signal_id": "S123", "generated_ts": 1700000000000}
        
        await repo.save_pending(signal)
        
        assert signal["status"] == "PENDING"
        assert signal["expires_ts"] == 1700000000000 + (1800 * 1000)
        
        pipe = redis_client.client.pipeline.return_value.__aenter__.return_value
        pipe.hset.assert_called_once()
        pipe.expire.assert_called_once()
        pipe.zadd.assert_called_once()
        pipe.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_approve_signal(self, repo, redis_client):
        redis_client.client.hgetall.return_value = {"status": "PENDING"}
        
        await repo.approve("S123", "O456", 1700000000100)
        
        pipe = redis_client.client.pipeline.return_value.__aenter__.return_value
        pipe.hset.assert_called_once()
        # Verify status is updated in hset mapping
        call_kwargs = pipe.hset.call_args.kwargs
        assert call_kwargs["mapping"]["status"] == "APPROVED"
        assert call_kwargs["mapping"]["approved_order_id"] == "O456"
        
        pipe.zrem.assert_called_once_with(RedisKey.SIGNAL_PENDING_INDEX, "S123")
        pipe.expire.assert_called_once_with(redis_signal_pending_key("S123"), 86400)

    @pytest.mark.asyncio
    async def test_approve_non_pending_signal_fails(self, repo, redis_client):
        redis_client.client.hgetall.return_value = {"status": "DISMISSED"}
        
        res = await repo.approve("S123", "O456", 1700000000100)
        
        assert res is None
        pipe = redis_client.client.pipeline.return_value.__aenter__.return_value
        pipe.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_dismiss_signal(self, repo, redis_client):
        redis_client.client.hgetall.return_value = {"status": "PENDING"}
        
        await repo.dismiss("S123")
        
        pipe = redis_client.client.pipeline.return_value.__aenter__.return_value
        pipe.hset.assert_called_once()
        assert pipe.hset.call_args.kwargs["mapping"]["status"] == "DISMISSED"
        pipe.zrem.assert_called_once_with(RedisKey.SIGNAL_PENDING_INDEX, "S123")
        pipe.expire.assert_called_once_with(redis_signal_pending_key("S123"), 3600)

    @pytest.mark.asyncio
    async def test_cleanup_expired(self, repo, redis_client):
        redis_client.client.zrange.return_value = ["S1", "S2", "S3"]
        # S1 exists, S2 doesn't, S3 exists
        redis_client.client.exists.side_effect = [True, False, True]
        
        cleaned = await repo.cleanup_expired()
        
        assert cleaned == 1
        redis_client.client.zrem.assert_called_once_with(RedisKey.SIGNAL_PENDING_INDEX, "S2")
