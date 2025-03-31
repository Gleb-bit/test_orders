from redis.asyncio import StrictRedis as AStrictRedis
from redis import StrictRedis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD


class RedisFactory:
    @classmethod
    def get_redis_client(cls, async_mode: bool = False, **kwargs):
        credentials = dict(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            **kwargs,
        )

        if async_mode:
            return AStrictRedis(**credentials)
        else:
            return StrictRedis(**credentials)


def get_async_redis_client() -> AStrictRedis:
    return RedisFactory.get_redis_client(async_mode=True)


def get_redis_client() -> StrictRedis:
    return RedisFactory.get_redis_client()
