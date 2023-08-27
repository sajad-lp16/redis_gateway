from time import time

from django_redis import get_redis_connection

from utils import shared_classes


class RedisGateway(shared_classes.AbstractPropertySingleton):
    def __init__(self, cache_name: str) -> None:
        self._db = get_redis_connection()
        self._prefix_key = cache_name

    @staticmethod
    def _perform_pythonic_data(data, cast, many=False):
        if many:
            return tuple(map(lambda item: cast(item.decode()), data))
        return cast(data.decode())

    def _combine_key_and_prefix(self, key: str) -> str:
        return f"{self._prefix_key}:{key}"

    def _set_timeout(self, cache_key: str, timeout: int or None):
        if timeout is not None:
            self._db.expire(cache_key, timeout)

    def _perform_db_set(self, key: str, *value, timeout, is_array):
        cache_key = self._combine_key_and_prefix(key)
        if is_array:
            self._db.lpush(cache_key, *value)
            return self._set_timeout(cache_key, timeout)
        self._db.set(cache_key, value[0], timeout)

    def _perform_db_get(self, key: str, cast, is_array: bool, start: int, stop: int):
        cache_key = self._combine_key_and_prefix(key)
        if is_array:
            data = self._db.lrange(cache_key, start, stop)
            return self._perform_pythonic_data(data, cast, many=True)
        value = self._db.get(cache_key)
        if value is not None:
            return self._perform_pythonic_data(value, cast)

    def _perform_incr(self, key, value, timeout) -> None:
        cache_key = self._combine_key_and_prefix(key)
        if self.get(key, cast=int) is None:
            count = self._db.incr(cache_key, value)
            self._set_timeout(cache_key, timeout)
            return count
        return self._db.incr(cache_key, value)

    def _perform_ordered_set_get(
            self, ordered_set_name, min_score, max_score, cast, rm_out_range
    ):
        cache_key = self._combine_key_and_prefix(ordered_set_name)
        if rm_out_range:
            self._db.zremrangebyscore(cache_key, 0, min_score)
        data = self._db.zrangebyscore(cache_key, min_score, max_score)
        return self._perform_pythonic_data(data, cast, many=True)

    def _perform_set_add(self, ordered_set_name, key, score):
        cache_key = self._combine_key_and_prefix(ordered_set_name)
        user_score_mapping = {key: score or "inf"}
        self._db.zadd(cache_key, user_score_mapping)
        max_score = self._db.zrange(cache_key, 0, -1, 1, 1)[0][1]
        try:
            ordered_set_life_cycle = abs(int(max_score) - int(time()))
            self._set_timeout(cache_key, ordered_set_life_cycle + 5)
        except OverflowError:
            pass

    # ============================================== REACHABLE FUNCTIONS ===============================================
    def get(
            self, key: str, cast: type, is_array=False, start=0, stop=-1
    ) -> int or str or tuple:
        return self._perform_db_get(key, cast, is_array, start, stop)

    def set(self, key: str, *value, timeout=None, is_array=False) -> None:
        self._perform_db_set(key, *value, timeout=timeout, is_array=is_array)

    def incr(self, key: str, value=1, timeout=None) -> None:
        return self._perform_incr(key, value, timeout)

    def zadd(self, ordered_set_name, key, score):
        return self._perform_set_add(ordered_set_name, key, score)

    def zget(
            self,
            ordered_set_name: str,
            min_score: int,
            max_score,
            cast: type,
            rm_out_range=True,
    ):
        return self._perform_ordered_set_get(
            ordered_set_name, min_score, max_score, cast, rm_out_range
        )

    def get_key_ttl(self, key):
        cache_key = self._combine_key_and_prefix(key)
        return int(self._db.ttl(cache_key))

