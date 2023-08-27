import pickle
import logging

from django_redis import get_redis_connection

logger = logging.getLogger("redis")


class RedisGateway:
    def __init__(self, cache_name: str) -> None:
        self._db = get_redis_connection()
        self._prefix_key = cache_name

    @staticmethod
    def _touch_redis_db(partial_function):
        try:
            return partial_function()
        except Exception as err:
            logger.warning(str(err))
            raise

    @staticmethod
    def _parse_2_pythonic_data(data, cast, many=False):
        if many:
            return tuple(map(lambda item: cast(item.decode()), data))
        return cast(data.decode())

    def _combine_key_and_prefix(self, key: str) -> str:
        return f"{self._prefix_key}:{key}"

    def _set_timeout(self, cache_key: str, timeout: int or None):
        def _action():
            if timeout is not None:
                self._db.expire(cache_key, timeout)

        return self._touch_redis_db(_action)

    def _perform_db_set(self, key: str, value, timeout):
        def _action():
            cache_key = self._combine_key_and_prefix(key)
            self._db.set(cache_key, value, timeout)

        return self._touch_redis_db(_action)

    def _perform_db_get(self, key: str, cast):
        def _action():
            cache_key = self._combine_key_and_prefix(key)
            value = self._db.get(cache_key)
            if value is not None:
                return self._parse_2_pythonic_data(value, cast)

        return self._touch_redis_db(_action)

    def _perform_incr(self, key, value, timeout) -> int:
        def _action():
            cache_key = self._combine_key_and_prefix(key)
            if self.get(key, cast=int) is None:
                count = self._db.incr(cache_key, value)
                self._set_timeout(cache_key, timeout)
                return count
            return self._db.incr(cache_key, value)

        return self._touch_redis_db(_action)

    def _perform_xget(self, key, combine):
        def _action():
            cache_key = key
            if combine:
                cache_key = self._combine_key_and_prefix(key)
            if (data := self._db.get(cache_key)) is None:
                return
            return pickle.loads(data)

        return self._touch_redis_db(_action)

    def _perform_xset(self, key, value):
        def _action():
            cache_key = self._combine_key_and_prefix(key)
            return self._db.set(cache_key, pickle.dumps(value))

        return self._touch_redis_db(_action)

    def _perform_remove_by_pattern(self, pattern):
        def _action():
            q_pattern = f"*{pattern}*"
            cache_key = self._combine_key_and_prefix(q_pattern)
            keys = self._db.keys(cache_key)
            for key in keys:
                self._db.delete(key)

        return self._touch_redis_db(_action)

    def _perform_delete(self, key):
        def _action():
            cache_key = self._combine_key_and_prefix(key)
            self._db.delete(cache_key)

        return self._touch_redis_db(_action)

    def _get_pattern(self, key):
        def _action():
            cache_key = self._combine_key_and_prefix(key)
            return self._db.keys(cache_key)

        return self._touch_redis_db(_action)

    # ============================================== REACHABLE FUNCTIONS ===============================================
    def get(self, key: str, cast: type) -> str | int:
        return self._perform_db_get(key, cast)

    def remove_by_pattern(self, pattern):
        return self._perform_remove_by_pattern(pattern)

    def set(self, key: str, value, timeout=None) -> None:
        self._perform_db_set(key, value, timeout=timeout)

    def incr(self, key: str, value=1, timeout=None) -> int:
        return self._perform_incr(key, value, timeout)

    def xget(self, key: str, combine=True):
        return self._perform_xget(key, combine)

    def xset(self, key, value):
        self._perform_xset(key, value)

    def delete_key(self, key):
        self._perform_delete(key)

    def get_pattern(self, key):
        return self._get_pattern(key)

