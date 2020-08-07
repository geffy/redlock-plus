"""
Distributed locks using Redis.
An Implementation of the `Redlock <http://redis.io/topics/distlock>`_ algorithm.
"""

import sys
import time
import uuid
import random
import threading
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Union,
    Optional,
    Tuple,
    List,
    Any,
    Dict,
    Type,
    Iterator,
    Callable,
    TypeVar,
    cast,
)

if sys.version_info >= (3, 7):
    from time import monotonic_ns as monotonic  # pylint: disable=no-name-in-module

    def _monotonic_to_ms(_time: float) -> float:
        return _time / 1_000_000


else:
    from time import monotonic

    def _monotonic_to_ms(_time: float) -> float:
        return _time * 1000


if sys.version_info >= (3, 8):
    import importlib.metadata as importlib_metadata  # pylint: disable=no-name-in-module, import-error  # noqa: E501
else:
    import importlib_metadata  # type: ignore # pylint: disable=import-error

import redis  # pylint: disable=wrong-import-position


__version__ = importlib_metadata.version("redlock-plus")


DecoratorT = TypeVar("DecoratorT", bound=Callable[..., Any])


def _monotonic_ms() -> float:
    """
    Return the current monotonic time in milliseconds using the most precise source
    available. On Python => 3.7 use :func:`time.monotonic_ns`, below use
    :func:`time.monotonic`
    """
    return _monotonic_to_ms(monotonic())


def _monotonic_delta_ms(time_a: float, time_b: float) -> float:
    """
    Return the delta between two monotonic points in time acquired with
    :func:`monotonic` in milliseconds. Always use this function when computing the delta
    between two monotonic times since this assures the returned value will be in
    milliseconds independent from the predicion of the monotonic source.
    """
    return _monotonic_to_ms(time_a - time_b)


def sleep_ms(milliseconds: float) -> None:
    """
    Convenience wrapper around :func:`time.sleep` that accepts input in miliseconds
    """
    time.sleep(milliseconds / 1000)


CLOCK_DRIFT_FACTOR: float = 0.01

# Reference:  http://redis.io/topics/distlock
# Section Correct implementation with a single instance
RELEASE_LUA_SCRIPT: str = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
"""

GET_TTL_LUA_SCRIPT: str = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("pttl",KEYS[1])
    else
        return 0
    end
"""

# Reference:  http://redis.io/topics/distlock
# Section Making the algorithm more reliable: Extending the lock
BUMP_LUA_SCRIPT: str = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("pexpire",KEYS[1],ARGV[2])
    else
        return 0
    end
"""


class RedlockError(Exception):
    """
    Base redlock exception
    """


class InvalidOperationError(RedlockError):
    """
    An operation was performed on the lock which the current state of the lock does not
    allow
    """


class InsufficientNodesError(RedlockError):
    """
    Raised if the minimum amount of 3 nodes was not met
    """

    def __init__(self, node_count: int, *args: Any) -> None:
        msg = (
            "At least 3 redis nodes are required for redlock to work, got "
            f"{node_count}. If you need a distributed lock with lesser guarantees, "
            "consider using a simpler mechanism such as offered by py-redis' "
            "https://redis-py.readthedocs.io/en/stable/#redis.Redis.lock"
        )
        super().__init__(msg, *args)


class _AutoextendThread(threading.Thread):
    def __init__(
        self, lock: "Lock", timeout: Optional[float] = None,
    ):
        """

        :param lock: Lock instance
        :param interval: Duration between checks
        :param timeout: Maximum time after which the lock will not be renewed again in
            seconds
        """
        self.lock = lock
        self.timeout_ms = timeout * 1000 if timeout else None
        self.released: threading.Event = threading.Event()
        super().__init__(daemon=True)

    def run(self) -> None:
        time_start = monotonic()
        expected_ttl = min(self.lock.check_times()[1] or [0])
        while not self.released.is_set():
            ms_to_wait: float = expected_ttl * 0.75
            if (
                not expected_ttl >= 2  # sleep for at least 2ms to account for overhead
                or (
                    self.timeout_ms
                    and _monotonic_delta_ms(monotonic() + ms_to_wait, time_start)
                )
            ):
                break
            self.released.wait(ms_to_wait / 1000)
            expected_ttl = self.lock.extend()


def init_redis_nodes(
    connection_details: List[Dict[str, Any]]
) -> List[redis.StrictRedis]:
    """
    Initialise redis nodes by adding lua scripts to release, bump and check locks.
    If passed a list of dictionaries, create :class:`redis.StrictRedis` instances
    from them first.
    """

    redis_nodes: List[redis.StrictRedis] = []

    for conn in connection_details:
        if isinstance(conn, redis.StrictRedis):
            node = conn
        elif "url" in conn:
            conn = {**conn}
            node = redis.StrictRedis.from_url(conn.pop("url"), **conn)
        else:
            node = redis.StrictRedis(**conn)
        node.redlock_release_script = node.register_script(RELEASE_LUA_SCRIPT)  # type: ignore # noqa: E501
        node.redlock_bump_script = node.register_script(BUMP_LUA_SCRIPT)  # type: ignore
        node.redlock_get_ttl_script = node.register_script(GET_TTL_LUA_SCRIPT)  # type: ignore # noqa: E501
        redis_nodes.append(node)
    return redis_nodes


class Lock:
    """
    A distributed lock implementation based on Redis.
    It shares the same API as Python's :class:`threading.Lock` and behaves mostly the
    same. Generally this can be used as a drop-in replacement, given the lock is
    provided from a :class:`LockFactory`.

    The lock supports the context manager protocol, which is the preferred way to use it
    ::

        with Lock("my_resource"):
            # do some work

        # this is equivalent to

        lock = Lock("my_resource")
        lock.acquire()
        # do some stuff
        lock.release()

    :param resource_name: Global identifier to be used for the lock. This will be shared
        across all redis nodes
    :param connection_details: A list containing either redis client instances or dicts
        that can be used to create a redis client. If `None`, `nodes` must not be `None`
    :param nodes: A list containing already initialised redis nodes. Takes precedence
        over `connection_details`. If `None`, `connection_details` must not be `None`
    :param retry_times: Amount of times to retry acquiring a lock after a failed attempt
    :param retry_delay: Time in milliseconds between retry attempts to acquire a lock
    :param ttl: Time in seconds until the lock should expire. This should be set to a
        relatively high amount compared to the time it takes to complete the work for
        which the lock should be held. Default is 120_000 milliseconds (2 minutes)
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        resource_name: str,
        connection_details: Union[List[Dict[str, Any]], None] = None,
        nodes: Optional[List[redis.StrictRedis]] = None,
        retry_times: int = 3,
        retry_delay: int = 200,
        ttl: int = 120_000,
    ):
        # pylint: disable=too-many-arguments
        self.lock_key: Optional[str] = None
        self.resource_name = resource_name
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.ttl = ttl
        self._autoextend_thread: Optional[_AutoextendThread] = None

        if nodes is None:
            if connection_details is None:
                raise ValueError(
                    "Either 'connection_details' or 'nodes' must be specified"
                )
            nodes = init_redis_nodes(connection_details)

        if len(nodes) < 3:
            raise InsufficientNodesError(len(nodes))

        self.redis_nodes: List[redis.StrictRedis] = nodes
        self.quorum: int = max(3, len(self.redis_nodes) // 2 + 1)

    def __enter__(self) -> float:
        return self.acquire()

    def __exit__(self, *a: Any) -> None:
        self.release()

    def _requires_key(func: DecoratorT) -> DecoratorT:  # type: ignore
        """
        Mark a function as requiring :attr:`Lock.key` to be set.
        If the function is called without the key being present, raise an
        `InvalidOperationError`

        The typing is currently a bit whacky, since mypy doesn't have great support for
        constructs like this. See https://github.com/python/mypy/issues/7778,
        https://github.com/python/mypy/issues/1927

        """
        # pylint: disable=no-self-argument,not-callable
        @functools.wraps(func)
        def wrapped(self, *args, **kwargs):  # type: ignore
            if not self.lock_key:
                raise InvalidOperationError("Invalid operation on not-acquired lock")
            return func(self, *args, **kwargs)

        return cast(DecoratorT, wrapped)

    @_requires_key
    def _acquire_node(self, node: redis.StrictRedis) -> bool:
        """
        Attempt to lock a single redis node

        :param node: An initialised redis client instance
        :returns: `True` if the node was locked successfully, `False` otherwise
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        try:
            return bool(
                node.set(self.resource_name, self.lock_key, nx=True, px=self.ttl)  # type: ignore # noqa: E501
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return False

    @_requires_key
    def _release_node(self, node: redis.StrictRedis) -> bool:
        """
        Release a single redis node

        :param node: An initialised redis client instance
        :returns: `True` if the node was released successfully, `False` otherwise
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        try:
            return node.redlock_release_script(  # type: ignore
                keys=[self.resource_name], args=[self.lock_key]
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return False

    @_requires_key
    def _bump_node(self, node: redis.StrictRedis) -> bool:
        """
        Update the ttl of a single redis node

        :param node: An initialised redis client instance
        :returns: `True` if the ttl was updated successfully, `False` otherwise
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        try:
            return node.redlock_bump_script(  # type: ignore
                keys=[self.resource_name], args=[self.lock_key, self.ttl]
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return False

    @_requires_key
    def _get_ttl_from_node(self, node: redis.StrictRedis) -> Union[float, None]:
        """
        Get the ttl of a single redis node

        :param node: An initialised redis client instance
        :returns: Time to live in milliseconds as a `float` if the request was s
            uccessful, `None` otherwise
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        try:
            return node.redlock_get_ttl_script(  # type: ignore # noqa: E501
                keys=[self.resource_name], args=[self.lock_key]
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return None

    def _map_nodes(
        self, func: Callable, nodes: Optional[List[redis.StrictRedis]] = None
    ) -> Iterator[Any]:
        """
        Apply a function to redis nodes asynchronously using
        :meth:`concurrent.futures.ThreadPoolExecutor.map`

        :param func: Callable that accepts a node as its first parameter
        :param nodes: Redis nodes to map. Defaults to :attr:`Lock.redis_nodes`
        :returns: Result iterator for the created futures
        """
        if nodes is None:
            nodes = self.redis_nodes
        with ThreadPoolExecutor() as executor:
            return executor.map(func, nodes)

    def start_autoextend(self, timeout: Optional[float] = None) -> threading.Thread:
        """
        Start an autoextending thread which will attempt to extedn the lock at 3/4 of
        its expected ttl.

        :param timeout: Timeout in seconds until the thread terminates. If `None`, the
            lock will be extended indefinitely (i.e. as long as the main thread is
            running)
        :returns: The autoextending thread
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        if not self.locked():
            raise InvalidOperationError("Cannot autoextend un-acquired lock")
        self.stop_autoextend()
        self._autoextend_thread = _AutoextendThread(self, timeout=timeout)
        self._autoextend_thread.start()
        return self._autoextend_thread

    def stop_autoextend(self) -> None:
        """
        Stop the autoextending thread
        """
        if self._autoextend_thread:
            self._autoextend_thread.released.set()
            self._autoextend_thread = None

    def _acquire(self, retry_times: Optional[int] = None) -> float:
        """
        Perform the actions necessary to acquire a lock.
        If the lock could not be acquired, release all possibly acquired nodes. If
        `retry_times` is set to a positive integer, sleep :attr:`Lock.retry_delay`
        miliseconds before trying to acquire it again.

        :param retry_times: Amount of times to retry after a failed attempt to acquire.
            Defaults to :attr:`Lock.retry_times`
        :returns: A float indicating the minimal time in milliseconds the lock can be
            considered held in case the lock could be acquired, else `False`
        """
        retry_times = retry_times or self.retry_times
        for _ in range(retry_times + 1):
            previous_lock_key = self.lock_key
            self.lock_key = uuid.uuid4().hex
            acquired_node_count = 0
            start_time = monotonic()

            for node in self.redis_nodes:
                if self._acquire_node(node):
                    acquired_node_count += 1
            end_time = monotonic()
            elapsed_milliseconds = _monotonic_delta_ms(end_time, start_time)

            # Add 2 milliseconds to the drift to account for Redis expires
            # precision, which is 1 milliescond, plus 1 millisecond min drift
            # for small TTLs.
            drift = (self.ttl * CLOCK_DRIFT_FACTOR) + 2

            validity = self.ttl - (elapsed_milliseconds + drift)

            if acquired_node_count >= self.quorum and validity > 0:
                return validity

            self._map_nodes(self._release_node)
            self.lock_key = previous_lock_key
            sleep_ms(random.randint(0, self.retry_delay))
        return False

    def _acquire_blocking(self, timeout: float = -1) -> float:
        """
        Make a call to :meth:`Lock._acquire` blocking

        :param timeout: If set to a positive value, block for at most this many
            seconds. If set to `-1`, block indefinitely
        :returns: Minimal ttl in milliseconds if a lock could be acquired, else 0
        """
        validity = 0.0
        timeout_ms = timeout * 1000 if timeout > 0 else 0

        time_start = monotonic()
        while not validity:  # try to acquire until success or timeout exceeded
            ms_elapsed = _monotonic_delta_ms(monotonic(), time_start)
            estimated_ms_next_round = ms_elapsed + self.retry_delay
            if timeout_ms and estimated_ms_next_round >= timeout_ms:
                break
            validity = self._acquire(retry_times=0)  # no need to retry in _acquire
        return validity

    def acquire(
        self,
        blocking: bool = True,
        timeout: float = -1,
        autoextend: bool = True,
        autoextend_timeout: Optional[float] = None,
    ) -> float:
        """
        Attempt to acquire a new lock, blocking or non-blocking.

        :param blocking: If `True`, block until the lock can be acquired
        :param timeout: If `blocking` is `True` and `timeout` is a positive value,
            in the case a request would block, block at most `timeout` seconds
        :param autoextend: If `True` start a thread once the lock is acquired that will
            attempt to extend the lock at 3/4 of its expected ttl
        :param autoextend_timeout: Timeout in seconds after which the autoextend thread
            will terminate regardless of the lock status
        :returns: A float indicating the minimal time the lock can be considered held in
            milliseconds in case the lock could be acquired, else `False`
        :raises ValueError: If `blocking` is `False` and `timeout` is a positive value
        """
        if blocking:
            validity = self._acquire_blocking(timeout=timeout)
        else:
            if timeout != -1:
                raise ValueError("Timout must be -1 when requiring non-blocking")
            validity = self._acquire()
        if autoextend and validity:
            self.start_autoextend(timeout=autoextend_timeout)

        return validity

    @_requires_key
    def extend(self) -> Union[bool, float]:
        """
        Extend an acquired lock.

        :returns: A float indicating the minimal time the lock can be considered held
            in milliseconds in case the lock could be acquired, else `False`
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        for _ in range(self.retry_times + 1):
            start_time = monotonic()
            bumped_count = len([n for n in self._map_nodes(self._bump_node) if n])
            end_time = monotonic()
            elapsed_milliseconds = _monotonic_delta_ms(end_time, start_time)
            drift = (self.ttl * CLOCK_DRIFT_FACTOR) + 2
            validity = self.ttl - (elapsed_milliseconds + drift)
            if bumped_count >= self.quorum and validity > 0:
                return validity
            sleep_ms(random.randint(0, self.retry_delay))
        return False

    def acquire_or_extend(
        self,
        blocking: bool = True,
        timeout: float = -1,
        autoextend: bool = True,
        autoextend_timeout: Optional[float] = None,
    ) -> float:
        """
        If the lock is currently not held, try to acquire it. If it is held, extend it.
        `blocking`, `timeout`, `autoextend` and `autoextend_timeout` will be passed to
        :meth:`Lock.acquire`.

        :returns: A float indicating the minimal time the lock can be considered held
            in milliseconds in case the lock could be acquired, else `False`
        """
        if self.locked():
            extended = self.extend()
            if extended:
                return extended
        return self.acquire(
            blocking=blocking,
            timeout=timeout,
            autoextend=autoextend,
            autoextend_timeout=autoextend_timeout,
        )

    @_requires_key
    def check_times(self) -> Tuple[bool, List[float]]:
        """
        Check if the lock is still held and the time to live on each node, accounting
        for clock drift and request time.

        :returns: A tuple consisting of a boolean, indicating if the lock can still be
            considered held and a list of the reported times in milliseconds of each
            node
        :raises InvalidOperationError: If the lock was not previously acquired
        """
        start_time = monotonic()
        reported_ttls: List[float] = [
            node_ttl
            for node_ttl in self._map_nodes(self._get_ttl_from_node)
            if node_ttl and node_ttl > 0
        ]
        end_time = monotonic()
        drift = (self.ttl * CLOCK_DRIFT_FACTOR) + 2
        elapsed_milliseconds = _monotonic_delta_ms(end_time, start_time)
        # Compute times taking into account how long it took to query all
        # the nodes as well as clock drift constant. Sort out any negative
        # times (keys that may have expired while we were querying other nodes).
        times = [
            float(ttl - (elapsed_milliseconds + drift))
            for ttl in reported_ttls
            if ttl - (elapsed_milliseconds + drift) > 0
        ]
        if len(times) > 0:
            return min(times) > 0 and len(times) >= self.quorum, times
        return False, []

    @_requires_key
    def release(self) -> bool:
        """
        Release the lock.
        A lock is considered released if the action to release it could be performed
        sucessfully on the majority of redis nodes. So unlike Python's
        :class:`threading.Lock` this can fail for reasons other than the lock not being
        held in the first place.

        :raises InvalidOperationError: If the lock was not previously acquired
        :returns: Whether or not the lock was successfully released
        """
        self.stop_autoextend()
        released_nodes = self._map_nodes(self._release_node)
        return len([x for x in released_nodes if x]) >= self.quorum

    def locked(self) -> bool:
        """
        Check if the lock is still held.

        :returns: `True` if the lock has previously been acquired and the smallest
            reported time to live of any node is positive, `False` otherwise.
        """
        return self.lock_key is not None and self.check_times()[0]


class RLock(Lock):
    """
    A reentrant version of :class:`Lock`, the only difference being that calls to
    `acquire` / `release` may be nested. Only the final call to `acquire` actually
    releases the lock.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._acquired = 0

    def acquire(
        self,
        blocking: bool = True,
        timeout: float = -1,
        autoextend: bool = False,
        autoextend_timeout: Optional[float] = None,
    ) -> float:
        """
        Attempt to acquire a new lock and / or increment the recursion level. If the
        recursion level was `0`, acquire a lock, otherwise just increase the recursion
        level.

        :param blocking: If `True`, block until the lock can be acquired
        :param timeout: If `blocking` is `True` and `timeout` is a positive value,
            in the case a request would block, block at most `timeout` seconds
        :param autoextend: If `True` start a thread once the lock is acquired that will
            attempt to extend the lock at 3/4 of its expected ttl
        :param autoextend_timeout: Timeout in seconds after which the autoextend thread
            will terminate regardless of the lock status
        :returns: A float indicating the minimal time the lock can be considered held in
            milliseconds in case the lock could be acquired, else `False`
        :raises ValueError: If `blocking` is `False` and `timeout` is a positive value
        :raises RedlockError: The recursion level should be increased but the lock was
            lost in the meantime
        """
        if self._acquired > 0:
            locked, validity_times = self.check_times()
            if not locked:
                raise RedlockError("Lost rlock while re-acquiring")
            self._acquired += 1
            return min(validity_times)

        acquired = super().acquire(
            blocking=blocking,
            timeout=timeout,
            autoextend=autoextend,
            autoextend_timeout=autoextend_timeout,
        )
        if acquired:
            self._acquired += 1
        return acquired

    def acquire_or_extend(
        self,
        blocking: bool = True,
        timeout: float = -1,
        autoextend: bool = True,
        autoextend_timeout: Optional[float] = None,
    ) -> float:
        """
        If the lock is currently not held, try to acquire it. If it is held, extend it.
        `blocking`, `timeout`, `autoextend` and `autoextend_timeout` will be passed to
        :meth:`Lock.acquire`. If a lock was acquired or extended, increase the recursion
        level.

        :returns: A float indicating the minimal time the lock can be considered held in
            milliseconds in case the lock could be acquired, else `False`
        """
        if self.locked():
            extended = self.extend()
            if extended:
                self._acquired += 1
                return extended
        return self.acquire(blocking=blocking, timeout=timeout)

    @Lock._requires_key
    def release(self) -> bool:
        """
        Release the lock and / or decrement the recursion level. If the recursion level
        reaches `0` release the lock.
        A lock is considered released if the action to release it could be performed
        sucessfully on the majority of redis nodes. So unlike Python's
        :class:`threading.RLock` this can fail for reasons other than the lock not being
        held in the first place.

        :raises InvalidOperationError: If the lock was not previously acquired
        :returns: Whether or not the lock was successfully released
        """
        if self._acquired > 0:
            self._acquired -= 1
            if self._acquired == 0:
                return super().release()
            return True
        raise InvalidOperationError("Cannot release un-acquired lock")


class LockFactory:
    # pylint: disable=too-few-public-methods
    """
    Create new :class:`Lock` instances from a fixed configuration.

    :param connection_details: An iterable of connection parameters. See
        :class:`Lock` for details
    :param kwargs: Default values for keyword arguments to pass to each created
        :class:`Lock` instance
    """

    lock_class: Type[Lock] = Lock

    def __init__(
        self,
        connection_details: List[Dict[str, Any]],
        lock_class: Optional[Type[Lock]] = None,
        **kwargs: Any,
    ):
        if len(connection_details) < 3:
            raise InsufficientNodesError(len(connection_details))
        if lock_class is not None:
            self.lock_class = lock_class
        self.redis_nodes = init_redis_nodes(connection_details)
        self.lock_kwargs = kwargs

    def __call__(self, resource_name: str, **kwargs: Any) -> "Lock":
        """
        Create a new :class:`Lock` object and reuse stored Redis clients.
        Takes the same arguments as :class:`Lock`
        """
        lock_kwargs = {**self.lock_kwargs}
        lock_kwargs.update(kwargs)
        return self.lock_class(
            resource_name=resource_name, nodes=self.redis_nodes, **lock_kwargs
        )


class RLockFactory(LockFactory):
    # pylint: disable=too-few-public-methods
    """
    Convenience subclass of :class:`LockFactory`, to create RLocks.
    """
    lock_class: Type[RLock] = RLock
