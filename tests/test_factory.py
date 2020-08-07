import pytest

from redlock_plus import LockFactory, InsufficientNodesError
import redlock_plus


def test_create(fake_redis_client):
    factory = LockFactory(
        [fake_redis_client(), fake_redis_client(), fake_redis_client()]
    )

    lock = factory("test_factory_create", ttl=500, retry_times=5, retry_delay=100)
    assert isinstance(lock, redlock_plus.Lock)

    assert factory.redis_nodes == lock.redis_nodes
    assert lock.ttl == 500
    assert lock.retry_times == 5
    assert lock.retry_delay == 100


def test_create_rlock_factory(fake_redis_client):
    factory = redlock_plus.RLockFactory(
        [fake_redis_client(), fake_redis_client(), fake_redis_client()],
    )

    lock = factory("test_factory_create")
    assert isinstance(lock, redlock_plus.RLock)


def test_custom_lock_class(fake_redis_client):
    factory = LockFactory(
        [fake_redis_client(), fake_redis_client(), fake_redis_client()],
        lock_class=redlock_plus.RLock,
    )

    lock = factory("test_factory_create")
    assert isinstance(lock, redlock_plus.RLock)


def test_insufficient_redis_nodes(fake_redis_client):
    with pytest.raises(InsufficientNodesError):
        LockFactory([fake_redis_client(), fake_redis_client()])


def test_create_from_url(fake_redis_client):
    factory = LockFactory(
        [{"url": "redis://localhost/0"}, fake_redis_client(), fake_redis_client()]
    )

    lock = factory(
        "test_factory_create_from_url", ttl=500, retry_times=5, retry_delay=100
    )

    assert factory.redis_nodes == lock.redis_nodes
    assert lock.ttl == 500
    assert lock.retry_times == 5
    assert lock.retry_delay == 100
