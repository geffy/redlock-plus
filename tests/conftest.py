import sys
from unittest.mock import MagicMock

import pytest
import fakeredis

import redlock_plus


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch):
    monkeypatch.setattr("redlock_plus.redis.StrictRedis", fakeredis.FakeStrictRedis)


@pytest.fixture()
def mock():
    return MagicMock()


@pytest.fixture
def fake_redis_client():
    def inner():
        fake_redis_server = fakeredis.FakeServer()
        return fakeredis.FakeStrictRedis(
            server=fake_redis_server, decode_responses=True
        )

    return inner


def create_lock_factory(request, redis_nodes, lock_class):
    def _lock_factory(*a, **kw):
        kw.setdefault(
            "connection_details", redis_nodes,
        )
        if len(a) < 1:
            a = (request.node.name,)
        lock = lock_class(*a, **kw)
        request.addfinalizer(lock.stop_autoextend)
        return lock

    return _lock_factory


@pytest.fixture
def create_lock(fake_redis_client, request):
    redis_clients = [fake_redis_client(), fake_redis_client(), fake_redis_client()]
    return create_lock_factory(request, redis_clients, redlock_plus.Lock)


@pytest.fixture
def create_reentrant_lock(fake_redis_client, request):
    redis_clients = [fake_redis_client(), fake_redis_client(), fake_redis_client()]
    return create_lock_factory(request, redis_clients, redlock_plus.RLock)


@pytest.fixture
def lock(create_lock):
    return create_lock()


@pytest.fixture
def rlock(create_reentrant_lock):
    return create_reentrant_lock()


def pytest_runtest_setup(item):
    version_markers = list(item.iter_markers(name="py_version"))
    version_marker = version_markers[0] if version_markers else None
    if version_marker and not eval(
        f"{tuple(sys.version_info)} {version_marker.args[0]}"
    ):
        pytest.skip(f"Skipping for version {sys.version_info[:2]}")
