import time
from unittest.mock import call

import pytest

import redlock_plus


def test_sleep_ms(mocker):
    mock_sleep = mocker.patch("redlock_plus.time.sleep")
    redlock_plus.sleep_ms(100)
    mock_sleep.assert_called_once_with(0.1)


def test_monotonic_delta_ms(mocker):
    mock_to_ms = mocker.patch("redlock_plus._monotonic_to_ms", return_value=2)
    assert redlock_plus._monotonic_delta_ms(100, 25) == 2
    mock_to_ms.assert_called_once_with(75)


@pytest.mark.py_version("< (3, 7)")
def test_monotonic_to_ms_py_36():
    assert redlock_plus._monotonic_to_ms(1) == 1000


@pytest.mark.py_version(">= (3, 7)")
def test_monotonic_to_ms_py_37():
    assert redlock_plus._monotonic_to_ms(1_000_000) == 1


@pytest.mark.py_version("< (3, 7)")
def test_monotonic_py_36():
    assert redlock_plus.monotonic is time.monotonic


@pytest.mark.py_version(">= (3, 7)")
def test_monotonic_py_37():
    assert redlock_plus.monotonic is time.monotonic_ns


class TestInitRedisNodes:
    def test_create_instances_from_url(self, mocker, mock):
        mock_from_url = mocker.patch(
            "redlock_plus.redis.StrictRedis.from_url", return_value=mock
        )
        conf = {"url": "redis://localhost:1234/1", "foo": "bar"}
        node = redlock_plus.init_redis_nodes([conf])[0]
        assert node is mock
        mock_from_url.assert_called_once_with(conf["url"], foo="bar")

    def test_register_scripts(self, mocker):
        node = redlock_plus.redis.StrictRedis({"url": "redis://localhost:1234/1"})
        mock_register = mocker.patch(
            "redlock_plus.redis.StrictRedis.register_script", side_effect=lambda a: a
        )
        redlock_plus.init_redis_nodes([node])[0]
        mock_register.assert_has_calls(
            [
                call(redlock_plus.RELEASE_LUA_SCRIPT),
                call(redlock_plus.BUMP_LUA_SCRIPT),
                call(redlock_plus.GET_TTL_LUA_SCRIPT),
            ],
            any_order=True,
        )
        assert node.redlock_release_script == redlock_plus.RELEASE_LUA_SCRIPT
        assert node.redlock_bump_script == redlock_plus.BUMP_LUA_SCRIPT
        assert node.redlock_get_ttl_script == redlock_plus.GET_TTL_LUA_SCRIPT
