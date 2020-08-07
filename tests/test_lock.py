from unittest.mock import MagicMock, call
from time import sleep, monotonic

import redis
from pytest import fixture, raises
from pytest import mark

from redlock_plus import (
    CLOCK_DRIFT_FACTOR,
    InsufficientNodesError,
    Lock,
    InvalidOperationError,
)


class TestInitialisation:
    def test_insufficient_nodes(self, create_fake_nodes):
        with raises(InsufficientNodesError):
            Lock("insufficient_nodes", nodes=create_fake_nodes(2))

    def test_insufficient_nodes_from_connection_details(self):
        with raises(InsufficientNodesError):
            Lock("insufficient_nodes", connection_details=[{}, {}])

    def test_nodes_and_connection_details_none(self):
        with raises(ValueError):
            Lock("insufficient_nodes")

    def test_quorum(self, create_fake_nodes):
        lock = Lock("insufficient_nodes", create_fake_nodes(5))
        assert len(lock.redis_nodes) == 5
        assert lock.quorum == 3

        lock = Lock("insufficient_nodes", create_fake_nodes(7))
        assert len(lock.redis_nodes) == 7
        assert lock.quorum == 4

    def test_quorum_min_3(self, create_fake_nodes):
        lock = Lock("insufficient_nodes", create_fake_nodes(3))
        assert len(lock.redis_nodes) == 3
        assert lock.quorum == 3


class TestAcquireNode:
    def test_acquire_node(self, lock, mock):
        lock.lock_key = "foo"
        mock.set.return_value = True
        assert lock._acquire_node(mock) is True
        mock.set.assert_called_once_with(
            "test_acquire_node", "foo", nx=True, px=lock.ttl
        )

    def test_acquire_node_redis_raises_connection_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.set.side_effect = redis.exceptions.ConnectionError
        assert lock._acquire_node(mock) is False

    def test_acquire_node_redis_raises_timout_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.set.side_effect = redis.exceptions.TimeoutError
        assert lock._acquire_node(mock) is False


class TestReleaseNode:
    def test_release_node(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_release_script.return_value = True
        assert lock._release_node(mock) is True
        mock.redlock_release_script.assert_called_once_with(
            keys=["test_release_node"], args=["foo"]
        )

    def test_release_node_redis_raises_connection_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_release_script.side_effect = redis.exceptions.ConnectionError
        assert lock._release_node(mock) is False

    def test_release_node_redis_raises_timout_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_release_script.side_effect = redis.exceptions.TimeoutError
        assert lock._release_node(mock) is False


class TestBumpNode:
    def test_bump_node(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_bump_script.return_value = True
        assert lock._bump_node(mock) is True
        mock.redlock_bump_script.assert_called_once_with(
            keys=["test_bump_node"], args=["foo", lock.ttl]
        )

    def test_bump_node_redis_raises_connection_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_bump_script.side_effect = redis.exceptions.ConnectionError
        assert lock._bump_node(mock) is False

    def test_bump_node_redis_raises_timout_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_bump_script.side_effect = redis.exceptions.TimeoutError
        assert lock._bump_node(mock) is False


class TestGetTtlFromNode:
    def test_get_ttl_from_node(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_get_ttl_script.return_value = 1.0
        assert lock._get_ttl_from_node(mock) == 1.0
        mock.redlock_get_ttl_script.assert_called_once_with(
            keys=["test_get_ttl_from_node"], args=["foo"]
        )

    def test_get_ttl_from_node_redis_raises_connection_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_get_ttl_script.side_effect = redis.exceptions.ConnectionError
        assert lock._get_ttl_from_node(mock) is None

    def test_get_ttl_from_node_redis_raises_timout_error(self, lock, mock):
        lock.lock_key = "foo"
        mock.redlock_get_ttl_script.side_effect = redis.exceptions.TimeoutError
        assert lock._get_ttl_from_node(mock) is None


class TestMapNodes:
    def test_default_nodes(self, create_lock, mock, create_fake_nodes):
        nodes = create_fake_nodes(3)
        mock.side_effect = [1, 2, 3]
        lock = create_lock(nodes=nodes)
        assert list(lock._map_nodes(mock)) == [1, 2, 3]
        mock.assert_has_calls([call(n) for n in nodes], any_order=True)

    def test_pass_nodes(self, lock, mock, create_fake_nodes):
        node = create_fake_nodes(1)[0]
        mock.return_value = "foo"
        assert list(lock._map_nodes(mock, [node])) == ["foo"]
        mock.assert_called_once_with(node)


class TestAcquire:
    def test_blocking(self, lock, mocker):
        mocker.patch.object(lock, "_acquire_blocking", return_value=2)
        assert lock.acquire(autoextend=False) == 2
        lock._acquire_blocking.assert_called_once_with(timeout=-1)

        lock._acquire_blocking.reset_mock()
        assert lock.acquire(timeout=1, autoextend=False) == 2
        lock._acquire_blocking.assert_called_once_with(timeout=1)

    def test_non_blocking(self, lock, mocker):
        mocker.patch.object(lock, "_acquire", return_value=2)
        assert lock.acquire(blocking=False, autoextend=False) == 2
        lock._acquire.assert_called_once_with()

    def test_non_blocking_timeout_set(self, lock):
        with raises(ValueError):
            lock.acquire(blocking=False, timeout=1, autoextend=False)

    def test_fail_to_acquire_not_owned(self, create_lock):
        lock1 = create_lock("test_fail_to_lock_acquired", ttl=1000)
        lock2 = create_lock("test_fail_to_lock_acquired", ttl=1000)

        lock1_locked = lock1.acquire(blocking=False, autoextend=False)
        lock2_locked = lock2.acquire(blocking=False, autoextend=False)

        lock1.release()

        assert lock1_locked
        assert not lock2_locked

        assert lock2.acquire()

    def test_autoextend(self, lock, mocker):
        mocker.patch.object(lock, "start_autoextend")
        assert lock.acquire()
        lock.start_autoextend.assert_called_once()

    def test_autoextend_with_timeout(self, lock, mocker):
        mocker.patch.object(lock, "start_autoextend")
        assert lock.acquire(autoextend_timeout=2)
        lock.start_autoextend.assert_called_once_with(timeout=2)

    def test_autoextend_not_acquired(self, lock, mocker):
        mocker.patch.object(lock, "start_autoextend")
        mocker.patch.object(lock, "_acquire", return_value=0)
        assert not lock.acquire(blocking=False)
        lock.start_autoextend.assert_not_called()


class TestAutoextend:
    def test_acquire(self, create_lock):
        lock = create_lock("test_acquire_autoextend", ttl=100)
        assert lock.acquire()
        sleep(0.5)
        assert lock.locked()

    def test_initially_not_acquired(self, lock):
        with raises(InvalidOperationError):
            assert not lock.locked()
            lock.start_autoextend()

    def test_release(self, lock):
        assert lock.acquire()
        assert lock.release()
        assert not lock.locked()
        assert lock._autoextend_thread is None

    def test_autoextend_timeout(self, create_lock):
        lock = create_lock("test_acquire_autoextend", ttl=200)
        assert lock.acquire(autoextend_timeout=0.5)
        sleep(0.5)
        assert not lock.locked()

    def test_fail_to_extend(self, create_lock, mocker):
        lock = create_lock("test_acquire_autoextend", ttl=100)
        mocker.patch.object(lock, "extend", return_value=0)
        assert lock.acquire()
        sleep(0.1)
        assert not lock.locked()


@fixture
def create_fake_nodes():
    def inner(valid=0, invalid=0):
        return [
            *[MagicMock(return_value=True) for i in range(valid)],
            *[MagicMock(return_value=False) for i in range(invalid)],
        ]

    return inner


class TestDunderAcquire:
    def test_sets_lock_key(self, lock, mocker, mock):
        mock.hex = "foo"
        mocker.patch("redlock_plus.uuid.uuid4", return_value=mock)
        assert lock.lock_key is None
        lock._acquire()
        assert lock.lock_key == "foo"

    def test_resets_lock_key_on_fail(self, lock, mocker):
        mocker.patch.object(lock, "_acquire_node", return_value=False)
        lock.lock_key = "foo"
        lock._acquire()
        assert lock.lock_key == "foo"

    def test_acquires_all_nodes(self, lock, mocker):
        mocker.patch.object(lock, "_acquire_node")
        fake_nodes = [MagicMock() for i in range(5)]
        lock.redis_nodes = fake_nodes
        lock._acquire()
        lock._acquire_node.assert_has_calls(
            [call(n) for n in fake_nodes], any_order=True
        )

    @mark.parametrize("nodes_valid,nodes_invalid,result", [(3, 2, True), (2, 3, False)])
    def test_node_majority(
        self, create_lock, mocker, nodes_valid, nodes_invalid, result, create_fake_nodes
    ):
        fake_nodes = create_fake_nodes(nodes_valid, nodes_invalid)
        lock = create_lock(nodes=fake_nodes)
        mocker.patch.object(lock, "_acquire_node", new=lambda n: n())
        assert bool(lock._acquire()) == result

    def test_fail_if_validity_smaller_tll(self, lock, mocker):
        lock.ttl = 50

        def mock_acquire_node(node):
            sleep(0.05)
            return True

        mocker.patch.object(lock, "_acquire_node", new=mock_acquire_node)
        assert not lock._acquire()

    def test_release_all_nodes_on_fail(self, create_lock, mocker, create_fake_nodes):
        fake_nodes = create_fake_nodes(1, 2)
        lock = create_lock(nodes=fake_nodes)
        mocker.patch.object(lock, "_acquire_node", return_value=False)
        mocker.patch.object(lock, "_release_node")
        assert not lock._acquire()
        lock._release_node.assert_has_calls([call(n) for n in fake_nodes])

    def test_retry_on_fail(self, create_lock, create_fake_nodes, mocker):
        lock = create_lock(retry_times=1, nodes=create_fake_nodes(0, 3))
        mocker.patch.object(lock, "_acquire_node", return_value=False)
        lock._acquire()
        assert len(lock.redis_nodes) == 3
        assert lock._acquire_node.call_count == 6

    def test_retry_on_fail_sleep_random(self, create_lock, mocker):
        lock = create_lock()
        mocker.patch.object(lock, "_acquire_node", return_value=False)
        mock_sleep = mocker.patch("redlock_plus.time.sleep")
        mock_randint = mocker.patch("redlock_plus.random.randint", return_value=2000)
        lock.retry_times = 0
        lock._acquire()

        mock_randint.assert_called_once_with(0, lock.retry_delay)
        mock_sleep.assert_called_once_with(2)

    def test_calculate_ttl(self, lock, mocker):
        mocker.patch("redlock_plus.monotonic", side_effect=[2, 4])
        mocker.patch("redlock_plus._monotonic_to_ms", new=lambda t: t)
        drift = (lock.ttl * CLOCK_DRIFT_FACTOR) + 2
        assert lock._acquire() == lock.ttl - (2 + drift)

    def test_negative_ttl(self, create_lock, mocker):
        """
        Acquiring the lock has taken more time than the ttl of the lock
        """
        lock = create_lock(retry_times=0)
        mocker.patch("redlock_plus.monotonic", side_effect=[2, lock.ttl])
        mocker.patch("redlock_plus._monotonic_to_ms", new=lambda t: t)
        assert lock._acquire() is False


class TestAcquireBlocking:  # TODO: rewrite
    def test_success(self, lock, mocker):
        mocker.patch.object(lock, "_acquire", return_value=True)
        mock_sleep = mocker.patch("redlock_plus.time.sleep")
        assert lock._acquire_blocking()
        mock_sleep.assert_not_called()

    def test_block(self, lock, mocker):
        mock_acquire = mocker.patch.object(lock, "_acquire", side_effect=[False, True])
        assert lock._acquire_blocking()
        assert mock_acquire.call_count == 2

    def test_block_timeout(self, lock, mocker):
        lock.retry_delay = 50

        def _mock_acquire(retry_times):
            sleep(lock.retry_delay / 1000)
            return False

        mocker.patch.object(lock, "_acquire", _mock_acquire)
        timeout = 0.06
        time_start = monotonic()
        assert not lock._acquire_blocking(timeout=timeout)
        assert monotonic() - time_start < timeout


class TestLocked:
    def test_no_key_not_locked(self, lock, mocker):
        mocker.patch.object(lock, "check_times")
        assert not lock.lock_key
        assert not lock.locked()
        lock.check_times.assert_not_called()

    def test_is_locked_calls_check_times(self, lock, mocker):
        mocker.patch.object(lock, "check_times", return_value=(True, [1, 2]))
        assert lock.acquire(autoextend=False)
        assert lock.locked() is True

    def test_is_locked(self, lock):
        assert lock.acquire(autoextend=False)
        assert lock.locked() is True

    def test_lock_was_released(self, lock):
        assert lock.acquire(autoextend=False)
        assert lock.release()
        assert lock.locked() is False

    def test_lock_timed_out(self, create_lock):
        ttl = 10
        lock = create_lock(ttl=ttl)
        assert lock.acquire(autoextend=False)
        sleep(ttl / 1000)
        assert lock.locked() is False


class TestRelease:
    def test_not_acquired(self, lock):
        with raises(InvalidOperationError):
            lock.release()

    def test_releases_all_nodes(self, create_lock, create_fake_nodes, mocker):
        fake_nodes = create_fake_nodes(5)
        lock = create_lock(nodes=fake_nodes)
        mocker.patch.object(lock, "_release_node")
        assert lock.acquire(autoextend=False)
        lock.release()
        lock._release_node.assert_has_calls(
            [call(n) for n in fake_nodes], any_order=True
        )

    @mark.parametrize("nodes_valid,nodes_invalid,result", [(3, 2, True), (2, 3, False)])
    def test_node_majority(
        self, create_lock, mocker, nodes_valid, nodes_invalid, result, create_fake_nodes
    ):
        fake_nodes = create_fake_nodes(nodes_valid, nodes_invalid)
        lock = create_lock(nodes=fake_nodes)
        assert lock.acquire(autoextend=False)
        mocker.patch.object(lock, "_release_node", new=lambda n: n())
        assert bool(lock.release()) == result


class TestExtend:
    def test_not_acquired(self, lock):
        with raises(InvalidOperationError):
            lock.extend()

    def test_bumps_all_nodes(self, create_lock, create_fake_nodes, mocker):
        fake_nodes = create_fake_nodes(5)
        lock = create_lock(nodes=fake_nodes)
        mocker.patch.object(lock, "_bump_node")
        assert lock.acquire(autoextend=False)
        assert lock.extend()
        lock._bump_node.assert_has_calls([call(n) for n in fake_nodes], any_order=True)

    @mark.parametrize("nodes_valid,nodes_invalid,result", [(3, 2, True), (2, 3, False)])
    def test_node_majority(
        self, create_lock, mocker, nodes_valid, nodes_invalid, result, create_fake_nodes
    ):
        fake_nodes = create_fake_nodes(nodes_valid, nodes_invalid)
        lock = create_lock(nodes=fake_nodes)
        assert lock.acquire(autoextend=False)
        mocker.patch.object(lock, "_bump_node", new=lambda n: n())
        assert bool(lock.extend()) == result

    def test_fail_if_validity_smaller_tll(self, lock, mocker):
        lock.ttl = 50

        def mock_acquire_node(node):
            sleep(0.05)
            return True

        mocker.patch.object(lock, "_bump_node", new=mock_acquire_node)
        assert lock.acquire(autoextend=False)
        assert not lock.extend()

    def test_retry_on_fail(self, create_lock, create_fake_nodes, mocker):
        lock = create_lock(retry_times=1, nodes=create_fake_nodes(3))
        mocker.patch.object(
            lock, "_bump_node", side_effect=[False, False, False, True, True, True]
        )
        assert lock.acquire(autoextend=False)
        assert lock.extend()
        assert lock._bump_node.call_count == 6

    def test_retry_on_fail_sleep_random(self, lock, mocker):
        mocker.patch.object(lock, "_bump_node", return_value=False)
        mock_sleep = mocker.patch("redlock_plus.time.sleep")
        mock_randint = mocker.patch("redlock_plus.random.randint", return_value=2000)
        lock.retry_times = 0
        assert lock.acquire(autoextend=False)
        assert not lock.extend()

        mock_randint.assert_called_once_with(0, lock.retry_delay)
        mock_sleep.assert_called_once_with(2)

    def test_calculate_ttl(self, lock, mocker):
        assert lock.acquire(autoextend=False)
        mocker.patch("redlock_plus.monotonic", side_effect=[2, 4])
        mocker.patch("redlock_plus._monotonic_to_ms", new=lambda t: t)
        drift = (lock.ttl * CLOCK_DRIFT_FACTOR) + 2
        assert lock.extend() == lock.ttl - (2 + drift)

    def test_negative_ttl(self, create_lock, mocker):
        """
        Extending the lock has taken more time than the ttl of the lock
        """
        lock = create_lock(retry_times=0)
        assert lock.acquire(autoextend=False)
        mocker.patch("redlock_plus.monotonic", side_effect=[2, lock.ttl])
        mocker.patch("redlock_plus._monotonic_to_ms", new=lambda t: t)
        assert lock.extend() is False


class TestAcquireOrExtend:
    def test_is_locked(self, lock, mocker):
        assert lock.acquire(autoextend=False)
        mocker.patch.object(lock, "extend")
        lock.acquire_or_extend()
        lock.extend.assert_called_once()

    def test_is_locked_extended_succesfully(self, lock, mocker):
        assert lock.acquire(autoextend=False)
        mocker.patch.object(lock, "extend", return_value=True)
        mocker.patch.object(lock, "acquire")

        assert lock.acquire_or_extend()
        lock.extend.assert_called_once()
        lock.acquire.assert_not_called()

    def test_is_locked_not_extended_succesfully(self, lock, mocker):
        assert lock.acquire(autoextend=False)
        mocker.patch.object(lock, "extend", return_value=False)
        mocker.patch.object(lock, "acquire")

        assert lock.acquire_or_extend()
        lock.extend.assert_called_once()
        lock.acquire.assert_called_once()

    def test_not_locked(self, lock, mocker):
        mocker.patch.object(lock, "extend")
        mocker.patch.object(lock, "acquire")

        assert lock.acquire_or_extend()
        lock.extend.assert_not_called()
        lock.acquire.assert_called_once()


class TestLockAsContextManager:
    def test_enter_acquires(self, lock):
        with lock:
            assert lock.locked()

    def test_enter_returns_validity(self, lock, mocker):
        mocker.patch.object(lock, "acquire", return_value="foo")
        with lock as acquired:
            assert acquired == "foo"
            lock._acquire()

    def test_enter_acquires_assert_call(self, lock, mocker):
        mocker.patch.object(lock, "acquire")
        with lock:
            lock._acquire()
        lock.acquire.assert_called_once_with()

    def test_exit_releases(self, lock):
        with lock:
            assert lock.locked()
        assert not lock.locked()
