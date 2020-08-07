"""
basic integration tests of public methods.
No patching/mocking of non-external resources
"""


from time import monotonic, sleep
from threading import Thread
import itertools

from pytest import raises, mark

import redlock_plus


def acquire_params(
    blocking=(True, False),
    timeout=(1, -1),
    autoextend=(True, False),
    autoextend_timeout=(None, 1),
):
    return [
        params
        for params in itertools.product(
            blocking, timeout, autoextend, autoextend_timeout
        )
        if not (not params[0] and params[1] != -1)
    ]


class TestAcquire:
    def test(self, create_lock):
        lock = create_lock(ttl=1000)
        assert lock.acquire()
        assert not lock.acquire(blocking=False)
        assert not create_lock(ttl=1000).acquire(blocking=False)

    def test_validity(self, create_lock):
        ttl = 1000
        lock = create_lock(ttl=ttl)
        validity = lock.acquire(autoextend=False)
        assert 0 < validity < ttl - ttl * redlock_plus.CLOCK_DRIFT_FACTOR - 2

    def test_acquire_blocking(self, create_lock):
        lock = create_lock(ttl=100)
        lock2 = create_lock()
        assert lock.acquire()

        thread = Thread(target=lock2.acquire, daemon=True)
        thread.start()
        sleep(0.12)
        assert not lock2.locked()

    def test_acquire_blocking_timeout(self, create_lock):
        lock_ttl = 1000
        timeout = 0.1
        lock = create_lock(ttl=lock_ttl)
        assert lock.acquire()
        time_requested = monotonic()
        assert not lock.acquire(blocking=True, timeout=timeout)
        time_terminated = monotonic()
        assert (time_terminated - time_requested) <= timeout
        assert (time_terminated - time_requested) < lock_ttl

    def test_acquire_blocking_no_autoextend(self, create_lock):
        lock = create_lock(ttl=100)
        lock2 = create_lock(ttl=100)
        assert lock.acquire(autoextend=False)
        start = monotonic()
        assert lock2.acquire(autoextend=False)
        assert monotonic() - start < lock.ttl + lock.retry_delay
        assert not lock.locked()

    def test_autoextend(self, create_lock):
        ttl = 500
        lock = create_lock(ttl=ttl)
        assert lock.acquire()
        sleep((ttl * 1.25) / 1000)
        assert lock.locked()

    def test_expires(self, create_lock):
        lock = create_lock(ttl=50)
        assert lock.acquire(autoextend=False)
        sleep(0.1)
        assert not lock.locked()


class TestLocked:
    @mark.parametrize("args_acquire", acquire_params())
    def test_locked(self, lock, args_acquire):
        assert lock.acquire(*args_acquire)
        assert lock.locked()

    def test_not_locked(self, lock):
        assert not lock.locked()

    def test_not_locked_timed_out(self, create_lock):
        lock = create_lock(ttl=10)
        assert lock.acquire(autoextend=False)
        sleep(0.1)
        assert not lock.locked()


class TestCheckTimes:
    def test_not_acquired(self, lock):
        with raises(redlock_plus.InvalidOperationError):
            assert lock.check_times()

    def test(self, create_lock):
        lock = create_lock(ttl=1000)
        assert lock.acquire()
        locked, times = lock.check_times()
        assert locked
        assert all(t > 950 for t in times)

    def test_timed_out(self, create_lock):
        lock = create_lock(ttl=100)
        assert lock.acquire(autoextend=False)
        sleep(0.1)
        assert lock.check_times() == (False, [])


class TestRelease:
    def test_not_acquired(self, lock):
        with raises(redlock_plus.InvalidOperationError):
            assert not lock.release()

    @mark.parametrize("args_acquire", acquire_params())
    def test(self, create_lock, args_acquire):
        lock = create_lock(ttl=1000)
        assert lock.acquire(*args_acquire)
        assert lock.release()
        assert not lock.locked()
        assert create_lock().acquire(blocking=False, autoextend=False)

    @mark.parametrize(
        "args_acquire", acquire_params(autoextend=(False,)),
    )
    def test_timeout(self, create_lock, args_acquire):
        lock = create_lock(ttl=10)
        assert lock.acquire(*args_acquire)
        sleep(0.01)
        assert not lock.release()
        assert not lock.locked()
        assert create_lock().acquire(blocking=False, autoextend=False)


class TestExtend:
    def test_not_acquired(self, lock):
        with raises(redlock_plus.InvalidOperationError):
            assert not lock.extend()

    @mark.parametrize("args_acquire", acquire_params())
    def test(self, create_lock, args_acquire):
        lock = create_lock(ttl=100)
        validity = lock.acquire(*args_acquire)
        start = redlock_plus._monotonic_ms()
        assert validity
        sleep(validity * 0.75 / 1000)
        assert lock.extend() > validity - redlock_plus._monotonic_ms() - start
        assert lock.locked()


@mark.slow
class TestAcquireOrExtend:
    @mark.parametrize("args_acquire_or_extend", acquire_params())
    def test_not_acquired(self, lock, args_acquire_or_extend):
        assert not lock.locked()
        assert lock.acquire_or_extend(*args_acquire_or_extend)

    @mark.parametrize("args_acquire", acquire_params())
    @mark.parametrize("args_acquire_or_extend", acquire_params())
    def test_acquired(self, create_lock, args_acquire, args_acquire_or_extend):
        lock = create_lock(ttl=200)
        validity = lock.acquire(*args_acquire)
        start = redlock_plus._monotonic_ms()
        assert validity
        sleep(validity * 0.75 / 1000)
        assert (
            lock.acquire_or_extend(*args_acquire_or_extend)
            > validity - redlock_plus._monotonic_ms() - start
        )
        assert lock.locked()


class TestContextManager:
    def test(self, create_lock):
        lock = create_lock(ttl=1000)
        with lock as validity:
            assert validity
            assert lock.locked()
            assert not lock.acquire(blocking=False)
            assert not create_lock().acquire(blocking=False)

    def test_releases(self, create_lock):
        lock = create_lock(ttl=100)
        with lock:
            assert lock.locked()
        assert not lock.locked()
        assert create_lock().acquire(blocking=False)

    def testautoextends(self, create_lock):
        lock = create_lock(ttl=100)
        with lock as validity:
            assert validity
            sleep(0.2)  # should have timed out, autoextend should have renewed
            assert lock.locked()
