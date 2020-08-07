from pytest import raises

from redlock_plus import InvalidOperationError, RedlockError


class TestAcquire:
    def test_acquire(self, mocker, rlock):
        mock_acquire = mocker.patch("redlock_plus.Lock.acquire", return_value=True)
        params = {
            "blocking": False,
            "timeout": 1,
            "autoextend": False,
            "autoextend_timeout": 2,
        }
        assert rlock.acquire(**params) is True
        mock_acquire.assert_called_once_with(**params)

    def test_acquire_no_success(self, mocker, rlock):
        mocker.patch("redlock_plus.Lock.acquire", return_value=False)
        assert rlock._acquired == 0
        assert rlock.acquire() is False
        assert rlock._acquired == 0

    def test_increase_recursion(self, mocker, rlock):
        mocker.patch("redlock_plus.Lock.acquire", return_value=True)
        mocker.patch("redlock_plus.Lock.check_times", return_value=(True, [0.1, 2]))
        assert rlock.acquire()
        assert rlock._acquired == 1
        assert rlock.acquire() == 0.1
        assert rlock._acquired == 2

    def test_lost_hold(self, mocker, rlock):
        mocker.patch("redlock_plus.Lock.check_times", return_value=(False, [0]))
        rlock._acquired = 1
        with raises(RedlockError):
            rlock.acquire()


class TestAcquireOrExtend:
    def test_is_locked(self, rlock, mocker):
        assert rlock.acquire(autoextend=False)
        assert rlock._acquired == 1
        mocker.patch.object(rlock, "extend")
        rlock.acquire_or_extend()
        assert rlock._acquired == 2
        rlock.extend.assert_called_once()

    def test_is_locked_extended_succesfully(self, rlock, mocker):
        assert rlock.acquire(autoextend=False)
        assert rlock._acquired == 1
        mocker.patch.object(rlock, "extend", return_value=True)
        mocker.patch.object(rlock, "acquire")

        assert rlock.acquire_or_extend()
        assert rlock._acquired == 2

        rlock.extend.assert_called_once()
        rlock.acquire.assert_not_called()

    def test_is_locked_not_extended_succesfully(self, rlock, mocker):
        assert rlock.acquire(autoextend=False)
        assert rlock._acquired == 1

        mocker.patch.object(rlock, "extend", return_value=False)
        mocker.patch.object(rlock, "acquire")

        assert rlock.acquire_or_extend()

        assert rlock._acquired == 1
        rlock.extend.assert_called_once()
        rlock.acquire.assert_called_once()

    def test_not_locked_acquired_successfully(self, rlock, mocker):
        mocker.patch.object(rlock, "extend")
        mock_acquire = mocker.patch("redlock_plus.Lock.acquire", return_value=True)

        assert rlock._acquired == 0
        assert rlock.acquire_or_extend()
        assert rlock._acquired == 1
        rlock.extend.assert_not_called()
        mock_acquire.assert_called_once()

    def test_not_locked_not_acquired_successfully(self, rlock, mocker):
        mocker.patch.object(rlock, "extend")
        mock_acquire = mocker.patch("redlock_plus.Lock.acquire", return_value=False)

        assert rlock._acquired == 0
        assert not rlock.acquire_or_extend()
        assert rlock._acquired == 0
        rlock.extend.assert_not_called()
        mock_acquire.assert_called_once()


class TestRelease:
    def test_not_acquired(self, rlock):
        with raises(InvalidOperationError):
            rlock.release()
        assert rlock._acquired == 0

    def test_decrease_recursion(self, rlock, mocker):
        mock_release = mocker.patch("redlock_plus.Lock.release")
        rlock._acquired = 2
        rlock.lock_key = "bar"
        assert rlock.release() is True
        assert rlock._acquired == 1
        mock_release.assert_not_called()

    def test_release_if_recursion_0(self, rlock, mocker):
        mock_release = mocker.patch("redlock_plus.Lock.release", return_value="foo")
        rlock._acquired = 1
        rlock.lock_key = "bar"
        assert rlock.release() == "foo"
        assert rlock._acquired == 0
        mock_release.assert_called_once()
