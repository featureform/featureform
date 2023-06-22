import pytest
import tenacity

from featureform.retry import add_exponential_retry


class TestClass:
    def __init__(self):
        self.counter = 0

    def failing_method(self):
        self.counter += 1
        raise ValueError("This method always fails")

    def eventually_succeeds(self):
        self.counter += 1
        if self.counter < 3:
            raise ValueError("Fails for the first 2 attempts")
        else:
            return "Success"


def test_always_fails():
    test = TestClass()
    retried_method = add_exponential_retry(test.failing_method, max_attempts=5)

    with pytest.raises(tenacity.RetryError) as excinfo:
        retried_method()

    assert "This method always fails" == str(excinfo.value.last_attempt.exception())
    assert test.counter == 5


def test_eventually_succeeds():
    obj = TestClass()
    retried_method = add_exponential_retry(obj.eventually_succeeds, max_attempts=5)

    assert retried_method() == "Success"
    assert obj.counter == 3


def test_args_kwargs():
    def foo(a, b, c=None):
        return (a, b, c)

    retried_foo = add_exponential_retry(foo)

    assert retried_foo("hello", "world", c="!") == ("hello", "world", "!")


def test_max_attempts():
    obj = TestClass()
    retried_method = add_exponential_retry(obj.failing_method, max_attempts=3)

    with pytest.raises(tenacity.RetryError) as excinfo:
        retried_method()

    assert "This method always fails" == str(excinfo.value.last_attempt.exception())
    assert obj.counter == 3
