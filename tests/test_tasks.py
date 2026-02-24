"""Tests for the shared task library."""

from prefect_examples.tasks import print_message, square_number


def test_print_message() -> None:
    result = print_message.fn("hello")
    assert result == "hello"


def test_square_number() -> None:
    result = square_number.fn(5)
    assert result == 25
