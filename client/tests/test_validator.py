import pytest

from featureform.lib.validator import (
    Validator,
)


def test_add_validation():
    validator = Validator()
    validator.add_validation(False, "Test failure message")
    assert validator.validation_error_messages == ["Test failure message"]


def test_raise_or_pass_with_errors():
    validator = Validator()
    validator.add_validation(False, "Test failure message")
    with pytest.raises(
        ValueError, match="Validation failed with errors: Test failure message"
    ):
        validator.raise_or_pass()


def test_raise_or_pass_without_errors():
    validator = Validator()
    validator.add_validation(True, "Test failure message")
    # Should not raise any exceptions
    validator.raise_or_pass()


def test_validate_non_empty_list_with_non_empty_list():
    # Should not raise any exceptions
    Validator.validate_non_empty_list([1, 2, 3], "List is empty.")


def test_validate_non_empty_list_with_empty_list():
    with pytest.raises(ValueError, match="List is empty."):
        Validator.validate_non_empty_list([], "List is empty.")


def test_validate_name_variant_with_valid_name_variant():
    # Should not raise any exceptions
    Validator.validate_name_variant(("Name", "Variant"), "Name Variant Context")


def test_validate_name_variant_with_invalid_name_variant():
    with pytest.raises(ValueError, match="Name must not be an empty string."):
        Validator.validate_name_variant(("", "Variant"), "Name Variant Context")

    with pytest.raises(ValueError, match="Variant must not be an empty string."):
        Validator.validate_name_variant(("Name", ""), "Name Variant Context")
