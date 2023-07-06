from typing import Tuple

from typeguard import typechecked


@typechecked
class Validator:
    def __init__(self, validation_context: str = ""):
        self.validation_context = validation_context
        self.validation_error_messages = []

    def add_validation(self, condition, validation_failure_message: str):
        if not condition:
            self.validation_error_messages.append(validation_failure_message)

    def raise_or_pass(self):
        if self.validation_error_messages:
            error_messages = ";".join(self.validation_error_messages)
            raise ValueError(
                f"{self.validation_context} Validation failed with errors: {error_messages}"
            )
        else:
            pass

    @staticmethod
    def validate_non_empty_list(ls: list, validation_failure_message: str):
        validator = Validator()
        # Read as "validate that the length of ls is greater than zero"
        validator.add_validation(len(ls) > 0, validation_failure_message)
        validator.raise_or_pass()

    @staticmethod
    def validate_name_variant(nvar: Tuple[str, str], validation_context: str):
        validator = Validator(validation_context)
        # Read as "validate that nvar[0] is not an empty string"
        validator.add_validation(nvar[0] != "", "Name must not be an empty string.")
        validator.add_validation(nvar[1] != "", "Variant must not be an empty string.")
        validator.raise_or_pass()
