from typing import Tuple

from typeguard import typechecked


@typechecked
class Validation:
    def __init__(self, validation_context: str = ""):
        self.validation_context = validation_context
        self.validation_error_messages = []

    def validate(self, condition, validation_failure_message: str):
        if not condition:
            self.validation_error_messages.append(validation_failure_message)

    def raise_or_pass(self):
        if self.validation_error_messages:
            error_messages = ';'.join(self.validation_error_messages)
            raise ValueError(f'{self.validation_context} Validation failed with errors: {error_messages}')
        else:
            pass

    @staticmethod
    def validate_non_empty_list(ls: list, validation_failure_message: str):
        validation = Validation()
        # Read as "validate that the length of ls is greater than zero"
        validation.validate(len(ls) > 0, validation_failure_message)
        validation.raise_or_pass()

    @staticmethod
    def validate_name_variant(nvar: Tuple[str, str], validation_context: str):
        validation = Validation(validation_context)
        # Read as "validate that nvar[0] is not an empty string"
        validation.validate(nvar[0] != "", "Name must not be an empty string.")
        validation.validate(nvar[1] != "", "Variant must not be an empty string.")
        validation.raise_or_pass()
