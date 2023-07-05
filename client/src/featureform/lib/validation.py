from typing import Tuple

from typeguard import typechecked


@typechecked
class Validation:
    def __init__(self, value: any, validation_context: str = ""):
        self.value = value
        self.validation_context = validation_context + ": " if validation_context != "" else ""
        self.validation_error_messages = []

    def validate(self, pred, validation_failure_message: str):
        if not pred(self.value):
            self.validation_error_messages.append(validation_failure_message)

    def throw_or_pass(self):
        if self.validation_error_messages:
            raise ValueError(self.validation_context + " ".join(self.validation_error_messages))
        else:
            pass

    @staticmethod
    @typechecked
    def validate_non_empty_list(value: list, validation_failure_message: str):
        validation = Validation(value)
        validation.validate(lambda ls: len(ls) != 0, validation_failure_message)
        validation.throw_or_pass()

    @staticmethod
    @typechecked
    def validate_name_variant(nvar: Tuple[str, str], validation_context: str):
        validation = Validation(nvar, validation_context)
        validation.validate(lambda v: v[0] != "", "Name must not be an empty string.")
        validation.validate(lambda v: v[1] != "", "Variant must not be an empty string.")
        validation.throw_or_pass()
