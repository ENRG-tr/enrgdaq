import os

UNIT_TESTING_ENV_VAR = "ENRGDAQ_IS_UNIT_TESTING"


def is_unit_testing():
    return os.environ.get(UNIT_TESTING_ENV_VAR) == "True"


def set_is_unit_testing(value: bool):
    os.environ[UNIT_TESTING_ENV_VAR] = "True" if value else "False"
