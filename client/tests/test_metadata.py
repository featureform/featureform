from featureform.metadata import get_provider


def test_get_provider():
    print(get_provider("local-mode"))
