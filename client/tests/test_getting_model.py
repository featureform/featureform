import pytest
from featureform.register import ModelRegistrar
from featureform.resources import Model
import featureform as ff


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ],
)
def test_getting_model_successfully(is_local, is_insecure):
    model_name = "model_i"

    resource_client = arrange_resources(model_name, is_local, is_insecure)

    model = resource_client.get_model(model_name, is_local)

    assert (
        isinstance(model, Model)
        and model.name == model_name
        and model.type() == "model"
    )


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ],
)
def test_getting_model_by_unregistered_name(is_local, is_insecure):
    model_name = "model_j"

    resource_client = arrange_resources(model_name, is_local, is_insecure)
    if is_local:
        with pytest.raises(IndexError, match="out of range"):
            resource_client.get_model("model_z", is_local)
    else:
        model = resource_client.get_model("model_z", is_local)
        assert model is None


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ],
)
def test_getting_model_no_name(is_local, is_insecure):
    model_name = "model_k"

    resource_client = arrange_resources(model_name, is_local, is_insecure)

    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'name'"
    ):
        resource_client.get_model(local=is_local)


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources(model_name, is_local, is_insecure):
    ff.register_model(model_name)
    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply(asynchronous=True)

    return resource_client
