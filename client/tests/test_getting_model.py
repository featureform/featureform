import pytest

@pytest.mark.parametrize(
    "resource_serving_client_fxt,is_local",
    [
        ('resource_serving_clients', True),
        ('resource_serving_clients', False),
    ]
)
def test_getting_model_successfully(resource_serving_client_fxt, is_local, request):
    resource_client = request.getfixturevalue(resource_serving_client_fxt)(is_local)[0];
    model_name = "model_a"

    arrange_resources(resource_client, model_name)

    model = resource_client.get_model(model_name)

    assert isinstance(model, ModelRegistrar) and model.name() == model_name


@pytest.mark.parametrize(
    "resource_serving_client_fxt,is_local",
    [
        ('resource_serving_clients', True),
        ('resource_serving_clients', False),
    ]
)
def test_getting_model_by_unregistered_name(resource_serving_client_fxt, is_local, request):
    resource_client = request.getfixturevalue(resource_serving_client_fxt)(is_local)[0];
    model_name = "model_a"

    arrange_resources(resource_client, model_name)

    with pytest.raises(ValueError, match="not found"):
        resource_client.get_model("model_b")


@pytest.mark.parametrize(
    "resource_serving_client_fxt,is_local",
    [
        ('resource_serving_clients', True),
        ('resource_serving_clients', False),
    ]
)
def test_getting_model_no_name(resource_serving_client_fxt, is_local, request):
    resource_client = request.getfixturevalue(resource_serving_client_fxt)(is_local)[0];
    model_name = "model_a"

    arrange_resources(resource_client, model_name)

    with pytest.raises(TypeError, match="missing 1 required positional argument: 'name'"):
        resource_client.get_model("model_b")


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()

def arrange_resources(resource_client, model_name):
    resource_client.register_model(model_name)
    resource_client.apply()
