import pytest
from featureform.register import ModelRegistrar
from featureform.resources import Model

@pytest.mark.parametrize(
    "ff_provider_source_fxt,is_local",
    [
        ('local_registrar_provider_source', True),
        ('hosted_sql_provider_and_source', False),
    ]
)
def test_getting_model_successfully(ff_provider_source_fxt, is_local, request):
    ff = request.getfixturevalue(ff_provider_source_fxt)[0];
    model_name = "model_a"

    resource_client = arrange_resources(ff, model_name, is_local)

    model = resource_client.get_model(model_name, is_local)

    assert isinstance(model, Model) and model.name == model_name and model.type() == "model"


@pytest.mark.parametrize(
    "ff_provider_source_fxt,is_local",
    [
        ('local_registrar_provider_source', True),
        ('hosted_sql_provider_and_source', False),
    ]
)
def test_getting_model_by_unregistered_name(ff_provider_source_fxt, is_local, request):
    ff = request.getfixturevalue(ff_provider_source_fxt)[0];
    model_name = "model_a"

    resource_client = arrange_resources(ff, model_name, is_local)

    with pytest.raises(ValueError, match="not found"):
        resource_client.get_model("model_b", is_local)


@pytest.mark.parametrize(
    "ff_provider_source_fxt,is_local",
    [
        ('local_registrar_provider_source', True),
        ('hosted_sql_provider_and_source', False),
    ]
)
def test_getting_model_no_name(ff_provider_source_fxt, is_local, request):
    ff = request.getfixturevalue(ff_provider_source_fxt)[0];
    model_name = "model_a"

    resource_client = arrange_resources(ff, model_name, is_local)

    with pytest.raises(TypeError, match="missing 1 required positional argument: 'name'"):
        resource_client.get_model(local=is_local)


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()

def arrange_resources(ff, model_name, is_local):
    ff.register_model(model_name)
    resource_client = ff.ResourceClient(local=is_local)
    resource_client.apply()

    return resource_client
