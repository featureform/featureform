from behave import *


@given("Featureform is installed")
def step_impl(context):
    import featureform


@when('I create a "{mode}" "{secure}" client for "{host}"')
def step_impl(context, mode, secure, host):
    import featureform

    if secure == "secure":
        is_secure = True
    elif secure == "insecure":
        is_secure = False
    elif secure == "None":
        is_secure = None
    else:
        raise ValueError(
            f"Invalid secure value: {secure}. Must be 'secure', 'insecure', or 'None'"
        )

    if mode == "localmode":
        context.client = featureform.Client(localmode=True)
    elif mode == "hosted":
        context.client = featureform.Client(host=host, insecure=not is_secure)
    else:
        raise ValueError(f"Invalid mode value: {mode}. Must be 'localmode' or 'hosted'")
