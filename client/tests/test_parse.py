#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

from featureform.parse import add_variant_to_name


def test_replace_name_with_default():
    text = "something something {{ name }} something else"
    result = add_variant_to_name(text, "default")
    assert result == "something something {{ name.default }} something else"


def test_replace_name_with_custom_default():
    text = "something something {{ name }} something else"
    result = add_variant_to_name(text, "custom_default")
    assert result == "something something {{ name.custom_default }} something else"


def test_replace_multiple_names():
    text = "something {{ name1 }} something {{ name2 }} something else"
    result = add_variant_to_name(text, "custom_default")
    assert (
        result
        == "something {{ name1.custom_default }} something {{ name2.custom_default }} something else"
    )


def test_replace_with_period():
    text = "something {{ name.variant }} something"
    result = add_variant_to_name(text, "custom_default")
    assert result == "something {{ name.variant }} something"


def test_replace_with_underscores():
    text = "something {{ name_my_name }} something"
    result = add_variant_to_name(text, "default")
    assert result == "something {{ name_my_name.default }} something"


def test_replace_no_space():
    text = "something {{name}} something"
    result = add_variant_to_name(text, "default")
    assert result == "something {{ name.default }} something"


def test_replace_extra_space():
    text = "something {{   name    }} something"
    result = add_variant_to_name(text, "default")
    assert result == "something {{ name.default }} something"
