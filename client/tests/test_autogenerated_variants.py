#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest

from featureform.register import Registrar, ColumnMapping
from featureform.resources import SQLTable


def test_set_run():
    registrar = Registrar()
    registrar.set_run("test")
    assert registrar.get_run() == "test"


@pytest.mark.parametrize("set_variant", [False, True])
def test_register_columns(set_variant):
    registrar = Registrar()
    if set_variant:
        registrar.set_run("test")
    run = registrar.get_run()
    registrar.register_column_resources(
        ("name", ""),
        entity="",
        entity_column="",
        owner="default_user",
        features=[
            ColumnMapping(name="name", type="int", source="source", column="column")
        ],
        inference_store="redis",
    )
    resource = registrar.state().sorted_list()[0]
    assert resource.variant == run
    assert resource.source[1] == run


@pytest.mark.parametrize("set_variant", [False, True])
def test_register_primary(set_variant):
    registrar = Registrar()
    if set_variant:
        registrar.set_run("test")
    run = registrar.get_run()
    registrar.register_primary_data(
        name="test",
        owner="default_user",
        location=SQLTable(name="table"),
        provider="",
        tags=[],
        properties={},
    )
    resource = registrar.state().sorted_list()[0]
    assert resource.variant == run


@pytest.mark.parametrize("set_variant", [False, True])
def test_register_sql(set_variant):
    registrar = Registrar()
    if set_variant:
        registrar.set_run("test")
    run = registrar.get_run()

    @registrar.sql_transformation(
        name="test",
        owner="default_user",
        provider="",
    )
    def query():
        return "SELECT * FROM {{ table }}"

    resource = registrar.state().sorted_list()[0]
    assert resource.definition.query == "SELECT * FROM {{ table." + run + " }}"
    assert resource.variant == run


@pytest.mark.parametrize("set_variant", [False, True])
def test_register_df(set_variant):
    registrar = Registrar()
    if set_variant:
        registrar.set_run("test")
    run = registrar.get_run()

    @registrar.df_transformation(
        name="test",
        owner="default_user",
        provider="",
        inputs=[("name", ""), "name2"],
        tags=[],
        properties={},
    )
    def some_function(x):
        return x

    resource = registrar.state().sorted_list()[0]
    for input in resource.definition.inputs:
        assert input[1] == run
    assert resource.variant == run


@pytest.mark.parametrize("set_variant", [False, True])
def test_register_trainingset(set_variant):
    registrar = Registrar()
    if set_variant:
        registrar.set_run("test")
    run = registrar.get_run()
    registrar.register_training_set(
        name="test",
        owner="default_user",
        features=[("feature", "")],
        label=("label", ""),
        tags=[],
        properties={},
    )
    resource = registrar.state().sorted_list()[0]
    assert resource.variant == run
    for feature in resource.features:
        assert feature[1] == run
    assert resource.label[1] == run


@pytest.mark.parametrize("set_variant", [False, True])
def test_register_trainingset_string(set_variant):
    registrar = Registrar()
    if set_variant:
        registrar.set_run("test")
    run = registrar.get_run()
    registrar.register_training_set(
        name="test",
        owner="default_user",
        features=["feature"],
        label="label",
        tags=[],
        properties={},
    )
    resource = registrar.state().sorted_list()[0]
    assert resource.variant == run
    for feature in resource.features:
        assert feature[1] == run
    assert resource.label[1] == run


def test_register_columns_with_variant():
    variant = "test"
    registrar = Registrar()
    run = registrar.get_run()
    registrar.register_column_resources(
        ("name", ""),
        entity="",
        entity_column="",
        owner="default_user",
        features=[
            ColumnMapping(
                name="name",
                variant=variant,
                type="int",
                source="source",
                column="column",
            )
        ],
        inference_store="redis",
    )
    resource = registrar.state().sorted_list()[0]
    assert resource.variant == variant
    assert resource.source[1] == run


def test_register_primary_with_variant():
    variant = "test"
    registrar = Registrar()
    registrar.register_primary_data(
        name="test",
        variant=variant,
        owner="default_user",
        location=SQLTable(name="table"),
        provider="",
        tags=[],
        properties={},
    )
    resource = registrar.state().sorted_list()[0]
    assert resource.variant == variant


def test_register_sql_with_variant():
    variant = "test"
    registrar = Registrar()
    run = registrar.get_run()

    @registrar.sql_transformation(
        name="test",
        variant=variant,
        owner="default_user",
        provider="",
    )
    def query():
        return "SELECT * FROM {{ table }}"

    resource = registrar.state().sorted_list()[0]
    assert resource.definition.query == "SELECT * FROM {{ table." + run + " }}"
    assert resource.variant == variant


def test_register_df_with_variant():
    variant = "test"
    registrar = Registrar()
    run = registrar.get_run()

    @registrar.df_transformation(
        name="test",
        variant=variant,
        owner="default_user",
        provider="",
        inputs=[("name", ""), "name2"],
        tags=[],
        properties={},
    )
    def some_function(x):
        return x

    resource = registrar.state().sorted_list()[0]
    for input in resource.definition.inputs:
        assert input[1] == run
    assert resource.variant == variant


def test_register_trainingset_with_variant():
    variant = "test"
    registrar = Registrar()
    run = registrar.get_run()
    registrar.register_training_set(
        name="test",
        variant=variant,
        owner="default_user",
        features=[("feature", "")],
        label=("label", ""),
        tags=[],
        properties={},
    )
    resource = registrar.state().sorted_list()[0]
    for feature in resource.features:
        assert feature[1] == run
    assert resource.label[1] == run
    assert resource.variant == variant


def test_register_trainingset_string_with_variant():
    variant = "test"
    registrar = Registrar()
    run = registrar.get_run()
    registrar.register_training_set(
        name="test",
        variant=variant,
        owner="default_user",
        features=["feature"],
        label="label",
        tags=[],
        properties={},
    )
    resource = registrar.state().sorted_list()[0]
    for feature in resource.features:
        assert feature[1] == run
    assert resource.label[1] == run
    assert resource.variant == variant
