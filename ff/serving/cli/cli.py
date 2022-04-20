# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import click

resource_types = [
    "feature",
    "source",
    "training-set",
    "label",
    "entity",
    "provider",
    "transformation",
]

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    \b
    ______         _                   __                     
    |  ___|       | |                 / _|                    
    | |_ ___  __ _| |_ _   _ _ __ ___| |_ ___  _ __ _ __ ___  
    |  _/ _ \/ _` | __| | | | '__/ _ \  _/ _ \| '__| '_ ` _ \ 
    | ||  __/ (_| | |_| |_| | | |  __/ || (_) | |  | | | | | |
    \_| \___|\__,_|\__|\__,_|_|  \___|_| \___/|_|  |_| |_| |_|

    Interact with Featureform's Feature Store via the official command line interface.
    """
    pass

@cli.command()
@click.argument("resource_type")
def list(resource_type):
    """list resources of a given type.
    """
    pass

@cli.command()
@click.argument("resource_type", type=click.Choice(resource_types, case_sensitive=False))
@click.argument("resources", nargs=-1, required=True)
def get(resource_type, resoruces):
    """get resources of a given type.
    """
    pass

@cli.command()
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
def plan(files):
    """print out resources that would be changed by applying these files.
    """
    pass

@cli.command()
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
def apply(files):
    """apply changes to featureform
    """
    pass

if __name__ == '__main__':
    cli()
