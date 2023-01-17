import click
import yaml

secretBase = {
    "apiVersion": "v1",
    "kind": "Secret",
    "metadata": {
        "name": "featureform-backup"
    },
    "type": "Opaque"
}

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    Generates a Kubernetes secret to store Featureform backup data.

    Use this script to generate the Kubernetes secret, then apply it with:
    `kubectl apply -f backup_secret.yaml`
    """
    pass


@cli.command()
@click.argument("storage_account", required=True)
@click.argument("storage_key", required=True)
@click.argument("container_name", required=True)
@click.argument("container_path", required=True)
def azure(storage_account, storage_key, container_name, container_path):
    """
    Create secret for azure storage containers

    STORAGE_ACCOUNT is the name of the Azure storage account

    STORAGE_KEY is the key for the Azure storage account

    CONTAINER_NAME is the name of the Azure storage container to store the backups

    CONTAINER_PATH a subdirectory in the container to store the backups
    """
    secretBase["stringData"] = {
        "CLOUD_PROVIDER": "AZURE",
        "AZURE_STORAGE_ACCOUNT": storage_account,
        "AZURE_STORAGE_KEY": storage_key,
        "AZURE_CONTAINER_NAME": container_name,
        "AZURE_STORAGE_PATH": container_path
    }
    with open("./backup_secret.yaml", 'w+') as f:
        yaml.dump(secretBase, f)


if __name__ == '__main__':
    cli()
