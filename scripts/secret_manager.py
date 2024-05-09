import os
import json
import argparse

import boto3

# Get Current Directory
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
FILE_NAME = ".env"

FILE_PATH = os.path.join(PARENT_DIR, FILE_NAME)


def main(arg):
    client = boto3.client("secretsmanager")
    secrets_with_values = get_secrets_with_values(client)

    if arg.update or not os.path.exists(FILE_PATH):
        print(f"Writing secrets to {FILE_PATH}")
        with open(FILE_PATH, "w") as f:
            for key, value in secrets_with_values.items():
                f.write(f"{key}={value}\n")
    else:
        print("File already exists. Use --update or make update_secrets to overwrite.")


def get_secrets_with_values(client):
    secrets = {}

    next_token = ""
    testing_only_secrets = [
        {
            "Key": "name",
            "Values": [
                "testing/",
            ],
        }
    ]

    while True:
        if not next_token:
            response = client.batch_get_secret_value(
                Filters=testing_only_secrets,
            )
        else:
            response = client.batch_get_secret_value(
                Filters=testing_only_secrets,
                NextToken=next_token,
            )

        for s in response["SecretValues"]:
            secret_value = json.loads(s["SecretString"])
            for key, value in secret_value.items():
                secrets[key] = f'"{value}"'

        next_token = response.get("NextToken", "")
        if not next_token:
            break

    print(f"Found {len(secrets)} secrets")
    return secrets


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Featureform Secret Manager")
    parser.add_argument(
        "--update",
        action="store_true",
    )

    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args())
