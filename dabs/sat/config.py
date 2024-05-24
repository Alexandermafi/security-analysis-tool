import json
import os
import re
import subprocess

from databricks.sdk import WorkspaceClient
from inquirer import Confirm, List, Password, Text, list_input, prompt
from rich.progress import Progress, SpinnerColumn, TextColumn
from sat.utils import (
    cloud_validation,
    get_catalogs,
    get_profiles,
    get_warehouses,
    loading,
)


def form():
    profile = list_input(
        message="Select profile",
        choices=loading(get_profiles, "Loading profiles..."),
    )
    client = WorkspaceClient(profile=profile)

    questions = [
        Text(
            name="account_id",
            message="Databricks Account ID",
            validate=lambda _, x: re.match(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x
            ),
        ),
        Confirm(
            name="enable_uc",
            message="Use Unity Catalog?",
            default=True,
        ),
        List(
            name="catalog",
            message="Select catalog",
            choices=loading(get_catalogs, client=client),
            ignore=lambda x: not x["enable_uc"],
            default="hive_metastore",
        ),
        List(
            name="warehouse",
            message="Select warehouse",
            choices=loading(get_warehouses, client=client),
        ),
    ]
    questions = questions + cloud_specific_questions(client)
    return client, prompt(questions), profile

def get_env_variable(var_name, prompt_message, is_password=False):
    """Get the environment variable or prompt the user for input if not set."""
    value = os.getenv(var_name)
    if value is None:
        if is_password:
            questions = [Password(name=var_name, message=prompt_message)]
        else:
            questions = [Text(name=var_name, message=prompt_message)]
        answers = prompt(questions)
        value = answers[var_name]
        os.environ[var_name] = value
    return value

def cloud_specific_questions(client):
    azure = [
        Text(
            name="azure-tenant-id",
            message="Azure Tenant ID",
            ignore=cloud_validation(client, "azure"),
            default=get_env_variable("AZURE_TENANT_ID", "Enter Azure Tenant ID")
        ),
        Text(
            name="azure-subscription-id",
            message="Azure Subscription ID",
            ignore=cloud_validation(client, "azure"),
            default=get_env_variable("AZURE_SUBSCRIPTION_ID", "Enter Azure Subscription ID")
        ),
        Text(
            name="azure-client-id",
            message="Client ID",
            ignore=cloud_validation(client, "azure"),
            default=get_env_variable("AZURE_CLIENT_ID", "Enter Azure Client ID")
        ),
        Password(
            name="azure-client-secret",
            message="Client Secret",
            ignore=cloud_validation(client, "azure"),
            default=get_env_variable("AZURE_CLIENT_SECRET", "Enter Azure Client Secret", is_password=True),
            echo="",
        ),
    ]
    gcp = [
        Text(
            name="gcp-gs-path-to-json",
            message="Path to JSON key file",
            ignore=cloud_validation(client, "gcp"),
            default=get_env_variable("GCP_GS_PATH_TO_JSON", "Enter Path to JSON key file")
        ),
        Text(
            name="gcp-impersonate-service-account",
            message="Impersonate Service Account",
            ignore=cloud_validation(client, "gcp"),
            default=get_env_variable("GCP_IMPERSONATE_SERVICE_ACCOUNT", "Enter Impersonate Service Account")
        ),
    ]
    aws = [
        Text(
            name="aws-client-id",
            message="Client ID",
            ignore=cloud_validation(client, "aws"),
            default=get_env_variable("AWS_CLIENT_ID", "Enter AWS Client ID")
        ),
        Password(
            name="aws-client-secret",
            message="Client Secret",
            ignore=cloud_validation(client, "aws"),
            default=get_env_variable("AWS_CLIENT_SECRET", "Enter AWS Client Secret", is_password=True),
            echo="",
        ),
    ]
    return aws + azure + gcp


def generate_secrets(client: WorkspaceClient, answers: dict, cloud_type: str):

    scope_name = "sat_scope"
    for scope in client.secrets.list_scopes():
        if scope.name == scope_name:
            client.secrets.delete_scope(scope_name)
            break

    client.secrets.create_scope(scope_name)

    token = client.tokens.create(
        lifetime_seconds=86400 * 90,
        comment="Security Analysis Tool",
    )
    client.secrets.put_secret(
        scope=scope_name,
        key=f"sat-token-{client.get_workspace_id()}",
        string_value=token.token_value,
    )
    client.secrets.put_secret(
        scope=scope_name,
        key="account-console-id",
        string_value=answers["account_id"],
    )
    client.secrets.put_secret(
        scope=scope_name,
        key="sql-warehouse-id",
        string_value=answers["warehouse"]["id"],
    )

    if cloud_type == "aws":
        client.secrets.put_secret(
            scope=scope_name,
            key="use-sp-auth",
            string_value=True,
        )

    for value in answers.keys():
        if cloud_type in value:
            client.secrets.put_secret(
                scope=scope_name,
                key=value.replace(f"{cloud_type}-", ""),
                string_value=answers[value],
            )
