from typing import Optional

from azure.common.credentials import ServicePrincipalCredentials
from azure.keyvault import KeyVaultAuthentication, KeyVaultClient

from karadoc.common import conf


def get_secret(  # nosec B107
    conn_name: str, secret_id: str, secret_version: str = "", env: Optional[str] = None
) -> str:
    """
    Retrieve a secret from the specified keyvault environment

    Example of keyvault secret store conf to put in the file ".secrets.toml" (should not be tracked in git) :

    >>>
    [development.secret.my_keyvault_env]
      type = "keyvault"
      client_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
      client_secret = "XXXXXXXXXXXXXXXXXXXXXXX"
      tenant_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
      url = "https://mykeyvault.vault.azure.net/"

    Example of connection conf to put in file "settings.toml" that will retrieve a secret from keyvault :

    >>>
    [development.connection.my_env]
        login = "some_login"
        password.secret.my_keyvault_env = "secret_id"


    :param conn_name: Name of the keyvault secret store
    :param secret_id: Id of the keyvault secret to retrieve
    :param secret_version: (Optional) version of the secret to retrieve
    :param env: (Optional) Execution environment
    :return: the secret value
    """
    keyvault_conf = conf.get_vault_conf(conn_name, env)

    def auth_callback(server, resource, scope):
        credentials = ServicePrincipalCredentials(
            client_id=keyvault_conf["client_id"],
            secret=keyvault_conf["client_secret"],
            tenant=keyvault_conf["tenant_id"],
            resource="https://vault.azure.net",
        )
        token = credentials.token
        return token["token_type"], token["access_token"]

    client = KeyVaultClient(KeyVaultAuthentication(auth_callback))

    secret_bundle = client.get_secret(keyvault_conf["url"], secret_id, secret_version)
    return secret_bundle.value
