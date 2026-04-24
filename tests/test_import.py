from escrowmint import Client, ClientConfig


def test_client_from_url() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    assert isinstance(client.config, ClientConfig)
    assert client.config.key_prefix == "escrowmint"
