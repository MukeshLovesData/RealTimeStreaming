from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='services/consumer/config/settings.env', env_file_encoding='utf-8'
    )
    # product_ids: list[str] = [
    #     'ETH/EUR',
    #     'BTC/USD',
    #     'BTC/EUR',
    #     'ETH/USD',
    #     'SOL/USD',
    #     'SOL/EUR',
    #     'XRP/USD',
    #     'XRP/EUR',
    # ]
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = "$ConnectionString"
    connection_string: str
    event_hub_name: str
    input_topic: str
    consumer_group:str


Settings = Config()
