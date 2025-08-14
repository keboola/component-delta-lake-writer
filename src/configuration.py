from enum import Enum
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class AccessMethod(str, Enum):
    unity_catalog = "unity_catalog"
    direct_storage = "direct_storage"


class TableType(str, Enum):
    external = "external"
    native = "native"


class LoadType(str, Enum):
    error = "error"
    append = "append"
    overwrite = "overwrite"
    upsert = "upsert"


class Destination(BaseModel):
    container_name: str = ""
    blob_name: str = ""

    catalog: str = ""
    schema_name: str = ""
    table: str = ""
    table_type: TableType = Field(default=TableType.external)
    warehouse: str = ""

    mode: LoadType = Field(default=LoadType.append)
    partition_by: list[str] = Field(default_factory=list)
    compression: str = "UNCOMPRESSED"


class Configuration(BaseModel):
    access_method: AccessMethod = Field(default=AccessMethod.direct_storage)
    unity_catalog_url: str = None
    unity_catalog_token: str = Field(alias="#unity_catalog_token", default=None)
    provider: str = None
    abs_account_name: str = None
    abs_sas_token: str = Field(alias="#abs_sas_token", default=None)
    aws_region: str = None
    aws_key_id: str = None
    aws_key_secret: str = Field(alias="#aws_key_secret", default=None)
    gcp_service_account_key: str = Field(alias="#gcp_service_account_key", default=None)
    destination: Destination
    batch_size: int = 100_000
    preserve_insertion_order: bool = True
    debug: bool = False
    threads: int = 1
    max_memory: int = 256

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
