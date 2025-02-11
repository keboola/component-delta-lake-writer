import logging
from enum import Enum
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class LoadType(str, Enum):
    error = "error"
    append = "append"
    overwrite = "overwrite"
    ignore = "ignore"


class Destination(BaseModel):
    container_name: str = Field()
    blob_name: str = Field()
    mode: LoadType = Field(default=LoadType.append)
    partition_by: list[str] = Field(default_factory=list)


class Configuration(BaseModel):
    account_name: str
    sas_token: str = Field(alias="#sas_token")
    destination: Destination
    batch_size: int = 10_000
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

        if self.debug:
            logging.debug("Component will run in Debug mode")
