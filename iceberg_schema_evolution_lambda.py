from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    UUIDType,
)

from constants import Constants, DefaultDBName, DefaultS3Bucket


class IcebergTransform(Enum):
    IDENTITY = "identity"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"


class IcebergSchemaChangeType(Enum):
    NO_CHANGE = "no_change"
    CREATE_TABLE = "create_table"
    UPDATE_SCHEMA = "update_table"


class IcebergDataType(Enum):
    """
    Enum to enforce proper types and lookups before trying to create iceberg table.
    Addresses incorrect or unhandled column type errors. The chime_type_set property is
    an array of iceberg -> chime type mappings. Allows for multiple chime type mappings
     for e.g. source type = int|integer will map to pyiceberg.types.IntegerType
    """

    FIXED_TYPE = (
        "fixed_type",
        "FixedType",
        {"fixed"},
        FixedType(4),
        False,
    )  # TODO - where is fixed type length read from?
    DECIMAL_TYPE = (
        "decimal_type",
        "DecimalType",
        {"decimal"},
        DecimalType(5, 5),
        False,
    )  # TODO - where is decimal precision and scale read from?
    LIST_TYPE = ("list_type", "ListType", {"list", "array"}, StringType(), True)
    STRUCT_TYPE = ("struct_type", "StructType", {"struct"}, StructType(), True, True)
    ARRAY_TYPE = (
        "array_type",
        "ListType",
        {"list"},
        StringType(),
        True,
    )  # TODO Remove?
    MAP_TYPE = (
        "map_type",
        "MapType",
        {"map"},
        StringType(),
        False,
    )  # Just use StringType as native type.
    BOOLEAN_TYPE = (
        "boolean_type",
        "BooleanType",
        {"boolean", "bool"},
        BooleanType(),
        False,
    )
    INTEGER_TYPE = (
        "integer_type",
        "IntegerType",
        {"integer", "int"},
        IntegerType(),
        False,
    )
    LONG_TYPE = ("long_type", "LongType", {"bigint", "long"}, LongType(), False)
    FLOAT_TYPE = ("float_type", "FloatType", {"float"}, FloatType(), False)
    DOUBLE_TYPE = ("double_type", "DoubleType", {"double"}, DoubleType(), False)
    DATE_TYPE = ("date_type", "DateType", {"date"}, DateType(), False)
    TIME_TYPE = ("time_type", "TimeType", {"time"}, TimeType(), False)
    TIMESTAMP_TYPE = (
        "timestamp_type",
        "TimestampType",
        {"timestamp"},
        TimestampType(),
        False,
    )
    TIMEZONE_TYPE = (
        "timezone_type",
        "TimezoneType",
        {"timezone"},
        TimestampType(),
        False,
    )  # TODO - time zone and time stamp example
    STRING_TYPE = ("string_type", "StringType", {"string", "str"}, StringType(), False)
    UUID_TYPE = ("uuid_type", "UUIDType", {"uuid"}, UUIDType(), False)
    BINARY_TYPE = ("binary_type", "BinaryType", {"binary"}, BinaryType(), False)
    UNKNOWN_TYPE = ("unknown_type", "UnknownType", {"unknown"}, StringType(), False)

    def __init__(
        self,
        value: str,
        description: str,
        chime_type_set: set[str],
        native_iceberg_type: IcebergType,
        is_nested_type: bool,
        can_flatten: bool = False,
    ):
        self._value = value
        self._description = description
        self._is_nested_type = is_nested_type
        self._chime_type_set = chime_type_set
        self._native_iceberg_type = native_iceberg_type
        self._can_flatten = can_flatten

    @property
    def is_nested_type(self):
        return self._is_nested_type

    @property
    def value(self):
        return self._value

    @property
    def description(self):
        return self._description

    @property
    def chime_type_set(self):
        return self._chime_type_set

    @property
    def native_iceberg_type(self):
        return self._native_iceberg_type

    @property
    def can_flatten(self):
        return self._can_flatten


class ProcessIcebergResponseStatus(Enum):
    SUCCESS = ("200", "Successfully processed iceberg action")
    ERROR = ("500", "Application error")
    INVALID = ("400", "Invalid content. Parsing error")
    NO_ALLOWED = ("401", "Action not allowed")
    NO_OP = ("402", "No operation")
    INVALID_FILE_TYPE = ("-100", "Invalid file type. YAML file required")
    UNPARSABLE_FILE = ("-200", "Cannot read YAML file")
    EMPTY_INPUT_FILE = ("-300", "File content is empty")

    def __init__(
        self,
        value: str,
        description: str,
    ):
        self._value = value
        self._description = description

    @property
    def value(self):
        return self._value

    @property
    def description(self):
        return self._description


class CatalogResourceType(Enum):
    DATABASE = ("database", "A database resource in Glue catalog")
    TABLE = ("table", "A table resource in Glue catalog")
    COLUMN = ("column", "A column resource in Glue catalog")

    def __init__(self, name: str, description: str):
        self._name = name
        self._description = description


@dataclass
class ProcessIcebergSchemaResponse:
    """
    Response from processing iceberg schema from source.
    """

    status: Optional[ProcessIcebergResponseStatus] = field(
        default=ProcessIcebergResponseStatus.SUCCESS
    )
    message_list: list[str] = field(default_factory=list)
    change_type: IcebergSchemaChangeType = field(
        default=IcebergSchemaChangeType.NO_CHANGE
    )


@dataclass
class ParsedColumn:
    """
    Holds the data for a single column from source. This is maintained in a list
    for nested types.
    """

    # column name. For nested fields the ancestors are joined with '.'
    # For e.g. f1 in struct s1 will be s1.f1
    name: str
    # derived IcebergDataType Enum from iceberg_constants
    # replace '.' in name with '_'. If duplicate found after
    # flattening seperator is '_'*dup instance
    iceberg_data_type: IcebergDataType
    flattened_name: Optional[str] = None
    parent: Optional[str] = field(
        default=None
    )  # if child field then the name of the parent i.e. ParsedColumn.name
    # indicates if the column is nested. TODO - we can derive it from type. Remove?
    nested: Optional[bool] = field(default=False)
    # nesting level. 1 based, i.e. non nested fields on table level value = 1
    nested_level: Optional[int] = field(default=None)
    # Required - T/F. Default is false but is read from source(yaml).Override in source.
    required: Optional[bool] = field(default=False)
    # For list (array) columns the type of List.
    list_type: Optional[IcebergDataType] = field(repr=False, default=None)
    map_key_type: Optional[IcebergDataType] = field(
        repr=False, default=None
    )  # For map columns the key type
    map_value_type: Optional[IcebergDataType] = field(
        repr=False, default=None
    )  # For map columns the value type
    # pyiceberg.NestedField. The nested field types have their entire definition
    # i.e. struct, map and list. # This is used while building the actual table schema
    ns: Optional[NestedField] = field(default=None)

    def __post_init__(self):
        self.flattened_name = self.name.replace(
            ".", "_"
        )  # Create default flattened name
        # Set nested level and parent if needed i.e. if name has '.'
        if self.nested_level is None:
            self.nested = self.name.__contains__(".")
            if self.nested:
                self.nested_level = len(self.name.split("."))
                self.parent = ".".join(self.name.split(".")[:-1])


@dataclass
class PartitionSpecification:
    partition_name: Optional[str] = field(default=None)
    partition_transform: Optional[str] = field(default=None)
    partition_field: Optional[str] = field(default=None)


@dataclass
class ProcessVariables:
    bronze_db_name: str = field(default=DefaultDBName.BRONZE_DB.value)
    bronze_s3_bucket: str = field(default=DefaultS3Bucket.SILVER_S3.value)
    silver_db_name: str = field(default=DefaultDBName.SILVER_DB.value)
    silver_s3_bucket: str = field(default=DefaultS3Bucket.SILVER_S3.value)
    silver_flattening_level: int = field(default=Constants.SILVER_FLATTEN_TO_MAX_LEVEL)
    partition_spec: list[PartitionSpecification] = field(default_factory=list)


@dataclass
class IcebergSchemaRegistryItem:
    layer_db_table_column_key: str
    layer_db_table_gsi: str
    layer: str
    db_name: str
    table_name: str
    column_name: str
    source_mapping: str
    source_db: str
    source_table: str
    field_order: int
    # is_active:bool


@dataclass
class ProcessVariables:
    bronze_db_name: str = field(default=DefaultDBName.BRONZE_DB.value)
    bronze_s3_bucket: str = field(default=DefaultS3Bucket.SILVER_S3.value)
    silver_db_name: str = field(default=DefaultDBName.SILVER_DB.value)
    silver_s3_bucket: str = field(default=DefaultS3Bucket.SILVER_S3.value)
    silver_flattening_level: int = field(default=Constants.SILVER_FLATTEN_TO_MAX_LEVEL)
    partition_spec: list[PartitionSpecification] = field(default_factory=list)


def find_enum_by_attribute(enum_class, attribute_name, attribute_value) -> Any | None:
    for member in enum_class:
        if (
            hasattr(member, attribute_name)
            and getattr(member, attribute_name).lower() == attribute_value.lower()
        ):
            return member
    return None


def map_chime_type_to_iceberg_type(chime_type_value: str) -> IcebergDataType:
    chime_type_lower = chime_type_value.casefold()
    return_type: IcebergDataType = IcebergDataType.UNKNOWN_TYPE
    if chime_type_value.lower().startswith("array"):
        return_type = IcebergDataType.LIST_TYPE
    elif chime_type_value.lower().startswith("struct"):
        return_type = IcebergDataType.STRUCT_TYPE
    elif chime_type_value.lower().startswith("map"):
        return_type = IcebergDataType.MAP_TYPE
    else:
        for member in IcebergDataType:
            if member.chime_type_set.__contains__(chime_type_lower):
                return_type = member
                break
    return return_type


from dataclasses import dataclass
from enum import Enum


class DefaultS3Bucket(Enum):
    BRONZE_S3 = ("data-lake-raw-events-de-sandbox", "Bucket for Bronze layer")
    SILVER_S3 = ("data-lake-raw-events-de-sandbox", "Bucket for Silver layer")
    GOLD_S3 = ("data-lake-raw-events-de-sandbox", "Bucket for Gold layer")

    def __init__(self, value: str, description: str):
        self._value = value
        self._description = description

    @property
    def description(self) -> str:
        return self._description

    @property
    def value(self) -> str:
        return self._value

    def __str__(self):
        return self._value


class DefaultDBName(Enum):
    BRONZE_DB = ("demo_bronze_db", "Default Bronze iceberg database")
    SILVER_DB = ("demo_silver_db", "Default Silver iceberg database")
    GOLD_DB = ("demo_gold_db", "Default Gold iceberg database")

    def __init__(self, value: str, description: str):
        self._value = value
        self._description = description

    @property
    def description(self) -> str:
        return self._description

    @property
    def value(self) -> str:
        return self._value

    def __str__(self):
        return self._value


@dataclass
class Constants:
    DEFAULT_ICEBERG_PARTITION_TRANSFORM: str = "hour"
    DEFAULT_ICEBERG_PARTITION_FIELD: str = "_created_at"
    BRONZE_DB_NAME: str = DefaultDBName.BRONZE_DB
    BRONZE_DB_S3_BUCKET: str = DefaultS3Bucket.BRONZE_S3
    ICEBERG_PARTITION_COLUMN: str = "_created_at"
    ICEBERG_PARTITION_TRANSFORM: str = "hour"
    SILVER_FLATTEN_TO_MAX_LEVEL: int = -1
    SILVER_DB_NAME: str = DefaultDBName.SILVER_DB
    SILVER_DB_S3_BUCKET: str = DefaultS3Bucket.SILVER_S3


import logging
from collections import Counter
from dataclasses import asdict
from typing import Any, Optional, Tuple

import boto3
import yaml
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import ListType, MapType, NestedField, StructType

from class_definitions import (
    DefaultS3Bucket,
    IcebergDataType,
    ParsedColumn,
    ProcessIcebergResponseStatus,
    ProcessIcebergSchemaResponse,
    map_chime_type_to_iceberg_type,
    IcebergSchemaChangeType,
    ProcessVariables,
    IcebergSchemaRegistryItem,
)
from constants import Constants

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
CREATE_TABLE_FIELD_ID: int = -1
ARRAY_FIELD: str = "array"
STRUCT_FIELD: str = "struct"
MAP_FIELD: str = "map"


def read_from_s3(bucket_name: str, key: str) -> Optional[str]:
    """
    Reads YAML from S3 for a chime-schema evolution

    Args:
      bucket_name: The name of the S3 bucket.
      key: The key of the JSON file in S3.

    Returns:
      Content of S3 file.
    """
    s3 = boto3.client("s3")

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")

    except Exception as e:
        logger.error(f"Error reading or parsing file from S3: {e}")
        return None


def split_struct_fields(s: str) -> list[str]:
    """Split struct fields handling nested structures"""
    result: list[str] = []
    current: str = ""
    bracket_count: int = 0

    for char in s:
        if char == "," and bracket_count == 0:
            result.append(current.strip())
            current = ""
        else:
            if char == "<":
                bracket_count += 1
            elif char == ">":
                bracket_count -= 1
            current += char

    if current:
        result.append(current.strip())

    return result


def parse_struct_type(type_str: str) -> list[dict[str, Any]]:
    """Parse struct type and return field information"""
    fields = split_struct_fields(type_str)
    struct_fields: list[dict[str, Any]] = []
    for field in fields:
        if ":" not in field:
            continue

        name, field_type = field.split(":", 1)
        if field_type.startswith(ARRAY_FIELD):
            array_of: str = field_type[len(ARRAY_FIELD) + 1 : -1]
            if array_of.startswith(STRUCT_FIELD):
                list_type = IcebergDataType.STRUCT_TYPE
                nested = True
            elif array_of.startswith(MAP_FIELD):
                list_type = IcebergDataType.MAP_TYPE
                nested = False
            elif array_of.startswith(ARRAY_FIELD):
                list_type = IcebergDataType.ARRAY_TYPE
                nested = False
            else:
                nested = False
                list_type: IcebergDataType = map_chime_type_to_iceberg_type(array_of)
            nested_fields = (
                parse_struct_type(array_of[len(STRUCT_FIELD) + 1 :]) if nested else None
            )
            struct_fields.append(
                {
                    "name": name,
                    "type": IcebergDataType.LIST_TYPE.value,
                    "field_type": IcebergDataType.LIST_TYPE,
                    "nested": IcebergDataType.LIST_TYPE.is_nested_type,
                    # 'array_of': array_of,
                    "fields": nested_fields,
                    "list_type": list_type,
                    "key_type": None,
                    "value_type": None,
                }
            )
        elif field_type.startswith(STRUCT_FIELD):
            nested_fields = parse_struct_type(field_type[len(STRUCT_FIELD) + 1 : -1])
            struct_fields.append(
                {
                    "name": name,
                    "type": IcebergDataType.STRING_TYPE.value,
                    "field_type": IcebergDataType.STRUCT_TYPE,
                    "nested": True,
                    "fields": nested_fields,
                    "key_type": None,
                    "value_type": None,
                    "list_type": None,
                }
            )
        elif field_type.startswith(MAP_FIELD):
            map_type = field_type[len(MAP_FIELD) + 1 : -1]
            kv_type = map_type.split(",")
            struct_fields.append(
                {
                    "name": name,
                    "type": IcebergDataType.MAP_TYPE.value,
                    "field_type": IcebergDataType.MAP_TYPE,
                    "nested": IcebergDataType.MAP_TYPE.is_nested_type,
                    "key_type": map_chime_type_to_iceberg_type(kv_type[0]),
                    "value_type": map_chime_type_to_iceberg_type(kv_type[1]),
                    "list_type": None,
                }
            )
        else:
            field_type = field_type[:-1] if field_type.endswith(">") else field_type
            struct_fields.append(
                {
                    "name": name,
                    "type": map_chime_type_to_iceberg_type(field_type).value,
                    "field_type": map_chime_type_to_iceberg_type(field_type),
                    "nested": map_chime_type_to_iceberg_type(field_type).is_nested_type,
                    "array_of": None,
                    "list_type": None,
                    "key_type": None,
                    "value_type": None,
                }
            )
    return struct_fields


def flatten_struct_fields(
    fields: list[dict[str, Any]], prefix: str = ""
) -> (list[dict[str, Any]], list[ParsedColumn]):
    """Flatten nested struct fields into a list of column definitions"""
    flat_columns = []
    parsed_flat_columns: list[ParsedColumn] = []

    for field in fields:
        field_name = f"{prefix}{field['name']}" if prefix else field["name"]

        if field.get("nested"):
            flat_columns.append(
                {
                    "column_name": field_name,
                    "field_type": field["field_type"],
                    "list_type": field["list_type"],
                    "is_required": False,
                }
            )
            parsed_flat_columns.append(
                ParsedColumn(
                    name=field_name,
                    iceberg_data_type=field["field_type"],
                    list_type=field["list_type"],
                    required=False,
                )
            )
            if "fields" in field and field["fields"] is not None:
                nested_columns, flat_parsed_flat_columns = flatten_struct_fields(
                    field["fields"], f"{field_name}."
                )
                flat_columns.extend(nested_columns)
                parsed_flat_columns.extend(flat_parsed_flat_columns)
        else:
            flat_columns.append(
                {
                    "column_name": field_name,
                    "field_type": field["field_type"],
                    "list_type": field["list_type"],
                    "key_type": field["key_type"],
                    "value_type": field["value_type"],
                    "is_required": False,
                }
            )
            parsed_flat_columns.append(
                ParsedColumn(
                    name=field_name,
                    iceberg_data_type=field["field_type"],
                    list_type=field["list_type"],
                    required=False,
                    map_key_type=field["key_type"],
                    map_value_type=field["value_type"],
                )
            )

    return flat_columns, parsed_flat_columns


def get_internal_struct(
    parent: str, field_list: list[ParsedColumn]
) -> list[NestedField]:
    """
    find all the fields in struct field list imp in struct field list which has all
    columns where column is complex column name.
    Remember this does not have non-complex fields which are already processed above.
    Child field of a structure could be complex or non-complex. Complex ones are
    already in sf_list so process only
    complex ones.
    """
    cf_list = list(
        filter(lambda cf: cf.parent is not None and cf.parent == parent, field_list)
    )
    return [cf.ns for cf in cf_list]


def p_process_struct_field(
    name: str,
    struct_definition: str,
    parent_type: IcebergDataType = IcebergDataType.STRUCT_TYPE,
    parent_list_type: IcebergDataType = None,
    parent_key_type: IcebergDataType = None,
    parent_value_type: IcebergDataType = None,
) -> list[ParsedColumn]:
    """

    Parameters
    ----------
    parent_value_type
    parent_key_type
    parent_list_type
    parent_type
    name
    struct_definition

    Returns
    -------

    """

    """
    sf_list = list of all fields in the struct unnested with attributes of
    parent and nested level. ncc_list = non complex field from sf_list.
    These are controlled by IcebergDataType.is_complex_type = False.
    cc_list = complex field from sf_list. These are controlled by
    IcebergDataType.is_complex_type = True.
    cf_list = child field list of cc_list.
    nf_list = Iceberg nested field list.
    """
    sd = (
        struct_definition[:-1] if struct_definition.endswith(">") else struct_definition
    )
    struct_level_field_list = parse_struct_type(sd)  # this will get level 1 fields
    parsed_sf_list: list[ParsedColumn] = []
    sf_list, parsed_sf_list_return = flatten_struct_fields(
        struct_level_field_list, f"{name}."
    )  # TODO - add period in the flatten routine if ends with
    parsed_sf_list.extend(parsed_sf_list_return)

    added_parent_list: list[ParsedColumn] = []
    for parsed_sf in parsed_sf_list:
        parent_exists: bool = (
            parsed_sf.parent is not None
            and len(
                list(filter(lambda cf: cf.name == parsed_sf.parent, parsed_sf_list))
            )
            > 0
        )
        if not parent_exists:
            if (
                len(
                    list(
                        filter(
                            lambda cf: cf.name == parsed_sf.parent, added_parent_list
                        )
                    )
                )
                == 0
            ):
                added_parent_list.append(
                    ParsedColumn(
                        name=parsed_sf.parent,
                        iceberg_data_type=parent_type,
                        list_type=parent_list_type,
                        map_key_type=parent_key_type,
                        map_value_type=parent_value_type,
                        nested_level=parsed_sf.nested_level - 1,
                        required=False,
                    )
                )

    parsed_sf_list.extend(added_parent_list)
    ncc_list: list[ParsedColumn] = [
        column
        for column in parsed_sf_list
        if not column.iceberg_data_type.is_nested_type
    ]
    for ncc in ncc_list:
        column_name = ncc.name.split(".")[-1]
        temp_pc_list: list[ParsedColumn] = p_get_iceberg_field_from_yaml_column(
            field_id=CREATE_TABLE_FIELD_ID,
            name=column_name,
            field_type=ncc.iceberg_data_type,
            required=ncc.required,
            parsed_column_reference=ncc,
            yaml_column_type=ncc.iceberg_data_type,
        )
        ncc.ns = temp_pc_list[0].ns
    cc_list: list[ParsedColumn] = [
        column for column in parsed_sf_list if column.iceberg_data_type.is_nested_type
    ]
    reverse_cc_list = sorted(
        cc_list, key=lambda column: column.nested_level, reverse=True
    )
    child_columns_to_remove: list[ParsedColumn] = []
    for reverse_cc in reverse_cc_list:
        process_reverse_cc(reverse_cc, parsed_sf_list, child_columns_to_remove)
    if child_columns_to_remove:
        for remove_cc in child_columns_to_remove:
            parsed_sf_list.remove(remove_cc)
    return parsed_sf_list


def process_reverse_cc(
    reverse_cc: ParsedColumn,
    parsed_sf_list: list[ParsedColumn],
    child_columns_to_remove: list[ParsedColumn],
):
    column_name = reverse_cc.name.split(".")[-1]
    # only 3 cases because only 3 nested fields.
    if reverse_cc.iceberg_data_type == IcebergDataType.STRUCT_TYPE:
        nf_list = get_internal_struct(reverse_cc.name, parsed_sf_list)
        ns: NestedField = NestedField(
            field_id=CREATE_TABLE_FIELD_ID,
            name=column_name,
            required=reverse_cc.required,
            field_type=StructType(*nf_list),
        )
        reverse_cc.ns = ns
    elif reverse_cc.iceberg_data_type == IcebergDataType.LIST_TYPE:
        if reverse_cc.list_type is not None:
            if reverse_cc.list_type == IcebergDataType.STRUCT_TYPE:
                nf_list = get_internal_struct(reverse_cc.name, parsed_sf_list)
                child_columns_to_remove.extend(
                    list(
                        filter(
                            lambda cf: cf.parent is not None
                            and cf.parent == reverse_cc.name,
                            parsed_sf_list,
                        )
                    )
                )
                reverse_cc.ns = NestedField(
                    field_id=CREATE_TABLE_FIELD_ID,
                    name=column_name,
                    field_type=ListType(
                        element_id=CREATE_TABLE_FIELD_ID,
                        element_type=StructType(*nf_list),
                    ),
                )
            elif (
                reverse_cc.list_type == IcebergDataType.LIST_TYPE
                or reverse_cc.list_type == IcebergDataType.MAP_TYPE
            ):
                logger.debug(f"process list of {reverse_cc.list_type.name}")
            else:
                reverse_cc.ns = NestedField(
                    field_id=-1,
                    name=column_name,
                    required=False,
                    field_type=ListType(-1, reverse_cc.list_type.native_iceberg_type),
                )
    elif reverse_cc.iceberg_data_type == IcebergDataType.MAP_TYPE:
        logger.info("TODO TODO IMPLEMENT process map")


def p_get_iceberg_field_from_yaml_column(
    field_id: int,
    name: str,
    field_type: IcebergDataType,
    yaml_column_type: Any,
    required: bool = False,
    parsed_column_reference: Optional[ParsedColumn] = None,
) -> list[ParsedColumn]:  # TODO remove none
    if not field_type.is_nested_type:
        if field_type == IcebergDataType.MAP_TYPE:  # TODO - explore more!
            map_key_type = (
                parsed_column_reference.map_key_type
                if parsed_column_reference is not None
                else map_chime_type_to_iceberg_type("string")
            )
            map_value_type = (
                parsed_column_reference.map_value_type
                if parsed_column_reference is not None
                else map_chime_type_to_iceberg_type("string")
            )
            map_ns: NestedField = NestedField(
                field_id=CREATE_TABLE_FIELD_ID,
                name=name,
                field_type=MapType(
                    key_id=CREATE_TABLE_FIELD_ID,
                    value_id=CREATE_TABLE_FIELD_ID,
                    key_type=map_key_type.native_iceberg_type,
                    value_type=map_value_type.native_iceberg_type,
                ),
                required=required,
            )
            return [
                ParsedColumn(
                    name=name,
                    iceberg_data_type=IcebergDataType.LIST_TYPE,
                    map_key_type=map_value_type,
                    map_value_type=map_value_type,
                    required=required,
                    ns=map_ns,
                )
            ]
        else:
            native_ns: NestedField = NestedField(
                field_id=field_id,
                name=name,
                field_type=field_type.native_iceberg_type,
                required=required,
            )
            return [
                ParsedColumn(
                    name=name,
                    iceberg_data_type=field_type.native_iceberg_type,
                    required=required,
                    ns=native_ns,
                )
            ]
    else:
        if field_type == IcebergDataType.STRUCT_TYPE:
            pc_return: list[ParsedColumn] = p_process_struct_field(
                name, yaml_column_type[len(STRUCT_FIELD) + 1 :]
            )
            return pc_return
        elif field_type == IcebergDataType.LIST_TYPE:
            array_of: str = yaml_column_type[len(ARRAY_FIELD) + 1 : -1]
            if array_of.startswith(STRUCT_FIELD):
                return p_process_struct_field(
                    name,
                    array_of[len(STRUCT_FIELD) + 1 :],
                    field_type,
                    IcebergDataType.STRUCT_TYPE,
                )
            elif array_of.startswith("list"):
                logger.info("TODO TODO - IMPLEMENT process array of map")
            elif array_of.startswith(ARRAY_FIELD):
                logger.info("TODO TODO - IMPLEMENT process array of array")
            else:
                # array of non-complex field
                element_type: IcebergDataType = map_chime_type_to_iceberg_type(array_of)
                non_complex_ns: NestedField = NestedField(
                    field_id=CREATE_TABLE_FIELD_ID,
                    name=name,
                    field_type=ListType(
                        element_id=CREATE_TABLE_FIELD_ID,
                        element_type=element_type.native_iceberg_type,
                        required=required,
                    ),
                )
                return [
                    ParsedColumn(
                        name=name,
                        iceberg_data_type=IcebergDataType.LIST_TYPE,
                        list_type=element_type,
                        required=required,
                        ns=non_complex_ns,
                    )
                ]
        elif field_type == IcebergDataType.MAP_TYPE:
            # handle simple map for now.
            map_type = yaml_column_type[len(MAP_FIELD) + 1 : -1]
            kv_type = map_type.split(",")
            key_type: IcebergDataType = map_chime_type_to_iceberg_type(kv_type[0])
            value_type: IcebergDataType = map_chime_type_to_iceberg_type(kv_type[1])
            ns: NestedField = NestedField(
                field_id=CREATE_TABLE_FIELD_ID,
                name=name,
                field_type=MapType(
                    key_id=CREATE_TABLE_FIELD_ID,
                    value_id=CREATE_TABLE_FIELD_ID,
                    key_type=key_type.native_iceberg_type,
                    value_type=value_type.native_iceberg_type,
                ),
                required=required,
            )
            return [
                ParsedColumn(
                    name=name,
                    iceberg_data_type=IcebergDataType.LIST_TYPE,
                    map_key_type=key_type,
                    map_value_type=value_type,
                    required=required,
                    ns=ns,
                )
            ]

        return []


def log_message(response: ProcessIcebergSchemaResponse, message: str):
    if response is not None:
        response.message_list.append(message)
    logger.info(message)


def process_iceberg_evolution(
    bucket: str, yaml_key: str, process_variables: ProcessVariables
) -> ProcessIcebergSchemaResponse:
    """
    Gets the schema from YAML content. This is the main loop which goes through each
    of the columns tag in the YAML file

    Parameters
    ----------
    yaml_content - yaml content to process

    Returns
    -------
    ProcessIcebergSchemaResponse - response object with message list.
    """
    response: ProcessIcebergSchemaResponse = ProcessIcebergSchemaResponse()
    yaml_content = read_from_s3(bucket, yaml_key)
    if yaml_content is not None:
        parsed_column_list: list[ParsedColumn] = []
        try:
            yaml_data = yaml.safe_load(yaml_content)
            yaml_columns = yaml_data.get("columns", [])
            yaml_resource_tags = yaml_data.get("resource_tags", [])
            database_name = yaml_data.get(
                "database_name", process_variables.bronze_db_name
            )
            table_name = f"{yaml_data.get('table_name', None)}"

            log_message(
                response,
                f"Database name: {database_name}. Table name: {table_name}."
                f"Column Count: {len(yaml_columns)}."
                f" Resource tags count: {len(yaml_resource_tags)}",
            )
            # *****START loop for each column in yaml columns to get ParsedColumn******
            for yaml_column in yaml_columns:
                yaml_column_name = yaml_column["name"]
                yaml_column_type = yaml_column["type"]
                log_message(
                    response,
                    f"****************Working with column: {yaml_column_name}. "
                    f"Column type: {yaml_column_type}",
                )
                iceberg_data_type: IcebergDataType = map_chime_type_to_iceberg_type(
                    yaml_column_type
                )
                log_message(
                    response,
                    f"IcebergData type: {iceberg_data_type} "
                    f"for yaml_column_type: {yaml_column}",
                )
                if (
                    iceberg_data_type is None
                    or iceberg_data_type == IcebergDataType.UNKNOWN_TYPE
                ):
                    log_message(
                        response,
                        f"Parse error. Column '{yaml_column_name}' "
                        f"protobuf type: {yaml_column_type} "
                        f"has unknown type '{yaml_column_type}'",
                    )
                    response.status = ProcessIcebergResponseStatus.INVALID
                    break  # break out the main loop and return from function
                else:
                    parsed_column_list.extend(
                        p_get_iceberg_field_from_yaml_column(
                            CREATE_TABLE_FIELD_ID,
                            yaml_column_name,
                            iceberg_data_type,
                            yaml_column_type,
                        )
                    )
            # ******END loop for each column in yaml columns to get ParsedColumn*********

            # **********START Build column hierarchy i.e. dictionary of each column
            # and its children.
            log_message(response, "START build column hierarchy")
            column_hierarchy: dict[str, list[str]] = {}
            for parsed_column in parsed_column_list:
                parsed_column.nested_level = (
                    1
                    if parsed_column.nested_level is None
                    else parsed_column.nested_level
                )
                child_column_list: list[str] = [
                    cf.name
                    for cf in parsed_column_list
                    if cf.parent == parsed_column.name
                ]
                column_hierarchy[parsed_column.name] = child_column_list
            log_message(response, "END build column hierarchy")
            # ***************END column hierarchy***************

            # ***************START check for duplicate flattened column names***************
            log_message(response, "START duplicate flattened column check")
            flattened_column_name_list = [
                column.flattened_name for column in parsed_column_list
            ]
            counter: Counter = Counter(flattened_column_name_list)
            duplicate_field_list = {
                key: count for key, count in counter.items() if count > 1
            }
            log_message(response, f"Duplicate field count: {len(duplicate_field_list)}")
            for dup in duplicate_field_list:
                duplicate_pc_list: list[ParsedColumn] = list(
                    filter(
                        lambda p: p.flattened_name == dup and p.name.__contains__("."),
                        parsed_column_list,
                    )
                )
                d = 1
                for dup_pc in duplicate_pc_list:
                    d += d
                    separator: str = "_" * d
                    dup_pc.flattened_name = separator.join(dup_pc.name.split("."))
            log_message(response, "END duplicate flattened column check")
            # ***************END check for duplicate flattened column names************

            """
            Go through each column. Look at all its ancestors and if we find any one 
            in the tree that is non-flattenable mark the field as non-flattenable.
            """
            process_broze_silver_layers(
                parsed_column_list,
                column_hierarchy,
                database_name,
                table_name,
                process_variables,
                response,
            )
        except Exception as e:
            log_message(response, e.__str__())
            response.status = ProcessIcebergResponseStatus.ERROR
    else:
        response.status = ProcessIcebergResponseStatus.EMPTY_INPUT_FILE
        response.message_list.append(
            f"{ProcessIcebergResponseStatus.EMPTY_INPUT_FILE.description} "
            f"{bucket}/{yaml_key}"
        )
    return response


def process_broze_silver_layers(
    parsed_column_list: list[ParsedColumn],
    column_hierarchy: dict[str, list[str]],
    database_name: str,
    table_name: str,
    process_variables: ProcessVariables,
    response: ProcessIcebergSchemaResponse,
):
    glue_catalog: Catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
        },
    )
    bronze_result: tuple[Schema, list[ParsedColumn]] = get_schema_by_level(
        parsed_column_list, column_hierarchy, 1
    )
    bronze_schema: Schema = bronze_result[0]

    identifier = f"{process_variables.bronze_db_name}.{table_name}"
    location = f"s3://{process_variables.bronze_s3_bucket}/{process_variables.bronze_db_name}/{table_name}"
    process_table(glue_catalog, identifier, bronze_schema, location, response=response)

    max_level: int = max(
        parsed_column_list, key=lambda obj: obj.nested_level
    ).nested_level
    if (
        process_variables.silver_flattening_level
        == Constants.SILVER_FLATTEN_TO_MAX_LEVEL
    ):  # full flatten
        silver_flattening_level = max_level
    elif max_level > process_variables.silver_flattening_level:
        # Table has more levels than max level
        silver_flattening_level = process_variables.silver_flattening_level
    else:
        # table has fewer levels than max level so use table level
        silver_flattening_level = max_level
    silver_result: Tuple[Schema, list[ParsedColumn]] = get_schema_by_level(
        parsed_column_list, column_hierarchy, silver_flattening_level
    )

    silver_schema: Schema = silver_result[0]
    level_column_list: list[ParsedColumn] = silver_result[1]

    identifier = f"{process_variables.silver_db_name}.{table_name}"
    location = f"s3://{process_variables.silver_s3_bucket}/{process_variables.silver_db_name}/{table_name}"
    process_table(glue_catalog, identifier, silver_schema, location, response=response)
    # PROS-50
    save_schema_registry(level_column_list, "silver", table_name, process_variables)


def process_table(
    glue_catalog: Catalog,
    identifier: str,
    schema: Schema,
    location: str,
    response: ProcessIcebergSchemaResponse,
):
    response.change_type = (
        IcebergSchemaChangeType.UPDATE_SCHEMA
        if glue_catalog.table_exists(identifier)
        else IcebergSchemaChangeType.CREATE_TABLE
    )

    log_message(response, f"Change: {response.change_type}. Identifier: {identifier}")
    if response.change_type == IcebergSchemaChangeType.CREATE_TABLE:
        glue_catalog.create_namespace_if_not_exists(identifier.split(".")[0])
        final_table: Table = glue_catalog.create_table(
            identifier, schema, location=location
        )
        log_message(response, f"Created table: {final_table.name()}")
    else:
        log_message(response, f"TODO - schema modification")
        verify_schema_change(identifier=identifier, schema=schema, response=response)


def verify_schema_change(
    identifier: str, schema: Schema, response: ProcessIcebergSchemaResponse
):
    # TODO - verify and update schema. Allow fields to add only at the end.
    # TODO - Allow meta fields to be added in the middle after last meta field.
    logger.info("Verifying schema change")


def get_schema_by_level(
    parsed_column_list: list[ParsedColumn],
    column_hierarchy: dict[str, list[str]],
    level: Optional[int] = None,
) -> (Optional[Schema], list[ParsedColumn]):
    """
    level_pc_list: list[ParsedColumn] = (
        parsed_column_list
        if level is None
        else [cf for cf in parsed_column_list if (cf.nested_level <= level)]
    )
    """

    if level is not None:
        level_pc_list = list(
            filter(
                lambda p: p.nested_level <= level,
                parsed_column_list,
            )
        )
    else:
        level_pc_list = parsed_column_list

    level_pc_column_name_list: list[str] = [cf.name for cf in level_pc_list]
    level_registry_list: list[ParsedColumn] = []
    if len(level_pc_list):
        level_ns: list[NestedField] = []
        for level_pc in level_pc_list:
            if len(set(column_hierarchy[level_pc.name])) == 0 or not set(
                column_hierarchy[level_pc.name]
            ).issubset(level_pc_column_name_list):
                level_ns.append(
                    NestedField(
                        field_id=level_pc.ns.field_id,
                        field_type=level_pc.ns.field_type,
                        name=level_pc.flattened_name,
                        required=level_pc.ns.required,
                    )
                )
                level_registry_list.append(level_pc)
        return Schema(*level_ns), level_registry_list
    else:
        return None


def save_schema_registry(
    level_pc_list: list[ParsedColumn],
    layer: str,
    table_name: str,
    process_variables: ProcessVariables,
):
    """This routine accepts list of parsed columns and saves it to
    iceberg_schema_registry in dynamodb.
    """
    registry_item_list: list[IcebergSchemaRegistryItem] = []
    field_order: int = 0
    for level_pc in level_pc_list:
        registry_item_list.append(
            IcebergSchemaRegistryItem(
                layer_db_table_column_key=f"{layer}"
                f"*{process_variables.silver_db_name}"
                f"*{table_name}"
                f"*{level_pc.flattened_name}",
                layer_db_table_gsi=f"silver"
                f"*{process_variables.silver_db_name}"
                f"*{table_name}",
                layer=layer,
                db_name=process_variables.silver_db_name,
                table_name=table_name,
                column_name=level_pc.flattened_name,
                source_mapping=level_pc.name,
                source_table=table_name,
                source_db=process_variables.bronze_db_name,
                field_order=field_order,
                # is_active=True,
            )
        )
        field_order += 1
    registry_table_name = "test_iceberg_schema_registry"
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(registry_table_name)

    with table.batch_writer() as batch:
        for item in registry_item_list:
            batch.put_item(Item=asdict(item))
    logger.info("Schema registry saved")


import json
import logging
import os

from class_definitions import (
    ProcessIcebergSchemaResponse,
    ProcessIcebergResponseStatus,
    ProcessVariables,
    PartitionSpecification,
)
from constants import DefaultDBName, DefaultS3Bucket, Constants
from lambda_helper import process_iceberg_evolution

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def gather_process_variables() -> ProcessVariables:
    process_variables = ProcessVariables(
        bronze_db_name=os.getenv("BRONZE_DB_NAME", DefaultDBName.BRONZE_DB.value),
        bronze_s3_bucket=os.getenv(
            "BRONZE_DB_S3_BUCKET", DefaultS3Bucket.BRONZE_S3.value
        ),
        silver_db_name=os.getenv("SILVER_DB_NAME", DefaultDBName.SILVER_DB.value),
        silver_s3_bucket=os.getenv(
            "SILVER_DB_S3_BUCKET", DefaultS3Bucket.SILVER_S3.value
        ),
        silver_flattening_level=os.getenv(
            "DEF_SILVER_FLATTENING_LEVEL", Constants.SILVER_FLATTEN_TO_MAX_LEVEL
        ),
    )
    env_silver_flattening_level = os.getenv("DEF_SILVER_FLATTENING_LEVEL")
    if env_silver_flattening_level is not None:
        try:
            process_variables.silver_flattening_level = int(env_silver_flattening_level)
        except ValueError:
            process_variables.silver_flattening_level = (
                Constants.SILVER_FLATTEN_TO_MAX_LEVEL
            )
    else:
        process_variables.silver_flattening_level = (
            Constants.SILVER_FLATTEN_TO_MAX_LEVEL
        )
    partition_transform: str = os.getenv(
        "DEF_ICEBERG_PARTITION_TRANSFORM", Constants.DEFAULT_ICEBERG_PARTITION_TRANSFORM
    )
    partition_field: str = os.getenv(
        "DEF_ICEBERG_PARTITION_COLUMN", Constants.DEFAULT_ICEBERG_PARTITION_FIELD
    )
    process_variables.partition_spec = [
        PartitionSpecification(
            partition_name=f"{partition_field}_" f"{partition_transform}",
            partition_field=partition_field,
            partition_transform=partition_transform,
        )
    ]

    return process_variables


def get_input_record(event) -> (list[tuple[str, str]], bool, str):
    """Reads SNS event. If no Records or no Sns or no Records inside Sns then error out"""
    record_list: list[tuple[str, str]] = []
    has_error: bool = False
    message: str = "Reading SNS event"
    if "Records" not in event:
        has_error = True
        message = "Records key not found found in SNS event"
        return record_list, has_error, message

    for record in event["Records"]:
        if "Sns" in record:
            sns = record["Sns"]
            if "Message" in sns:
                sns_message = sns["Message"]
                sns_records = json.loads(sns_message)
                if "Records" in sns_records:
                    s3_records = sns_records["Records"]
                    for s3_record in s3_records:
                        if "s3" in s3_record:
                            s3 = s3_record["s3"]
                            if "bucket" in s3 and "object" in s3:
                                bucket = s3["bucket"]["name"]
                                object_key = s3["object"]["key"]
                                record_list.append((bucket, object_key))
                                logger.info(
                                    f"bucket: {bucket}, object_key: {object_key}"
                                )
                            else:
                                has_error = True
                                message = "S3 key not found in SNS event"
                                logger.info(message)
                        else:
                            has_error = True
                            message = "S3 key not found in SNS event"
    return record_list, has_error, message


def lambda_handler(event, context):
    lambda_output = {"response": []}
    process_variables = gather_process_variables()

    event_parser: tuple[list[tuple[str, str]], bool, str] = get_input_record(event)
    record_list = event_parser[0]
    has_error = event_parser[1]
    message = event_parser[2]
    if not has_error or len(record_list) == 0:
        for record in record_list:
            bucket = record[0]
            object_key = record[1]
            logger.info(f"Processing {bucket}/{object_key}")
            if object_key.endswith(".yaml"):
                response: ProcessIcebergSchemaResponse = process_iceberg_evolution(
                    bucket=bucket,
                    yaml_key=object_key,
                    process_variables=process_variables,
                )
            else:
                logger.error(
                    f"Iceberg schema evolution failed for {object_key}. "
                    f"{ProcessIcebergResponseStatus.INVALID_FILE_TYPE.description}"
                )
                response = ProcessIcebergSchemaResponse(
                    status=ProcessIcebergResponseStatus.INVALID_FILE_TYPE,
                    message_list=[
                        ProcessIcebergResponseStatus.EMPTY_INPUT_FILE.description
                    ],
                )
            logger.info("*****************OUTPUT*************************")
            logger.info(f"Response Status: {response.status.value}")
            logger.info(f"Response Message: {response.status.description}")
            for message in response.message_list:
                logger.info(message)
    else:
        logger.error(f"Iceberg schema evolution failed for {message}.")
    return json.dumps(lambda_output)
