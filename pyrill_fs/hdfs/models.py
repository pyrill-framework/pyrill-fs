from datetime import datetime, timezone
from enum import Enum

from dirty_models import ArrayField
from dirty_models import BaseModel as DirtyBaseModel
from dirty_models import BooleanField
from dirty_models import DateTimeField as BaseDateTimeField
from dirty_models import EnumField, IntegerField, ModelField, StringIdField
from dirty_models.models import CamelCaseMeta


class DateTimeField(BaseDateTimeField):
    def convert_value(self, value):
        try:
            return super(DateTimeField, self).convert_value(value)
        except ValueError as ex:
            if isinstance(value, int):
                return datetime.fromtimestamp(value / 1000, tz=self.default_timezone)
            else:
                raise ex


class BaseModel(DirtyBaseModel, metaclass=CamelCaseMeta):
    pass


class BooleanResponse(BaseModel):
    boolean = BooleanField()
    """
    A boolean value.
    """


class ContentSummary(BaseModel):
    directory_count = IntegerField()
    """
    The number of directories
    """

    file_count = IntegerField()
    """
    The number of files.
    """

    length = IntegerField()
    """
    The number of bytes used by the content.
    """

    quota = IntegerField()
    """
    The namespace quota of this directory.
    """

    space_consumed = IntegerField()
    """
    The disk space consumed by the content.
    """

    space_quota = IntegerField()
    """
    The disk space quota.
    """


class ContentSummaryResponse(BaseModel):
    content_summary = ModelField(model_class=ContentSummary, alias=['ContentSummary'])


class FileChecksum(BaseModel):
    algorithm = StringIdField()
    """
    The name of the checksum algorithm.
    """

    bytes = StringIdField()
    """
    The byte sequence of the checksum in hexadecimal.
    """

    length = IntegerField()
    """
    The length of the bytes (not the length of the string).
    """


class FileChecksumResponse(BaseModel):
    file_checksum = ModelField(model_class=FileChecksum, alias=['FileChecksum'])


class FileStatus(BaseModel):
    class Type(Enum):
        FILE = 'FILE'
        DIRECTORY = 'DIRECTORY'

    access_time = DateTimeField(default_timezone=timezone.utc)
    """
    The access time.
    """

    block_size = IntegerField()
    """
    The block size of a file.
    """

    group = StringIdField()
    """
    The group owner.
    """

    length = IntegerField()
    """
    The number of bytes in a file.
    """

    modification_time = DateTimeField(default_timezone=timezone.utc)
    """
    The modification time.
    """

    owner = StringIdField()
    """
    The user who is the owner.
    """

    path_suffix = StringIdField()
    """
    The path suffix.
    """

    permission = StringIdField()
    """
    The permission represented as a octal string.
    """

    replication = IntegerField()
    """
    The number of replication of a file.
    """

    type = EnumField(enum_class=Type)
    """
    The type of the path object.
    """


class FileStatusResponse(BaseModel):
    file_status = ModelField(model_class=FileStatus, alias=['FileStatus'])


class FileStatuses(BaseModel):
    file_status = ArrayField(field_type=ModelField(model_class=FileStatus), alias=['FileStatus'])


class FileStatusesResponse(BaseModel):
    file_statuses = ModelField(model_class=FileStatuses, alias=['FileStatuses'])


class LongResponse(BaseModel):
    long = IntegerField()
    """
    A long integer value.
    """


class PathResponse(BaseModel):
    path = StringIdField(alias=['Path'])
    """
    The string representation a Path.
    """


class Token(BaseModel):
    url_string = StringIdField()
    """
    A delegation token encoded as a URL safe string.
    """


class TokenResponse(BaseModel):
    token = ModelField(model_class=Token, alias=['Token'])


class RemoteException(BaseModel):
    exception = StringIdField()
    """
    Name of the exception.
    """

    message = StringIdField()
    """
    Exception message.
    """

    java_class_name = StringIdField()
    """
    Java class name of the exception
    """
