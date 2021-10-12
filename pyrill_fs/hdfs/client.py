from asyncio import AbstractEventLoop
from datetime import datetime
from functools import wraps
from logging import Logger, getLogger
from typing import Iterable, List, Union

from aiohttp import ContentTypeError
from service_client import ServiceClient
from service_client.json import json_decoder, json_encoder
from service_client.plugins import BasePlugin, PathTokens, QueryParams

from .errors import FileNotFound, WebHDFSError
from .models import (ContentSummaryResponse, FileChecksumResponse,
                     FileStatusesResponse, FileStatusResponse, PathResponse,
                     RemoteException)
from .webhdfs_api_spec import WEBHDFS_SPEC


def none_response(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        await func(*args, **kwargs)

        return None

    return wrapper


def boolean_response(func):
    @wraps(func)
    async def wrapper(*args, **kwargs) -> bool:
        response = await func(*args, **kwargs)

        return response.data['boolean']

    return wrapper


def object_response(klass):
    def inner(func):
        @wraps(func)
        async def wrapper(*args, **kwargs) -> klass:
            response = await func(*args, **kwargs)

            return klass(data=response.data)

        return wrapper

    return inner


def stream_response(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        response = await func(*args, **kwargs)

        return response.content

    return wrapper


def raise_exception_on_error(allowed_status_code=None):
    allowed_status_code = allowed_status_code or [200, ]

    def inner(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            response = await func(*args, **kwargs)

            if response.status not in allowed_status_code:
                error = RemoteException(data=response.data['RemoteException'])

                if error.exception == 'FileNotFoundException':
                    raise FileNotFound(error=error)
                raise WebHDFSError(error=error)

            return response

        return wrapper

    return inner


def select_service_node(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if self._service is None:
            for service in self._service_nodes:
                self._service = service

                try:
                    resp = await self.service.get_home_directory()

                    if resp.status not in [200, ]:
                        continue

                    resp_data = await resp.json()

                    resp = await self.service.get_file_status(file_path=resp_data['Path'])

                    if resp.status not in [200, ]:
                        continue

                except Exception:
                    pass
                else:
                    break

        try:
            return await func(self, *args, **kwargs)
        except WebHDFSError as ex:
            if ex.model.exception == 'StandbyException':
                self._service = None
            raise

    return wrapper


def retry(func=None, default=0):
    def inner(f):
        @wraps(f)
        async def wrapper(self: 'WebHDFSClient', *args, retries=default, **kwargs):
            try:
                return await f(self, *args, **kwargs)
            except FileNotFound:
                raise
            except Exception as ex:
                self.logger.exception(ex)
                if retries > 0:
                    self.logger.warning(f'Retrying... pendant retries: {retries - 1}')
                    return await wrapper(self, *args, retries=retries - 1, **kwargs)
                raise ex

        return wrapper

    if func:
        return inner(func)
    return inner


class WebHDFSClient:

    def __init__(self, endpoint: Union[Iterable[str], str], name: str = 'WebHFDS',
                 user_name: str = None,
                 plugins: List[BasePlugin] = None,
                 logger: Logger = None,
                 loop: AbstractEventLoop = None):
        default_plugins = [PathTokens(),
                           QueryParams(default_query_params={'user.name': user_name})]

        default_plugins.extend(plugins or [])

        self._service = None
        self._service_nodes = []
        self.logger = logger or getLogger(name)

        if isinstance(endpoint, str):
            endpoint = [endpoint, ]

        for ep in endpoint:
            self._service_nodes.append(ServiceClient(name=name,
                                                     base_path=f'{ep.rstrip("/")}/webhdfs/v1/',
                                                     parser=json_decoder,
                                                     serializer=json_encoder,
                                                     spec=WEBHDFS_SPEC,
                                                     plugins=default_plugins,
                                                     logger=self.logger,
                                                     loop=loop))

    @property
    def service(self):
        return self._service

    @select_service_node
    @raise_exception_on_error(allowed_status_code=[201, ])
    async def put(self, stream, path: str, overwrite: bool = False, block_size: int = None,
                  replication: int = None, permission: str = None, buffer_size: int = None):
        query_params = {'overwrite': str(overwrite).lower()}

        if block_size is not None:
            query_params['blocksize'] = block_size

        if replication is not None:
            query_params['replication'] = replication

        if permission is not None:
            query_params['permission'] = permission

        if buffer_size is not None:
            query_params['buffersize'] = buffer_size

        response = await self.service.put(file_path=path.lstrip('/'), params=query_params, allow_redirects=False)

        if response.status != 307:
            return response

        async with self.service.session.put(response.headers['location'], data=stream) as resp:
            try:
                resp.data = await resp.json()
            except ContentTypeError:
                pass

            return resp

    @select_service_node
    @raise_exception_on_error()
    async def append(self, stream, path: str, buffer_size: int = None):
        query_params = {}

        if buffer_size is not None:
            query_params['buffer_size'] = buffer_size

        response = await self.service.append(file_path=path.lstrip('/'), params=query_params, allow_redirects=False)

        if response.status != 307:
            return response

        async with self.service.session.put(response.headers['location'], data=stream, params=query_params) as resp:
            try:
                resp.data = await resp.json()
            except ContentTypeError:
                pass

            return resp

    @select_service_node
    @stream_response
    @raise_exception_on_error()
    @retry(default=0)
    async def get(self, path: str, offset: int = None, length: int = None, buffer_size: int = None):
        query_params = {}
        if offset is not None:
            query_params['offset'] = offset

        if length is not None:
            query_params['length'] = length

        if buffer_size is not None:
            query_params['buffer_size'] = buffer_size
        return await self.service.get(file_path=path.lstrip('/'), params=query_params)

    @select_service_node
    @boolean_response
    @raise_exception_on_error()
    async def mkdir(self, path: str, permission: str = None):
        query_params = {}

        if permission is not None:
            query_params['permission'] = permission

        return await self.service.mkdir(file_path=path.lstrip('/'), params=query_params)

    @select_service_node
    @boolean_response
    @raise_exception_on_error()
    async def move(self, orig_path: str, dest_path: str):
        return await self.service.move(file_path=orig_path.lstrip('/'), params={'destination': dest_path})

    @select_service_node
    @boolean_response
    @raise_exception_on_error()
    async def remove(self, path: str, recursive: bool = False):
        return await self.service.remove(file_path=path.lstrip('/'), params={'recursive': str(recursive)})

    @select_service_node
    @object_response(FileStatusResponse)
    @raise_exception_on_error()
    async def get_file_status(self, path: str):
        return await self.service.get_file_status(file_path=path.lstrip('/'))

    @select_service_node
    @object_response(FileStatusesResponse)
    @raise_exception_on_error()
    async def list_dir(self, path: str):
        return await self.service.list_dir(file_path=path)

    @select_service_node
    @object_response(ContentSummaryResponse)
    @raise_exception_on_error()
    async def get_content_summary(self, path: str):
        return await self.service.get_content_summary(file_path=path.lstrip('/'))

    @object_response(FileChecksumResponse)
    @raise_exception_on_error()
    async def get_file_checksum(self, path: str):
        return await self.service.get_file_checksum(file_path=path.lstrip('/'))

    @select_service_node
    @object_response(PathResponse)
    @raise_exception_on_error()
    async def get_home_directory(self, path: str):
        return await self.service.get_home_directory(file_path=path.lstrip('/'))

    @select_service_node
    @boolean_response
    @raise_exception_on_error()
    async def chmod(self, path: str, permission: str):
        return await self.service.chmod(file_path=path.lstrip('/'), params={'permission': permission})

    @select_service_node
    @boolean_response
    @raise_exception_on_error()
    async def chown(self, path: str, user: str = None, group: str = None):
        query_params = {}

        if user is not None:
            query_params['user'] = user

        if group is not None:
            query_params['group'] = group

        return await self.service.chown(file_path=path.lstrip('/'), params=query_params)

    @select_service_node
    @boolean_response
    @raise_exception_on_error()
    async def set_replication(self, path: str, replication: int):
        return await self.service.set_replication(file_path=path.lstrip('/'), params={'replication': replication})

    @select_service_node
    @none_response
    @raise_exception_on_error()
    async def set_times(self, path: str, access_time: datetime = None, modification_time: datetime = None):
        query_params = {}

        if access_time is not None:
            query_params['accesstime'] = access_time

        if modification_time is not None:
            query_params['modificationtime'] = modification_time

        return await self.service.set_times(file_path=path.lstrip('/'), params=query_params)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        for service in self._service_nodes:
            await service.session.close()


def clean_path(file_name: str):
    if file_name.startswith('hdfs://'):
        return file_name[len('hdfs://'):]
    return file_name
