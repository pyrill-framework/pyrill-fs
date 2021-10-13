import os
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Dict, Iterable, List, Optional, Union
from urllib.parse import unquote, urlparse, urlunparse

from pyrill import (BaseProducer, BaseSink, BaseSource,
                    BytesChunksSlowStartSource, FrameSkippedError)

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

from ..base import (BaseFsManager, FileDescription, FileDescriptionMixin,
                    FileDescriptionType, ManagerSourceMixin)
from ..exceptions import (FileNotFoundError, FileNotReadableError,
                          InvalidSchemeError)
from .client import WebHDFSClient
from .models import FileStatus

__all__ = ['HDFSFileDescription', 'HDFSManager', 'HDFSFileSource', 'HDFSFileSink', 'HDFSListContent',
           'WebHDFSClient']


@dataclass(frozen=True)
class HDFSFileDescription(FileDescription):
    host: str = ''
    protocol: 'Final[str]' = 'hdfs'

    @property
    def uri(self) -> str:
        return urlunparse((self.protocol, self.host, str(self.full_path.resolve()), '', '', ''))


class HDFSManager(BaseFsManager[HDFSFileDescription]):

    def __init__(self,
                 *args,
                 client_params: Dict = None,
                 **kwargs):
        super(HDFSManager, self).__init__(*args, **kwargs)

        self._client: Optional[WebHDFSClient] = None
        self._client_params = client_params or {}

    @property
    def client(self) -> WebHDFSClient:
        return self._client

    async def exists(self, fd: HDFSFileDescription) -> bool:
        try:
            await self.load(fd)
        except FileNotFoundError:
            return False
        return True

    async def is_readable(self, fd: HDFSFileDescription) -> bool:
        try:
            await self.load(fd)
        except FileNotFoundError:
            return False
        return True

    async def is_writable(self, fd: HDFSFileDescription) -> bool:
        return True

    async def _load(self, fd: FileDescriptionType) -> FileDescriptionType:
        resp = await self.client.get_file_status(str(fd.full_path))
        return self.build_file_description(resp.file_status, fd.path, fd.name)

    async def _list_content(self,
                            fd: HDFSFileDescription,
                            recursive: bool = False) -> 'HDFSListContent':
        return HDFSListContent(manager=self, file_description=fd)

    async def _read_file(self,
                         fd: HDFSFileDescription) -> 'HDFSFileSource':
        return HDFSFileSource(manager=self, file_description=fd)

    async def _write_file(self,
                          fd: HDFSFileDescription,
                          stream: BaseProducer[bytes]):
        sink = stream >> HDFSFileSink(file_description=fd, manager=self)

        sink.consume_all()
        await sink.wait_until_eos()

    async def _remove_file(self,
                           fd: HDFSFileDescription):
        await self.client.remove(
            str(fd.full_path)
        )

    def _create_client(self, endpoint: Union[str, Iterable[str]] = None, user_name: str = '', **kwargs):
        endpoint = endpoint or os.environ.get('WEBHDFS_ENDPOINT').split(' ')
        user_name = user_name or os.environ.get('WEBHDFS_USER')
        return WebHDFSClient(endpoint=endpoint, user_name=user_name, **kwargs)

    async def _prepare_contexts(self):
        self._client = await self._context_collection.enter_async_context(self._create_client(
            **self._client_params
        ))

    @classmethod
    def build_file_description_from_uri(cls, uri: str) -> FileDescriptionType:
        parsed = urlparse(uri)
        scheme = parsed.scheme
        if '+' in scheme:
            scheme, fmt = scheme.split('+', 1)
        if scheme not in ['hdfs']:
            raise InvalidSchemeError(uri)

        path = Path(unquote(parsed.path))

        return HDFSFileDescription(
            name=path.name or '',
            path=path.parent if path.parent != path else None,
            size=0,
            type=FileDescription.ObjectType.UNKNOWN,
            loaded=False
        )

    @classmethod
    def build_file_description(cls, file_status: FileStatus, path: Path, name: str = '') -> HDFSFileDescription:
        return HDFSFileDescription(
            name=file_status.path_suffix or name,
            path=path,
            size=file_status.length,
            type=HDFSFileDescription.ObjectType.FILE
            if file_status.type == FileStatus.Type.FILE
            else HDFSFileDescription.ObjectType.DIRECTORY,
            metadata=file_status.export_data(),
            loaded=True
        )


class HDFSListContent(ManagerSourceMixin[HDFSManager],
                      FileDescriptionMixin[HDFSFileDescription],
                      BaseSource[HDFSFileDescription]):
    _buffer: Optional[List[HDFSFileDescription]] = None
    _load_finished: bool = False
    _continuation_token: Optional[str] = None

    async def _refill_buffer(self) -> List[HDFSFileDescription]:
        result = []
        if self._load_finished:
            return result

        if self.file_description.type == HDFSFileDescription.ObjectType.FILE:
            result.append(self.file_description)

        else:
            resp = await self.manager.client.list_dir(
                str(self.file_description.full_path)
            )

            for item in resp.file_statuses.file_status:
                result.append(self.manager.build_file_description(item, self.file_description.full_path))

        self._load_finished = True
        return result

    async def _next_frame(self) -> HDFSFileDescription:
        while True:
            if self._buffer is not None:
                try:
                    return self._buffer.pop()
                except IndexError:
                    pass

            if self._load_finished:
                raise StopAsyncIteration()

            self._buffer = await self._refill_buffer()

    async def _mount(self):
        await super(HDFSListContent, self)._mount()

        self._load_finished = False

    async def _unmount(self):
        self._load_finished = True
        await super(HDFSListContent, self)._unmount()


class HDFSFileSource(ManagerSourceMixin[HDFSManager],
                     FileDescriptionMixin[HDFSFileDescription],
                     BytesChunksSlowStartSource):
    _fd: Optional[AsyncIterator[bytes]] = None

    async def _next_chunk(self) -> bytes:
        while True:
            try:
                if self._open_buffer:
                    if self._fd is None:
                        raise RuntimeError('File description not initialized')
                    try:
                        data = await self._fd.__anext__()
                    except StopAsyncIteration:
                        self._open_buffer = False
                        self._fd = None
                    else:
                        await self.push_frame(data)

                return await super(HDFSFileSource, self)._next_chunk()
            except FrameSkippedError:
                continue

    async def _mount(self):
        if not await self.manager.is_readable(self.file_description):
            raise FileNotReadableError(self.file_description.uri)

        async def download() -> AsyncIterator[bytes]:
            obj = await self.manager.client.get(
                str(self.file_description.full_path)
            )

            async for data in obj:
                yield data

        self._fd = download()

        await super(HDFSFileSource, self)._mount()

    async def _unmount(self):
        await super(HDFSFileSource, self)._unmount()

        self._fd = None


class HDFSFileSink(ManagerSourceMixin[HDFSManager],
                   FileDescriptionMixin[HDFSFileDescription],
                   BaseSink[bytes]):

    async def consume_frame(self):
        async def consumer():
            while True:
                try:
                    yield await super(HDFSFileSink, self).consume_frame()
                except StopAsyncIteration:
                    break

        await self.manager.client.put(stream=consumer(), path=str(self.file_description.full_path), overwrite=True)
        raise StopAsyncIteration()
