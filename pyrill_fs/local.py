import os
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator, Optional, Union
from urllib.parse import urlparse

from pyrill import (BaseConsumer, BaseProducer, BaseSink, BaseSource,
                    BytesChunksSlowStartSource)

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

from .base import (BaseFsManager, FileDescription, FileDescriptionMixin,
                   ManagerSourceMixin)
from .exceptions import InvalidSchemeError, NotFileError

__all__ = ['LocalFileDescription', 'LocalFsManager', 'LocalFileSource', 'LocalFileSink',
           'BaseLocalFileWrite', 'LocalListContent']


@dataclass(frozen=True)
class LocalFileDescription(FileDescription):
    protocol: 'Final[str]' = 'file'


class LocalFsManager(BaseFsManager[LocalFileDescription]):

    async def exists(self, fd: LocalFileDescription) -> bool:
        return fd.full_path.exists()

    async def is_readable(self, fd: LocalFileDescription) -> bool:
        return await self.exists(fd) and os.access(fd.full_path, os.R_OK)

    async def is_writable(self, fd: LocalFileDescription) -> bool:
        return os.access(fd.full_path, os.W_OK) or \
            (not await self.exists(fd) and await self.is_writable(self.build_file_description(fd.path)))

    async def _load(self, fd: LocalFileDescription) -> LocalFileDescription:
        return self.build_file_description(fd.full_path)

    async def _list_content(self,
                            fd: LocalFileDescription) -> 'LocalListContent':
        return LocalListContent(manager=self, file_description=fd)

    async def _read_file(self,
                         fd: LocalFileDescription) -> 'LocalFileSource':
        return LocalFileSource(manager=self, file_description=fd)

    async def _write_file(self,
                          fd: LocalFileDescription,
                          stream: BaseProducer[bytes]):
        if fd.full_path.exists() and not fd.full_path.is_file():
            raise NotFileError(fd.uri)
        sink: BaseSink = stream >> LocalFileSink(manager=self, file_description=fd)

        sink.consume_all()
        await sink.wait_until_eos()

    async def _remove_file(self,
                           fd: LocalFileDescription):
        if fd.type == FileDescription.ObjectType.DIRECTORY:
            fd.full_path.rmdir()
        else:
            fd.full_path.unlink()

    async def _prepare_contexts(self):
        pass

    @classmethod
    def build_file_description(cls, path: Union[str, Path]) -> LocalFileDescription:
        path = Path(path)

        return LocalFileDescription(
            name=path.name,
            path=path.parent if path.parent != path else None,
            size=path.stat().st_size or 0 if path.is_file() else 0,
            type=FileDescription.ObjectType.FILE if path.is_file() else FileDescription.ObjectType.DIRECTORY,
            loaded=True
        )

    @classmethod
    def build_file_description_from_uri(cls, uri: str) -> LocalFileDescription:
        parsed = urlparse(uri)
        scheme = parsed.scheme
        if '+' in scheme:
            scheme, fmt = scheme.split('+', 1)
        if scheme not in ['file', 'local']:
            raise InvalidSchemeError(uri)

        path = Path(parsed.path)
        return LocalFileDescription(
            name=path.name,
            path=path.parent if path.parent != path else None,
            size=0,
            type=FileDescription.ObjectType.UNKNOWN,
            loaded=False
        )


class LocalListContent(ManagerSourceMixin[LocalFsManager],
                       FileDescriptionMixin[LocalFileDescription],
                       BaseSource[LocalFileDescription]):
    _iter: Optional[Iterator[Union[Path, LocalFileDescription]]] = None

    async def _next_frame(self) -> LocalFileDescription:
        try:
            p = self._iter.__next__()
        except StopIteration:
            raise StopAsyncIteration()

        if isinstance(p, LocalFileDescription):
            return p

        return self.manager.build_file_description(p)

    async def _mount(self):
        await super(LocalListContent, self)._mount()

        if self.file_description.type == FileDescription.ObjectType.FILE:
            self._iter = iter([self.file_description, ])
        else:
            self._iter = self.file_description.full_path.iterdir()

    async def _unmount(self):
        self._iter = None
        await super(LocalListContent, self)._unmount()


class LocalFileSource(ManagerSourceMixin[LocalFsManager],
                      FileDescriptionMixin[LocalFileDescription],
                      BytesChunksSlowStartSource):
    _fd: Optional[BinaryIO] = None

    async def _next_chunk(self) -> bytes:
        if self._open_buffer:
            if self._fd is None:
                raise RuntimeError('File description not initialized')
            self._calculate_chunk_size()
            data = self._fd.read(self._min_size)
            if len(data) == 0:
                self._open_buffer = False
                self._fd = None
                raise StopAsyncIteration()
            return data

        raise StopAsyncIteration()

    async def _mount(self):
        self._fd = self.file_description.full_path.open(mode='rb')

        await super(LocalFileSource, self)._mount()

    async def _unmount(self):
        await super(LocalFileSource, self)._unmount()

        try:
            self._fd.close()
        except (AttributeError, OSError):
            pass

        self._fd = None


class BaseLocalFileWrite(ManagerSourceMixin[LocalFsManager],
                         FileDescriptionMixin[LocalFileDescription],
                         BaseConsumer[bytes]):
    _fd: Optional[BinaryIO] = None

    async def _consume_frame(self) -> bytes:
        if self._fd is None:
            raise RuntimeError('File description not initialized')

        frame = await super(BaseLocalFileWrite, self)._consume_frame()
        self._fd.write(frame)
        self._fd.flush()

        return frame

    async def _mount(self):
        self._fd = self.file_description.full_path.open(mode='wb')

        await super(BaseLocalFileWrite, self)._mount()

    async def _unmount(self):
        await super(BaseLocalFileWrite, self)._unmount()

        try:
            self._fd.close()
        except (AttributeError, OSError):
            pass

        self._fd = None


class LocalFileSink(BaseLocalFileWrite, BaseSink[bytes]):
    pass
