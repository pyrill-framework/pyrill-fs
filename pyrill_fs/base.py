from abc import ABC, abstractmethod
from asyncio import (AbstractEventLoop, Condition, Future, ensure_future,
                     get_event_loop, iscoroutinefunction)
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Dict, Generic, Optional, Tuple, TypeVar, Union
from urllib.parse import urlunparse
from weakref import WeakSet

try:
    from contextlib import AsyncExitStack
except ImportError:
    from async_exit_stack import AsyncExitStack

from pyrill import BaseProducer, BaseSink, BaseStage, Noop
from pyrill.base import Source_co

from .exceptions import (FileNotFoundError, FileNotReadableError,
                         FileNotWritableError)

__all__ = ['FileDescription', 'BaseFsManager', 'add_probe', 'FileCheckRemoveMixin', 'FileDescriptionMixin',
           'ListRecursiveContentStage', 'ManagerProbe', 'ReadFilesStage', 'RemoveAllFilesSink',
           'WriteAllFilesSink']


@dataclass(frozen=True)
class FileDescription(ABC):
    class ObjectType(Enum):
        FILE = 'file'
        DIRECTORY = 'directory'
        SPECIAL = 'special'
        UNKNOWN = 'unknown'

    name: str
    path: Optional[Path]
    size: int
    type: ObjectType
    protocol: str
    metadata: Dict = field(default_factory=dict)
    loaded: bool = False

    @property
    def full_path(self) -> Path:
        return (self.path or Path('/')) / self.name

    @property
    def uri(self) -> str:
        return urlunparse((self.protocol, '', str(self.full_path.resolve()), '', '', ''))


FileDescriptionType = TypeVar('FileDescriptionType', bound=FileDescription)


class FileDescriptionMixin(Generic[FileDescriptionType]):

    def __init__(self, *args, file_description: FileDescriptionType, **kwargs):
        super(FileDescriptionMixin, self).__init__(*args, **kwargs)

        self.file_description = file_description


class BaseFsManager(Generic[FileDescriptionType], ABC):

    def __init__(self, *, loop: AbstractEventLoop = None):
        self._loop = loop or get_event_loop()

        self._pendant_streams: WeakSet[Future] = WeakSet()
        self._context_collection = AsyncExitStack()
        self._open_fut: Optional[Future] = None
        self._close_fut: Optional[Future] = None
        self._pendant_event: Condition = Condition(loop=self._loop)

    @classmethod
    @abstractmethod
    def build_file_description_from_uri(cls, uri: str) -> FileDescriptionType:
        raise NotImplementedError()

    @abstractmethod
    async def exists(self, fd: FileDescriptionType) -> bool:  # pragma: no cover
        raise NotImplementedError()

    @abstractmethod
    async def is_readable(self, fd: FileDescriptionType) -> bool:  # pragma: no cover
        raise NotImplementedError()

    @abstractmethod
    async def is_writable(self, fd: FileDescriptionType) -> bool:  # pragma: no cover
        raise NotImplementedError()

    async def load(self, fd: FileDescriptionType) -> FileDescriptionType:
        if fd.loaded:
            return fd
        return await self._load(fd)

    @abstractmethod
    async def _load(self, fd: FileDescriptionType) -> FileDescriptionType:
        raise NotImplementedError()

    async def list_content(self,
                           fd: FileDescriptionType,
                           recursive: bool = False) -> Union[BaseProducer[FileDescriptionType],
                                                             'ListRecursiveContentStage[FileDescriptionType]']:
        fd = await self.load(fd)

        if not await self.exists(fd):
            raise FileNotFoundError(fd.uri)

        source = await self._list_content(fd) >> ManagerProbe(manager=self)

        if recursive:
            return source >> ListRecursiveContentStage[self.__class__, FileDescriptionType](manager=self)
        return source

    @abstractmethod
    async def _list_content(self,
                            fd: FileDescriptionType) -> BaseProducer[FileDescriptionType]:  # pragma: no cover
        raise NotImplementedError()

    async def read_file(self,
                        fd: FileDescriptionType) -> BaseProducer[bytes]:
        fd = await self.load(fd)

        if not await self.exists(fd):
            raise FileNotFoundError(fd.uri)
        if fd.type == FileDescription.ObjectType.DIRECTORY:
            raise FileNotReadableError(fd.uri)

        return await self._read_file(fd) >> ManagerProbe(manager=self)

    @abstractmethod
    async def _read_file(self,
                         fd: FileDescriptionType) -> BaseProducer[bytes]:  # pragma: no cover
        raise NotImplementedError()

    async def write_file(self,
                         fd: FileDescriptionType,
                         stream: BaseProducer[bytes]):
        if not await self.is_writable(fd):
            raise FileNotWritableError(fd.uri)

        await self._write_file(fd, stream >> ManagerProbe(manager=self))

    @abstractmethod
    async def _write_file(self,
                          fd: FileDescriptionType,
                          stream: BaseProducer[bytes]):  # pragma: no cover
        raise NotImplementedError()

    async def write_all_files_stream(self,
                                     stream: BaseProducer[Tuple[FileDescriptionType, BaseProducer[bytes]]]):
        sink = stream >> ManagerProbe(manager=self) >> WriteAllFilesSink(manager=self)

        sink.consume_all()
        await sink.wait_until_eos()

    async def remove_file(self,
                          fd: FileDescriptionType):
        fd = await self.load(fd)

        if not await self.exists(fd):
            return
        if not await self.is_writable(fd):
            raise FileNotWritableError(fd.uri)

        return await self._remove_file(fd)

    @abstractmethod
    async def _remove_file(self,
                           fd: FileDescriptionType):  # pragma: no cover
        raise NotImplementedError()

    async def remove_all_files_stream(self,
                                      stream: BaseProducer[FileDescriptionType]):
        sink = stream >> ManagerProbe(manager=self) >> RemoveAllFilesSink(manager=self)

        sink.consume_all()
        await sink.wait_until_eos()

    def add_pendant(self, fut: Future):
        if fut in self._pendant_streams or fut.done():
            return
        self._pendant_streams.add(fut)
        fut.add_done_callback(lambda f: self.remove_pendant(f))

    def remove_pendant(self, fut: Future):
        try:
            self._pendant_streams.remove(fut)
            ensure_future(self._notify_change(), loop=self._loop)
        except KeyError:
            pass

    async def _notify_change(self):
        async with self._pendant_event:
            self._pendant_event.notify_all()

    async def wait_until_finish_pendant(self):
        while True:
            if len(self._pendant_streams) == 0:
                return

            async with self._pendant_event:
                if len(self._pendant_streams) == 0:
                    return

                await self._pendant_event.wait()

    async def __aenter__(self):
        if self._open_fut is not None:
            return
        self._open_fut = Future()
        self._close_fut = None

        await self._context_collection.__aenter__()
        await self._prepare_contexts()

        return self

    async def _prepare_contexts(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._close_fut is None:
            self._close_fut = ensure_future(self._close(exc_type, exc_val, exc_tb))
            self._open_fut.set_result(None)

    async def _close(self, exc_type=None, exc_val=None, exc_tb=None):
        await self.wait_until_finish_pendant()
        await self._context_collection.__aexit__(exc_type, exc_val, exc_tb)
        self._open_fut = None

    async def close(self):
        if self._close_fut is None:
            self._close_fut = ensure_future(self._close())
            if self._open_fut is not None and not self._open_fut.done():
                self._open_fut.set_result(None)

        await self._close_fut


FsManagerType = TypeVar('FsManagerType', bound=BaseFsManager)


class ManagerSourceMixin(Generic[FsManagerType]):

    def __init__(self, *args, manager: FsManagerType, **kwargs):
        super(ManagerSourceMixin, self).__init__(*args, **kwargs)

        self.manager = manager


class ManagerProbe(ManagerSourceMixin, Noop[Source_co]):
    _fut: Optional[Future] = None

    def __init__(self, *args, **kwargs):
        super(ManagerProbe, self).__init__(*args, **kwargs)

        if self._fut is None or self._fut.done():
            self._fut = Future()
        self.manager.add_pendant(self._fut)

    async def _mount(self):
        if self._fut is None or self._fut.done():
            self._fut = Future()
            self.manager.add_pendant(self._fut)

        await super(ManagerProbe, self)._mount()

    async def _unmount(self):
        if self._fut is not None:
            if not self._fut.done():
                self._fut.set_result(None)
            self.manager.remove_pendant(self._fut)
            self._fut = None

        await super(ManagerProbe, self)._unmount()

    def __del__(self):
        if self._fut is not None and not self._fut.done():
            self._fut.set_result(None)
            self.manager.remove_pendant(self._fut)
        super(ManagerProbe, self).__del__()


def add_probe(func):
    @wraps(func)
    async def async_inner(self, *args, **kwargs):
        stream = await func(self, *args, **kwargs)

        return stream >> ManagerProbe(manager=self)

    @wraps(func)
    async def inner(self, *args, **kwargs):
        stream = func(self, *args, **kwargs)

        return stream >> ManagerProbe(manager=self)

    if iscoroutinefunction(func):
        return async_inner
    return inner


class ListRecursiveContentStage(ManagerSourceMixin[FsManagerType],
                                BaseStage[FileDescriptionType, FileDescriptionType]):
    _inner_iter: Optional['ListRecursiveContentStage'] = None
    _last_fd: Optional[FileDescriptionType] = None

    async def _next_frame(self) -> FileDescriptionType:
        while True:
            if self._inner_iter is None:
                self._last_fd = await super(ListRecursiveContentStage, self)._next_frame()
                self._inner_iter = await self.manager.list_content(
                    self._last_fd,
                    recursive=self._last_fd.type == FileDescription.ObjectType.DIRECTORY
                )

            try:
                return await self._inner_iter.__anext__()
            except StopAsyncIteration:
                await self._inner_iter.unmount()
                self._inner_iter = None
                fd = self._last_fd
                self._last_fd = None
                if fd.type == FileDescription.ObjectType.DIRECTORY:
                    return fd

    async def _mount(self):
        await super(ListRecursiveContentStage, self)._mount()

        self._inner_iter = None
        self._last_fd = None

    async def _unmount(self):
        try:
            await self._inner_iter.unmount()
        except AttributeError:
            pass

        self._inner_iter = None
        self._last_fd = None

        await super(ListRecursiveContentStage, self)._unmount()

    async def process_frame(self, frame: FileDescriptionType) -> FileDescriptionType:
        return frame


class FileCheckRemoveMixin(Generic[FileDescriptionType]):

    async def _consume_frame(self) -> FileDescriptionType:
        frame: FileDescriptionType = await super(FileCheckRemoveMixin, self)._consume_frame()
        if not await self.manager.exists(frame):
            raise FileNotFoundError(frame.uri)
        if not await self.manager.is_writable(frame):
            raise FileNotWritableError(frame.uri)

        return frame


class RemoveAllFilesSink(FileCheckRemoveMixin[FileDescriptionType],
                         ManagerSourceMixin[FsManagerType],
                         BaseSink[FileDescriptionType]):

    async def _consume_frame(self) -> FileDescriptionType:
        fd: FileDescriptionType = await super(RemoveAllFilesSink, self)._consume_frame()
        await self.manager.remove_file(fd)
        return fd


class WriteAllFilesSink(ManagerSourceMixin[FsManagerType], BaseSink[Tuple[FileDescriptionType, BaseProducer[bytes]]]):

    async def _consume_frame(self) -> FileDescription:
        fd, stream = await super(WriteAllFilesSink, self)._consume_frame()
        await self.manager.write_file(fd, stream)
        return fd


class ReadFilesStage(ManagerSourceMixin[FsManagerType],
                     BaseStage[FileDescriptionType,
                               Tuple[FileDescriptionType, BaseProducer[bytes]]]):

    async def process_frame(self, frame: FileDescriptionType) -> Tuple[FileDescriptionType, BaseProducer[bytes]]:
        stream = await self.manager.read_file(frame)
        return frame, stream
