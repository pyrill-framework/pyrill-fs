from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional
from urllib.parse import unquote, urlparse, urlunparse

import aiobotocore
from aiobotocore.client import AioBaseClient
from pyrill import (BaseConsumer, BaseProducer, BaseSink, BaseSource,
                    BytesChunksSlowStart, BytesChunksSlowStartSource)

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

from .base import (BaseFsManager, FileDescription, FileDescriptionMixin,
                   FileDescriptionType, ManagerSourceMixin)
from .exceptions import (FileNotFoundError, FileNotReadableError,
                         FileNotWritableError, InvalidSchemeError)

__all__ = ['S3FileDescription', 'S3FsManager', 'S3FileSink', 'S3FileSource', 'S3ListContent', 'BaseS3FileWrite']


@dataclass(frozen=True)
class S3FileDescription(FileDescription):
    bucket: str = ''
    protocol: 'Final[str]' = 's3'

    @property
    def uri(self) -> str:
        return urlunparse((self.protocol, self.bucket, str(self.full_path.resolve()), '', '', ''))


class S3FsManager(BaseFsManager[S3FileDescription]):

    def __init__(self,
                 *args,
                 session: aiobotocore.AioSession = None,
                 aws_params: Dict = None, **kwargs):
        super(S3FsManager, self).__init__(*args, **kwargs)

        self._session = session or aiobotocore.get_session()
        self._client: Optional[AioBaseClient] = None
        self._aws_params = aws_params or {}

    @property
    def client(self) -> AioBaseClient:
        return self._client

    async def exists(self, fd: S3FileDescription) -> bool:
        try:
            await self.load(fd)
        except FileNotFoundError:
            return False
        return True

    async def is_readable(self, fd: S3FileDescription) -> bool:
        try:
            await self.load(fd)
        except FileNotFoundError:
            return False
        return True

    async def is_writable(self, fd: S3FileDescription) -> bool:
        return True

    async def _load(self, fd: FileDescriptionType) -> FileDescriptionType:
        from botocore.exceptions import ClientError

        if str(fd.full_path) in ['', '/']:
            return self._map_from_prefix({'Prefix': str(fd.full_path)}, bucket=fd.bucket)

        path = str(fd.full_path).lstrip('/')
        try:
            metadata = await self._client.get_object(
                Bucket=fd.bucket,
                Key=str(fd.full_path).lstrip('/')
            )
        except ClientError:
            resp = await self._client.list_objects_v2(
                Bucket=fd.bucket,
                EncodingType='url',
                Delimiter='/',
                MaxKeys=1,
                Prefix=path + '/' if len(path.strip('/')) else '',
                FetchOwner=False
            )
            if len(resp.get('Contents', [])) == 0 and len(resp.get('CommonPrefixes', [])) == 0:
                raise FileNotFoundError(fd.uri)

            return self._map_from_prefix({'Prefix': str(fd.full_path)}, bucket=fd.bucket)
        else:
            return self._map_from_object_metadata(metadata, bucket=fd.bucket, key=path)

    async def _list_content(self,
                            fd: S3FileDescription) -> 'S3ListContent':
        return S3ListContent(manager=self, file_description=fd)

    async def _read_file(self,
                         fd: S3FileDescription) -> 'S3FileSource':
        return S3FileSource(manager=self, file_description=fd)

    async def _write_file(self,
                          fd: S3FileDescription,
                          stream: BaseProducer[bytes]):
        sink = stream \
            >> BytesChunksSlowStart(min_size=1024 * 1024 * 5, max_size=1024 * 1024 * 50) \
            >> S3FileSink(file_description=fd, manager=self)

        sink.consume_all()
        await sink.wait_until_eos()

    async def _remove_file(self,
                           fd: S3FileDescription):
        await self.client.delete_object(
            Bucket=fd.bucket,
            Key=str(fd.full_path).lstrip('/')
        )

    async def _prepare_contexts(self):
        self._client = await self._context_collection.enter_async_context(self._session.create_client(
            's3',
            **self._aws_params
        ))

    @classmethod
    def build_file_description_from_uri(cls, uri: str) -> FileDescriptionType:
        parsed = urlparse(uri)
        scheme = parsed.scheme
        if '+' in scheme:
            scheme, fmt = scheme.split('+', 1)
        if scheme not in ['s3', 's3a', 's3n']:
            raise InvalidSchemeError(uri)

        path = Path(unquote(parsed.path))
        return S3FileDescription(
            name=path.name,
            path=path.parent if path.parent != path else None,
            bucket=parsed.hostname,
            size=0,
            type=FileDescription.ObjectType.UNKNOWN,
            loaded=False
        )

    @classmethod
    def build_file_description(cls, data: Dict, bucket: str, is_prefix=False) -> S3FileDescription:
        if is_prefix:
            return cls._map_from_prefix(data, bucket)
        return cls._map_from_object_metadata(data, bucket)

    @classmethod
    def _map_from_prefix(cls,
                         prefix: Dict,
                         bucket: str) -> 'S3FileDescription':
        path = Path('/' + unquote(prefix['Prefix']).lstrip('/'))
        name = path.name
        parent = path.parent
        if str(parent) == '.':
            parent = None

        return S3FileDescription(
            name=name,
            path=parent,
            type=FileDescription.ObjectType.DIRECTORY,
            bucket=bucket,
            size=0,
            loaded=True
        )

    @classmethod
    def _map_from_object_metadata(cls,
                                  metadata: Dict,
                                  bucket: str,
                                  key=None) -> 'S3FileDescription':
        path = Path('/' + unquote(metadata.pop('Key', key)).lstrip('/'))
        name = path.name
        parent = path.parent
        if str(parent) == '.':
            parent = None

        return S3FileDescription(
            name=name,
            path=parent,
            type=FileDescription.ObjectType.FILE,
            bucket=bucket,
            size=metadata.pop('Size',
                              metadata.pop('ContentLength', 0)),
            metadata=metadata,
            loaded=True
        )


class S3ListContent(ManagerSourceMixin[S3FsManager],
                    FileDescriptionMixin[S3FileDescription],
                    BaseSource[S3FileDescription]):
    _buffer: Optional[List[S3FileDescription]] = None
    _load_finished: bool = False
    _continuation_token: Optional[str] = None

    async def _refill_buffer(self) -> List[S3FileDescription]:
        result = []
        if self._load_finished:
            return result

        if self.file_description.type == S3FileDescription.ObjectType.FILE:
            result.append(self.file_description)
            self._load_finished = True
            return result

        extra_params = {}
        if self._continuation_token:
            extra_params['ContinuationToken'] = self._continuation_token

        resp = await self.manager.client.list_objects_v2(
            Bucket=self.file_description.bucket,
            EncodingType='url',
            Delimiter='/',
            MaxKeys=100,
            Prefix=(str(self.file_description.full_path).strip('/') + '/'
                    if len(str(self.file_description.full_path).strip('/'))
                    else ''),
            FetchOwner=False,
            **extra_params
        )

        for item in resp.get('Contents', []):
            if str(self.file_description.full_path).strip('/') == item['Key'].strip('/'):
                continue
            result.append(self.manager.build_file_description(item,
                                                              bucket=self.file_description.bucket,
                                                              is_prefix=False))

        for item in resp.get('CommonPrefixes', []):
            if str(self.file_description.full_path).strip('/') == item['Prefix'].strip('/'):
                continue
            result.append(self.manager.build_file_description(item,
                                                              bucket=self.file_description.bucket,
                                                              is_prefix=True))

        try:
            self._continuation_token = resp['NextContinuationToken']
        except KeyError:
            pass

        if not resp['IsTruncated']:
            self._load_finished = True

        return result

    async def _next_frame(self) -> S3FileDescription:
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
        await super(S3ListContent, self)._mount()

        self._load_finished = False

    async def _unmount(self):
        self._load_finished = True
        await super(S3ListContent, self)._unmount()


class S3FileSource(ManagerSourceMixin[S3FsManager],
                   FileDescriptionMixin[S3FileDescription],
                   BytesChunksSlowStartSource):
    _fd: Optional[AsyncIterator[bytes]] = None

    async def _next_chunk(self) -> bytes:
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

        return await super(S3FileSource, self)._next_chunk()

    async def _mount(self):
        if not await self.manager.is_readable(self.file_description):
            raise FileNotReadableError(self.file_description.uri)

        async def download() -> AsyncIterator[bytes]:
            obj = await self.manager.client.get_object(
                Bucket=self.file_description.bucket,
                Key=str(self.file_description.full_path).lstrip('/')
            )

            async for data in obj['Body']:
                yield data

        self._fd = download()

        await super(S3FileSource, self)._mount()

    async def _unmount(self):
        await super(S3FileSource, self)._unmount()

        self._fd = None


class BaseS3FileWrite(ManagerSourceMixin[S3FsManager],
                      FileDescriptionMixin[S3FileDescription],
                      BaseConsumer[bytes]):
    _upload_id: Optional[str] = None
    _part_etags: Optional[List[str]] = None

    async def _consume_frame(self) -> bytes:
        try:
            frame = await super(BaseS3FileWrite, self)._consume_frame()
        except StopAsyncIteration:
            if self._upload_id is not None:
                await self._close_file()
            raise
        else:
            if self._upload_id is None:
                if len(frame) >= 1024 * 1024 * 5:
                    await self._open_file()
                else:
                    await self._write_file(frame)
                    return frame
            await self._write_chunk(frame)

        return frame

    async def _open_file(self):
        upload_resp = await self.manager.client.create_multipart_upload(
            Bucket=self.file_description.bucket,
            Key=str(self.file_description.full_path).lstrip('/')
        )

        self._upload_id = upload_resp['UploadId']
        self._part_etags = []

    async def _write_file(self, frame: bytes):
        await self.manager.client.put_object(
            Bucket=self.file_description.bucket,
            Key=str(self.file_description.full_path).lstrip('/'),
            Body=frame
        )

    async def _write_chunk(self, chunk: bytes):
        if self._upload_id is None or self._part_etags is None:
            raise RuntimeError('S3 object not created before upload')

        part_resp = await self.manager.client.upload_part(
            Bucket=self.file_description.bucket,
            Key=str(self.file_description.full_path).lstrip('/'),
            Body=chunk,
            UploadId=self._upload_id,
            PartNumber=len(self._part_etags) + 1
        )

        self._part_etags.append(part_resp['ETag'])

    async def _close_file(self):
        if self._upload_id is None or self._part_etags is None:
            return
        upload_id = self._upload_id
        self._upload_id = None

        await self.manager.client.complete_multipart_upload(
            Bucket=self.file_description.bucket,
            Key=str(self.file_description.full_path).lstrip('/'),
            UploadId=upload_id,
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': i + 1,
                        'ETag': etag
                    }
                    for i, etag in enumerate(self._part_etags)
                ]
            }
        )

        self._part_etags = None

    async def _abort_file(self):
        if self._upload_id is None:
            return
        upload_id = self._upload_id
        self._upload_id = None

        await self.manager.client.abort_multipart_upload(
            Bucket=self.file_description.bucket,
            Key=str(self.file_description.full_path).lstrip('/'),
            UploadId=upload_id,
        )

        self._part_etags = None

    async def _mount(self):
        if not await self.manager.is_writable(self.file_description):
            raise FileNotWritableError(self.file_description.uri)

        self._upload_id = None
        self._part_etags = None

        await super(BaseS3FileWrite, self)._mount()

    async def _unmount(self):
        await super(BaseS3FileWrite, self)._unmount()

        try:
            if self._upload_id is not None:
                await self._abort_file()
        except (AttributeError, OSError):
            pass

        self._upload_id = None
        self._part_etags = None


class S3FileSink(BaseS3FileWrite, BaseSink[bytes]):
    pass
