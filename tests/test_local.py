from functools import wraps
from pathlib import Path
from tempfile import mkdtemp, mkstemp
from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import SumAcc
from pyrill.base import BaseProducer
from pyrill.sinks import Last
from pyrill.sources import SyncSource

from pyrill_fs.base import FileDescription, ReadFilesStage
from pyrill_fs.exceptions import FileNotFoundError as PyRillFSFileNotFoundError
from pyrill_fs.exceptions import FileNotReadableError, NotFileError
from pyrill_fs.local import LocalFsManager


def make_context(func):
    @wraps(func)
    async def wrapper(self):
        async with self.manager:
            return await func(self)
        await self.manager.close()

    return wrapper


class BaseLocalFsManagerTestCase(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.manager = LocalFsManager()


class BuildFileDescriptionTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_build_file_description_file(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'file_1.csv')

        self.assertEqual(fd.name, 'file_1.csv')
        self.assertEqual(fd.path, (Path(__file__).parent / 'data').resolve())
        self.assertEqual(fd.full_path, (Path(__file__).parent / 'data' / 'file_1.csv').resolve())
        self.assertEqual(fd.type, FileDescription.ObjectType.FILE)
        self.assertEqual(fd.protocol, 'file')
        self.assertEqual(fd.metadata, {})
        self.assertEqual(fd.size, 38)
        self.assertEqual(fd.uri, (Path(__file__).parent / 'data' / 'file_1.csv').resolve().as_uri())

    @make_context
    async def test_build_file_description_directory(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data')

        self.assertEqual(fd.name, 'data')
        self.assertEqual(fd.path, (Path(__file__).parent).resolve())
        self.assertEqual(fd.full_path, (Path(__file__).parent / 'data').resolve())
        self.assertEqual(fd.type, FileDescription.ObjectType.DIRECTORY)
        self.assertEqual(fd.protocol, 'file')
        self.assertEqual(fd.metadata, {})
        self.assertEqual(fd.size, 0)
        self.assertEqual(fd.uri, (Path(__file__).parent / 'data').resolve().as_uri())

    @make_context
    async def test_build_file_description_root_directory(self):
        fd = self.manager.build_file_description(Path('/'))

        self.assertEqual(fd.name, '')
        self.assertIsNone(fd.path)
        self.assertEqual(fd.full_path, Path('/'))
        self.assertEqual(fd.type, FileDescription.ObjectType.DIRECTORY)
        self.assertEqual(fd.protocol, 'file')
        self.assertEqual(fd.metadata, {})
        self.assertEqual(fd.size, 0)
        self.assertEqual(fd.uri, Path('/').resolve().as_uri())


class ExistsTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'file_1.csv')

        self.assertTrue(await self.manager.exists(fd))

    @make_context
    async def test_fail(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'not_exists.csv')

        self.assertFalse(await self.manager.exists(fd))


class IsReadableTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'file_1.csv')

        self.assertTrue(await self.manager.is_readable(fd))

    @make_context
    async def test_fail_not_exists(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'not_exists.csv')

        self.assertFalse(await self.manager.is_readable(fd))


class IsWritableTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success_exists(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'file_1.csv')

        self.assertTrue(await self.manager.is_writable(fd))

    @make_context
    async def test_success_not_exists(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'not_exists.csv')

        self.assertTrue(await self.manager.is_writable(fd))


class ListContentTestCase(BaseLocalFsManagerTestCase):

    async def test_success_directory(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data')

        async with self.manager:
            stream = await self.manager.list_content(fd)
            result = [f.full_path async for f in stream]

        self.assertEqual(sorted(result),
                         sorted([(Path(__file__).parent / 'data' / 'file_1.csv').resolve(),
                                 (Path(__file__).parent / 'data' / 'file_2.csv').resolve()]))

    async def test_success_file(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'file_1.csv')

        async with self.manager:
            stream = await self.manager.list_content(fd)
            result = [f.full_path async for f in stream]

        self.assertEqual(result,
                         [(Path(__file__).parent / 'data' / 'file_1.csv').resolve()])

    async def test_fail_not_exists(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data_not_exists')

        async with self.manager:
            with self.assertRaises(PyRillFSFileNotFoundError):
                await self.manager.list_content(fd)

    async def test_success_directory_recursive(self):
        base_path = (Path(__file__).parent / 'data_tree').resolve()
        fd = self.manager.build_file_description(base_path)

        async with self.manager:
            stream = await self.manager.list_content(fd, recursive=True)
            result = [f.full_path async for f in stream]

            base_path / 'file_1.csv',
            base_path / 'file_2.csv',
            base_path / 'inner_dir_1' / 'file_1.csv',
            base_path / 'inner_dir_1' / 'file_2.csv',
            base_path / 'inner_dir_1',
            base_path / 'inner_dir_2' / 'file_1.csv',
            base_path / 'inner_dir_2' / 'file_2.csv'
            base_path / 'inner_dir_2' / 'inner_dir_3'
            base_path / 'inner_dir_2'
        self.assertEqual(sorted(result),
                         sorted([base_path / 'file_1.csv',
                                 base_path / 'file_2.csv',
                                 base_path / 'inner_dir_1' / 'file_1.csv',
                                 base_path / 'inner_dir_1' / 'file_2.csv',
                                 base_path / 'inner_dir_1',
                                 base_path / 'inner_dir_2' / 'file_1.csv',
                                 base_path / 'inner_dir_2' / 'file_2.csv',
                                 base_path / 'inner_dir_2' / 'inner_dir_3' / 'file_1.csv',
                                 base_path / 'inner_dir_2' / 'inner_dir_3' / 'file_2.csv',
                                 base_path / 'inner_dir_2' / 'inner_dir_3',
                                 base_path / 'inner_dir_2']),
                         result)

    async def test_success_file_recursive(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data' / 'file_1.csv')

        async with self.manager:
            stream = await self.manager.list_content(fd, recursive=True)
            result = [f.full_path async for f in stream]

        self.assertEqual(result,
                         [(Path(__file__).parent / 'data' / 'file_1.csv').resolve()])


class ReadFileTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        path = Path(__file__).parent / 'data' / 'file_1.csv'
        fd = self.manager.build_file_description(path)

        sink = await self.manager.read_file(fd) >> SumAcc() >> Last()

        result = await sink.get_frame()

        self.assertEqual(result, path.read_bytes())

    @make_context
    async def test_fail_directory(self):
        path = Path(__file__).parent / 'data'
        fd = self.manager.build_file_description(path)

        with self.assertRaises(FileNotReadableError):
            await self.manager.read_file(fd)

    @make_context
    async def test_fail_file_not_exists(self):
        path = Path(__file__).parent / 'data' / 'not_exists.csv'
        fd = self.manager.build_file_description(path)

        with self.assertRaises(PyRillFSFileNotFoundError):
            await self.manager.read_file(fd)


class WriteFileTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        path = Path(mkstemp()[1])
        try:
            fd = self.manager.build_file_description(path)

            await self.manager.write_file(fd, SyncSource(source=[b'test_1\n', b'test_2']))

            self.assertEqual(b'test_1\ntest_2', path.read_bytes())
        finally:
            path.unlink(missing_ok=True)

    @make_context
    async def test_fail_directory(self):
        path = Path(mkdtemp())
        try:
            fd = self.manager.build_file_description(path)

            with self.assertRaises(NotFileError):
                await self.manager.write_file(fd, SyncSource(source=[b'test_1\n', b'test_2']))

        finally:
            path.rmdir()


class WriteAllFilesTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        path = Path(mkdtemp())
        files = [
            (path / f'file_{i}', [f'test_{i}_1\n'.encode(),
                                  f'test_{i}_2'.encode()])
            for i in range(10)
        ]
        try:

            await self.manager.write_all_files_stream(
                SyncSource(source=[(self.manager.build_file_description(p), SyncSource(source=d)) for p, d in files])
            )

            for p, data in files:
                self.assertTrue(p.exists())
                self.assertEqual(p.read_bytes(), b''.join(data), p)

        finally:
            import shutil

            shutil.rmtree(path, ignore_errors=True)


class RemoveFileTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success_file(self):
        path = Path(mkstemp()[1])
        path.touch(exist_ok=True)
        try:
            fd = self.manager.build_file_description(path)

            await self.manager.remove_file(fd)

            self.assertFalse(path.exists())
        finally:
            path.unlink(missing_ok=True)

    @make_context
    async def test_success_directory(self):
        path = Path(mkdtemp())
        path.mkdir(exist_ok=True)
        try:
            fd = self.manager.build_file_description(path)

            await self.manager.remove_file(fd)

            self.assertFalse(path.exists())

        finally:
            try:
                path.rmdir()
            except FileNotFoundError:
                pass


class DeleteAllFilesTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        path = Path(mkdtemp())
        files = [
            path / f'file_{i}'
            for i in range(10)
        ]
        try:
            [f.touch() for f in files]
            await self.manager.remove_all_files_stream(
                SyncSource(source=[(self.manager.build_file_description(p)) for p in files])
            )

            for p in files:
                self.assertFalse(p.exists())

        finally:
            import shutil

            shutil.rmtree(path, ignore_errors=True)


class ReadFilesTestCase(BaseLocalFsManagerTestCase):

    @make_context
    async def test_success(self):
        fd = self.manager.build_file_description(Path(__file__).parent / 'data')

        stage = await self.manager.list_content(fd) >> ReadFilesStage(manager=self.manager)
        async with self.manager:
            result = [f async for f in stage]

        self.assertEqual(sorted([f.full_path for f, _ in result]),
                         sorted([(Path(__file__).parent / 'data' / 'file_1.csv').resolve(),
                                 (Path(__file__).parent / 'data' / 'file_2.csv').resolve()]))

        for f, s in result:
            self.assertIsInstance(s, BaseProducer, f)
