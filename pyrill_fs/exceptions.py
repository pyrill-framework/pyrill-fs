__all__ = ['BaseFSError', 'FileNotFoundError', 'InvalidSchemeError', 'FileNotReadableError',
           'FileNotWritableError', 'NotFileError']


class BaseFSError(Exception):
    pass


class FileNotFoundError(BaseFSError):
    pass


class InvalidSchemeError(BaseFSError):
    pass


class FileNotWritableError(BaseFSError):
    pass


class FileNotReadableError(BaseFSError):
    pass


class NotFileError(BaseFSError):
    pass
