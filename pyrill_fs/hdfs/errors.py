from .models import RemoteException


class WebHDFSError(Exception):

    def __init__(self, error: RemoteException):
        self.model = error

        super(WebHDFSError, self).__init__(error.message)

    def __str__(self):
        return f'<{self.model.exception}> {self.model.message} [{self.model.java_class_name}]'

    __repr__ = __str__


class FileNotFound(WebHDFSError):
    pass
