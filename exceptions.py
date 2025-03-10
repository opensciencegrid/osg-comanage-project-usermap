""" Exceptions used in configuration script """


class Error(Exception):
    """Base exception class for all exceptions defined"""
    pass


class URLRequestError(Error):
    """Class for exceptions due to not being able to fulfill a URLRequest"""
    pass