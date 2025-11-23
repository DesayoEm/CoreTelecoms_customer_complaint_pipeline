class CoreTelecomsException(Exception):
    """Base class for all CoreTelecoms exceptions"""


class DataIngestionError(CoreTelecomsException):
    """Critical data ingestion failure - pipeline must stop"""

    pass


class DataQualityWarning(CoreTelecomsException):
    """
    Parent exception class for quality issues.
    Raised initially on trigger bur not raised down the call stack
    Pipeline can continue with logging
    """

    pass
