class CoreTelecomsException(Exception):
    """ Base class for all CoreTelecoms exceptions"""


class DataIngestionError(CoreTelecomsException):
    """Critical data ingestion failure - pipeline must stop"""
    pass

class DataQualityWarning(CoreTelecomsException):
    """Data quality issue - pipeline can continue with logging"""
    pass




