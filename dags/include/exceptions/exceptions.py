from include.exceptions.base_exceptions import DataIngestionError, DataQualityWarning

class UnSupportedFileFormatError(DataIngestionError):
    """Raised when file format is not supported for conversion"""
    def __init__(self, details: str):
        message = f"Unsupported file format: {details}"
        super().__init__(message)
        self.log = message


class EmptyDataFrameError(DataIngestionError):
    """Raised when parsed DataFrame contains no data"""
    def __init__(self, object_key: str):
        message = f"No data found in {object_key}. File may be empty or malformed."
        super().__init__(message)
        self.log = message


class GoogleCredentialsError(DataIngestionError):
    """Raised when parsed DataFrame contains no data"""
    def __init__(self, details: str):
        message = f"Could not retrieve Google service account credentials from AWS Secrets Manager{details}"
        super().__init__(message)
        self.log = message