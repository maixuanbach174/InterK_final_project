# --- Standard PEP 249 Exceptions ---

import traceback


# Base Exception classes (as provided by user, inheriting from Python's Exception)
class Warning(Exception):
    """Exception raised for important warnings like data truncations while inserting, etc."""

    pass


class Error(Exception):
    """Base class for all other exceptions."""

    def __str__(self):
        # Lấy frame cuối cùng từ traceback
        tb = traceback.extract_tb(self.__traceback__)[-1]
        filename = tb.filename
        lineno = tb.lineno
        line = tb.line
        name = tb.name  # Tên function/class

        # Format theo chuẩn Python
        return (
            f'File "{filename}", line {lineno}, in {name}\n'
            f"  {line}\n"
            f"{self.__class__.__name__}: {', '.join(map(str, self.args))}"
        )


# Subclasses of Error


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database interface
    rather than the database itself. (e.g., misuse of the DB-API, driver bugs)
    """

    pass


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""

    pass


# Subclasses of DatabaseError


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range, etc.
    """

    pass


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's operation
    and not necessarily under the programmer's control, e.g. an unexpected
    disconnect occurs, the data source name is not found, a transaction
    could not be processed, a memory allocation error occurred during
    processing, etc.
    """

    pass


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected,
    e.g. a foreign key check fails, duplicate key, etc.
    """

    pass


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    This may indicate a bug in the database itself or the driver.
    """

    pass


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors, e.g. table not found or
    already exists, syntax error in the SQL statement, wrong number of
    parameters specified, etc. These typically indicate an error in the
    application's code.
    """

    pass


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which is
    not supported by the database or driver, e.g. requesting a .rollback() on a
    connection that does not support transaction or has transactions turned off.
    """

    pass


class AuthenticationError(DatabaseError):
    """
    Exception raised when token validation error occurs, like the user is not authenticated,
    or the refresh endpoint is not online.
    """

    pass
