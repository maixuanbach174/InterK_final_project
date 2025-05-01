from typing import Optional, List, Any

from dbcsv.app.core.dbapi2.utils import validate_dsn_url, login, query, validate_token
from dbcsv.app.core.dbapi2.exception import InternalError, NotSupportedError


class Connection:
    def __init__(self, token: str):
        self.token = token
        self._url = None
        self._is_online = True
        self._schema = None

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    @property
    def is_online(self):
        return self._is_online

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    def cursor(self) -> "Cursor":
        if not self._is_online:
            raise InternalError("Cannot create any cursor from a closed connection")
        return Cursor(self)

    def rollback(self):
        if not self._is_online:
            raise InternalError("Connection is closed.")
        raise NotSupportedError("rollback() is currently not supported")

    def commit(self):
        if not self._is_online:
            raise InternalError("Connection is closed.")
        pass

    def close(self):
        if not self._is_online:
            raise InternalError("Connection is already closed")
        self._is_online = False


class Cursor:
    def __init__(self, connection: Connection):
        self.arraysize = 1
        self._connection = connection
        self._description = None
        self._rowcount = -1
        self._is_open = True

        # Buffer
        self._results = []

    @property
    def description(self):
        return self._description

    @property
    def is_open(self):
        return self._is_open

    def close(self):
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform close() on cursor of a closed connection"
            )
        if not self._is_open:
            raise InternalError("Cursor must be opened to close()")
        # Free resources
        self._is_open = False
        self._results = None

    def execute(self, q: str, parameters=None) -> None:
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform execute() on cursor of a closed connection"
            )
        if not self._is_open:
            raise InternalError("Cursor must be opened to execute any query")

        # Validate JWT token exp date before querying
        new_token = validate_token(self._connection.url, self._connection.token)

        if new_token is not None:
            self._connection.token = new_token

        # Run query
        result = query(
            self._connection.url, self._connection.token, self._connection.schema, q
        )

        # Assign result to buffer
        self._results = result

    def fetchone(self):
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform fetchone() on cursor of a closed connection"
            )
        if not self._is_open:
            raise InternalError("Cursor must be opened to fetch any result")
        if self._results:
            return self._results[0]
        return None

    def fetchmany(self, size=None):
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform fetchmany() on cursor of a closed connection"
            )
        if not self._is_open:
            raise InternalError("Cursor must be opened to fetch any result")
        size = size or self.arraysize
        return self._results[:size]

    def fetchall(self):
        if not self._connection.is_online:
            raise InternalError(
                "Cannot perform fetchall() on cursor of a closed connection"
            )
        if not self._is_open:
            raise InternalError("Cursor must be opened to fetch any result")
        return self._results

    # Does nothing, same as sqlite3 documentation
    def setinputsizes(self, sizes: List[Any]):
        raise NotSupportedError(
            "setinputsizes() is not supported. It is implemented to comply with DBAPI2 standard"
        )

    # Does nothing, same as sqlite3 documentation
    def setoutputsize(self, sizes: List[Any], column=None):
        raise NotSupportedError(
            "setinputsizes() is not supported. It is implemented to comply with DBAPI2 standard"
        )


def connect(
    dsn: str,
    user: Optional[str],
    password: Optional[str],
) -> Connection:
    """
    Initializes a connection to the database.

    Returns a Connection Object. It takes a number of parameters which are database dependent.

    E.g. a connect could look like this: connect(dsn='https://localhost:1234/schema', user='guido', password='1234')
    """

    # Validate url correctness
    schema, url = validate_dsn_url(dsn)

    # Request to /login endpoint of dsn to get JWT token. Catches exception if user doesn't exist in database
    token = login(url, user, password)

    # Create connection
    conn = Connection(token=token)

    conn.schema = schema
    conn.url = url

    return conn
