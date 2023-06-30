import os

from decorator import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import SingletonThreadPool


class DB:
    def __init__(self, db_uri):
        verbose: bool = os.environ.get("SQL_VERBOSE", "false").lower() == "true"

        self.engine = create_engine(
            db_uri,
            echo=verbose,
            poolclass=SingletonThreadPool,
            pool_recycle=1800  # 30 minutes
        )
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)

    @contextmanager
    def transaction(self) -> scoped_session:
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_session(self) -> scoped_session:
        return self.Session()

    def insert_or_update(self, tablename, keys, cols, *args):
        """
        Upserts a row into the table. `keys` indicate columns that are unique
        and `cols` are the columns that are updated.
        """
        query = (
            f"INSERT INTO {tablename} VALUES {str(args)} "
            f"ON CONFLICT ({','.join(keys)}) DO UPDATE SET {','.join([f'{col}=excluded.{col}' for col in cols])}"
        )
        with self.transaction() as trx:
            trx.execute(text(query))

    def close(self):
        self.Session.remove()


class _SqlLiteDB(DB):
    def __init__(self, path=".featureform/SQLiteDB"):
        self.path = path
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        super().__init__(f"sqlite:///{self.path}/metadata.db")


def get_local_db():
    return _SqlLiteDB()
