"""SQL client handling.

This includes MySQLStream and MySQLConnector.
"""

from __future__ import annotations

import collections
import datetime
import itertools
from typing import Iterator, cast

import sqlalchemy
from singer_sdk import SQLConnector, SQLStream
from singer_sdk import typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy import text

Column = collections.namedtuple(
    "Column",
    [
        "table_schema",
        "table_name",
        "column_name",
        "column_type",
        "is_nullable",
        "column_key",
    ],
)


class MySQLConnector(SQLConnector):
    """Connects to the MySQL SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        return (
            f"mysql+pymysql://{config['user']}:"
            f"{config['password']}@"
            f"{config['host']}:"
            f"{config['port']}/"
            f"mysql"
        )

    @staticmethod
    def to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Return a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        if from_type in ["bit", "bit(1)"]:
            return {"type": ["boolean"]}
        else:
            return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)

    def create_catalog_entry(
        self,
        db_schema_name: str,
        table_name: str,
        table_def: dict,
        columns: Iterator[Column],
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            db_schema_name: Name of the MySQL Schema being cataloged.
            table_name: Name of the MySQL Table being cataloged.
            table_def: A dict defining relevant elements of the table.
            columns: list of named tuples describing the column.

        Returns:
            CatalogEntry
        """
        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=db_schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Detect column properties
        primary_keys: list[str] = []
        table_schema = th.PropertiesList()
        for col in columns:
            if col.column_key == "PRI":
                primary_keys.append(col.column_name)

            # Initialize columns list
            jsonschema_type: dict = self.to_jsonschema_type(
                cast(sqlalchemy.types.TypeEngine, col.column_type),
            )
            table_schema.append(
                th.Property(
                    name=col.column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not col.is_nullable,
                ),
            )
        schema = table_schema.to_dict()
        replication_method = self.config.get("replication_method") or "FULL_TABLE"

        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=primary_keys,
            schema=Schema.from_dict(schema),
            is_view=table_def[db_schema_name][table_name]["is_view"] == "VIEW",
            replication_method=replication_method,  # Can be defined by user
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=db_schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=primary_keys,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    # Choosing to overwrite capabilities here because the default implementation was
    # slow and inefficient.
    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        entries: list[dict] = []

        with self._engine.connect() as connection:
            table_query = text(
                """
                SELECT
                    table_schema
                    , table_name
                    , table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN (
                        'information_schema'
                        , 'performance_schema'
                        , 'mysql'
                        , 'sys'
                    )
                ORDER BY table_schema, table_name
                """
            )
            table_results = connection.execute(table_query).fetchall()
            table_defs: dict = {}

            for mysql_schema, table, table_type in table_results:
                if mysql_schema not in table_defs:
                    table_defs[mysql_schema] = {}

                table_defs[mysql_schema][table] = {"is_view": table_type == "VIEW"}

            col_query = text(
                """
                SELECT
                    table_schema
                    , table_name
                    , column_name
                    , column_type
                    , is_nullable
                    , column_key
                FROM information_schema.columns
                WHERE table_schema NOT IN (
                        'information_schema'
                        , 'performance_schema'
                        , 'mysql'
                        , 'sys'
                    )
                ORDER BY table_schema, table_name, column_name
                -- LIMIT 40
            """
            )
            col_result = connection.execute(col_query)

            # Parse data into useable python objects
            columns = []
            rec = col_result.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = col_result.fetchone()

        for k, cols in itertools.groupby(
            columns, lambda c: (c.table_schema, c.table_name)
        ):
            # cols = list(cols)
            mysql_schema, table_name = k

            entry = self.create_catalog_entry(
                db_schema_name=mysql_schema,
                table_name=table_name,
                table_def=table_defs,
                columns=cols,
            )
            entries.append(entry.to_dict())

        return entries


class MySQLStream(SQLStream):
    """Stream class for MySQL streams."""

    connector_class = MySQLConnector

    # def get_records(self, partition: dict | None) -> Iterable[dict[str, Any]]:
    #     """Return a generator of record-type dictionary objects.
    #
    #     Developers may optionally add custom logic before calling the default
    #     implementation inherited from the base class.
    #
    #     Args:
    #         partition: If provided, will read specifically from this data slice.
    #
    #     Yields:
    #         One dict per record.
    #     """
    #     # Optionally, add custom logic instead of calling the super().
    #     # This is helpful if the source database provides batch-optimized record
    #     # retrieval.
    #     # If no overrides or optimizations are needed, you may delete this method.
    #     yield from super().get_records(partition)
