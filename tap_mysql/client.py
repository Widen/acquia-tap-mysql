"""SQL client handling.

This includes MySQLStream and MySQLConnector.
"""

from __future__ import annotations

import collections
import itertools
import typing as t
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

    def __init__(self, config: dict = {}):
        """Initialize MySQL connector.

        Args:
            config: A dict with connection parameters

        """
        super().__init__(config, self.get_sqlalchemy_url(config))

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
            # brake discovery into 2 queries for performance
            # Get the table definition
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

            # Get the column definitions
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
            """
            )
            col_result = connection.execute(col_query)

            # Parse data into useable python objects and append to entries
            columns = []
            rec = col_result.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = col_result.fetchone()

            for k, cols in itertools.groupby(
                columns, lambda c: (c.table_schema, c.table_name)
            ):
                mysql_schema, table_name = k

                entry = self.create_catalog_entry(
                    db_schema_name=mysql_schema,
                    table_name=table_name,
                    table_def=table_defs,
                    columns=cols,
                )
                entries.append(entry.to_dict())

            # append custom stream catalog entries
            custom_streams = self.config.get("custom_streams")
            if custom_streams:
                for stream_config in custom_streams:
                    for table_schema in stream_config.get("db_schemas"):
                        table_name = stream_config.get("name")
                        primary_keys = stream_config.get("primary_keys")

                        query = text(
                            stream_config.get("sql").replace(
                                "{db_schema}", table_schema
                            )
                        )
                        custom_result = connection.execute(query)
                        custom_rec = custom_result.fetchone()
                        # inject the table_schema into the list of columns
                        custom_rec_keys = list(custom_rec._fields) + ["mysql_schema"]

                        # note that all columns are forced to be strings to avoid
                        # the complexity of inferring their data types. Warning this
                        # could cause issues in the loss of precision of data
                        custom_columns = []
                        for col in custom_rec_keys:
                            custom_columns.append(
                                Column(
                                    table_schema=table_schema,
                                    table_name=table_name,
                                    column_name=col,
                                    column_type="STRING",
                                    is_nullable="YES",
                                    column_key="PRI" if col in primary_keys else None,
                                )
                            )

                        entry = self.create_catalog_entry(
                            db_schema_name=table_schema,
                            table_name=table_name,
                            table_def={table_schema: {table_name: {"is_view": False}}},
                            columns=iter(custom_columns),
                        )
                        entries.append(entry.to_dict())

        return entries


class MySQLStream(SQLStream):
    """Stream class for MySQL streams."""

    connector_class = MySQLConnector  # type: ignore


class CustomMySQLStream(SQLStream):
    """Custom stream class for MySQL streams."""

    connector_class = MySQLConnector  # type: ignore
    name = ""
    query: str = ""

    def __init__(
        self,
        tap,
        catalog_entry: dict,
        stream_config: dict,
        mysql_schema: str,
    ) -> None:
        """Initialize the stream.

        Args:
            tap: The tap object
            catalog_entry: The Singer Catalog entry
            stream_config: The portion of the config specific to this stream
            mysql_schema: the MySQL schema to use for the stream
        """
        super().__init__(
            tap=tap,
            catalog_entry=catalog_entry,
        )
        self.mysql_schema = mysql_schema
        self.query = stream_config["sql"].replace("{db_schema}", mysql_schema)

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        query = text(self.query)

        if self.replication_key:
            self.logger.info(
                f"A replication key was provided but will be ignored for "
                f"the custom stream '{self.tap_stream_id}'."
            )

        if self.ABORT_AT_RECORD_COUNT is not None:
            # Limit record count to one greater than the abort threshold. This ensures
            # `MaxRecordsLimitException` exception is properly raised by caller
            # `Stream._sync_records()` if more records are available than can be
            # processed.
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)  # type: ignore

        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                # TODO: Standardize record mapping type
                # https://github.com/meltano/sdk/issues/2096
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        # force all values to be strings for simplicity
        new_row = {k: str(v) for k, v in row.items()}
        # inject the mysql_schema into the record
        new_row["mysql_schema"] = self.mysql_schema
        return new_row
