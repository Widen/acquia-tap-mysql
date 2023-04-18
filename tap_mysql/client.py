"""SQL client handling.

This includes MySQLStream and MySQLConnector.
"""

from __future__ import annotations

from typing import cast

import sqlalchemy
from singer_sdk import SQLConnector, SQLStream
from singer_sdk import typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy import text


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
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
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

    def get_instance_schema(self) -> dict:
        """Return list of data necessary to construct a complete catalog.

        Returns:
            dict
        """
        with self._engine.connect() as connection:
            query = text(
                """
                SELECT c.table_schema,
                       c.table_name,
                       t.table_type,
                       c.column_name,
                       c.data_type,
                       c.is_nullable,
                       c.column_key
                FROM information_schema.columns AS c
                    LEFT JOIN information_schema.tables AS t
                        ON c.table_name = t.table_name
                WHERE c.table_schema NOT IN (
                        'information_schema'
                        , 'performance_schema'
                        , 'mysql'
                        , 'sys'
                    )
                ORDER BY table_schema, table_name, column_name
                -- LIMIT 40
            """
            )
            result = connection.execute(query).fetchall()
            instance_schema = {}

            # Parse data into useable python objects
            for row in result:
                db_schema = row[0]
                table = row[1]
                column_def = {
                    "name": row[3],
                    "type": row[4],
                    "nullable": row[5] == "YES",
                    "key_type": row[6],
                }
                table_def = {"table_type": row[2], "columns": [column_def]}
                if db_schema not in instance_schema:
                    instance_schema[db_schema] = {table: table_def}
                elif table not in instance_schema[db_schema]:
                    instance_schema[db_schema][table] = table_def
                else:
                    instance_schema[db_schema][table]["columns"].append(column_def)

        return instance_schema

    def create_catalog_entry(
        self, db_schema_name: str, table_name: str, table_def: dict
    ) -> CatalogEntry:
        """Create `CatalogEntry` object for the given table or a view.

        Args:
            db_schema_name: Name of the MySQL Schema being cataloged.
            table_name: Name of the MySQL Table being cataloged.
            table_def: A dict defining relevant elements of the table.

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
        for column_def in table_def["columns"]:
            column_name = column_def["name"]
            key_type = column_def["key_type"]
            is_nullable = column_def["nullable"]

            if key_type == "PRI":
                primary_keys.append(column_name)

            # Initialize columns list
            jsonschema_type: dict = self.to_jsonschema_type(
                cast(sqlalchemy.types.TypeEngine, column_def["type"]),
            )
            table_schema.append(
                th.Property(
                    name=column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not is_nullable,
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
            is_view=table_def["table_type"] == "VIEW",
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
        result: list[dict] = []
        instance_schema = self.get_instance_schema()

        for schema_name, schema_def in instance_schema.items():
            # Iterate through each tables and views
            for table_name, table_def in schema_def.items():
                entry = self.create_catalog_entry(schema_name, table_name, table_def)
                result.append(entry.to_dict())

        return result


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
