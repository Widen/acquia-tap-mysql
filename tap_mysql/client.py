"""SQL client handling.

This includes MySQLStream and MySQLConnector.
"""

from __future__ import annotations

import random
from typing import cast, Iterable, Optional, Tuple, Dict, List

import sqlalchemy
from singer_sdk import SQLConnector
from singer_sdk import typing as th
from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy import text
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
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

        # Parse data into useable python objects
        # todo: improve speed to match pipelinewise's variant
        instance_schema = {}
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

        Replaces `discover_catalog_entry` from base SQLConnector class.

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
        replication_method = next(iter(["LOG_BASED", "INCREMENTAL", "FULL_TABLE"]))

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

    @staticmethod
    def get_min_log_positions(tap_stream_id, state) -> Dict[str, Dict]:
        min_log_positions = {}

        for _, bookmark in state.get('bookmarks', {}).items():
            file = bookmark.get('log_file')
            position = bookmark.get('log_pos')

            if not min_log_positions.get(file):
                min_log_positions[file] = {
                    'log_pos': position,
                    'streams': [tap_stream_id]
                }

            elif min_log_positions[file]['log_pos'] > position:
                min_log_positions[file]['log_pos'] = position
                min_log_positions[file]['streams'].append(tap_stream_id)

            else:
                min_log_positions[file]['streams'].append(tap_stream_id)

        return min_log_positions

    def binlog_position(self, tap_stream_id: str, state: dict) -> Tuple[str, int, int]:
        """Return the starting position of the log file and position.

        Returns:
            A tuple of the log file name and position respectively.
        """
        min_log_positions = self.get_min_log_positions(tap_stream_id, state)

        with self._engine.connect() as connection:
            results = connection.execute("SHOW BINARY LOGS").fetchall()

        if results is None:
            raise Exception("MySQL binary logging is not enabled.")
        else:
            results_dict = {log[0]: log[1] for log in results}
            server_logs_set = set(results_dict.keys())
            state_logs_set = set(min_log_positions.keys())
            expired_logs = state_logs_set.difference(server_logs_set)

            if expired_logs:
                raise Exception(f"Unable to replicate bin log stream because the "
                                f"following binary log(s) no longer exist: "
                                f"{', '.join(expired_logs)}")

            for log_file in sorted(server_logs_set):
                end_pos: int = results_dict[log_file]
                if min_log_positions.get(log_file):
                    return log_file, min_log_positions[log_file]['log_pos'], end_pos
                elif log_file:
                    return log_file, 19, end_pos
                else:
                    raise Exception(
                        "Unable to replicate binlog stream because no binary logs exist "
                        "on the server."
                    )

    def create_binlog_reader(
        self,
        mysql_schema: Optional[str] = None,
        tap_stream_id: Optional[List[str]] = None,
        state: dict = None,
    ) -> BinLogStreamReader:
        """Yield binlog events.

        Returns:
            Iterable of binlog events
        """
        # Get starting binlog file and position
        file, start_position, end_position = self.binlog_position(tap_stream_id, state)
        if not file:
            raise ValueError(f"Log file is invalid: '{file}'")
        if not start_position or start_position < 0:
            raise ValueError(f"Log position ('pos') is invalid: '{start_position}'")

        # Get binlog files
        settings = self.config.copy()
        settings["port"] = int(settings["port"])

        return BinLogStreamReader(
            connection_settings=settings,
            server_id=random.randint(1, 2 ^ 32),
            report_slave="meltano",
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent],
            only_schemas=mysql_schema,
            log_file=file,
            log_pos=start_position,
            resume_stream=True,
        )
