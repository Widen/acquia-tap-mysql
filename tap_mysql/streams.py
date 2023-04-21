from __future__ import annotations

from datetime import datetime
from typing import Iterable, Any, Dict, Optional

import sqlalchemy
from singer_sdk import SQLStream
from tap_mysql.client import MySQLConnector
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

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

    def update_binlog_bookmarks(
            self,
            stream_state: Dict,
            file: str,
            position: int,
    ) -> Dict:
        """Update the state bookmarks with the given binlog file & position or GTID

        Args:
            stream_state: state to update
            file: new binlog file
            position: new binlog pos
            gtid: new gtid pos

        Returns:
            updated state
        """
        self.logger.debug(
            f"Updating state bookmark to binlog file and position: "
            f"{file}, {position}"
        )

        if file and not position:
            raise ValueError(
                "Binlog file name is present but binlog position is null! "
                "Please provide a binlog position to properly update the state."
            )

        stream_state["log_file"] = file
        stream_state["log_pos"] = position

        return stream_state

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
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
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning.",
            )

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )

        if self.replication_method in ["FULL_TABLE", "INCREMENTAL"]:
            query = table.select()

            if self.replication_key:
                replication_key_col = table.columns[self.replication_key]
                query = query.order_by(replication_key_col)

                start_val = self.get_starting_replication_key_value(context)
                if start_val:
                    query = query.where(
                        sqlalchemy.text(":replication_key >= :start_val").bindparams(
                            replication_key=replication_key_col,
                            start_val=start_val,
                        ),
                    )

            if self.ABORT_AT_RECORD_COUNT is not None:
                # Limit record count to one greater than the abort threshold. This ensures
                # `MaxRecordsLimitException` exception is properly raised by caller
                # `Stream._sync_records()` if more records are available than can be
                # processed.
                query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

            with self.connector._connect() as conn:
                for record in conn.execute(query):
                    yield dict(record._mapping)

        elif self.replication_method == "LOG_BASED":
            # binlog_streams = [stream["tap_stream_id"] for stream in self.ca]
            binlog_events = None
            try:
                binlog_events = self.connector.create_binlog_reader(
                    mysql_schema=self.metadata.get((), None).schema_name,
                    tap_stream_id=self.tap_stream_id,
                    state=self.stream_state,
                )
                for binlog_event in binlog_events:
                    if isinstance(binlog_event, RotateEvent):
                        self.logger.debug(
                            f"RotateEvent = "
                            f"file:{binlog_event.next_binlog}, "
                            f"position={binlog_event.position}"
                        )

                        self.update_binlog_bookmarks(
                            self.stream_state,
                            binlog_event.next_binlog,
                            binlog_event.position,
                        )
                        binlog_event.dump()

                    elif isinstance(binlog_event, WriteRowsEvent):
                        for row in binlog_event.rows:
                            yield {k: v for k, v in row['values'].items()}

                    elif isinstance(binlog_event, UpdateRowsEvent):
                        for row in binlog_event.rows:
                            yield {k: v for k, v in row['after_values'].items()}

                    elif isinstance(binlog_event, DeleteRowsEvent):
                        for row in binlog_event.rows:
                            vals = row['values']
                            vals["_sdc_deleted_at"] = datetime.utcnow().isoformat()
                            yield {k: v for k, v in vals.items()}

                    else:
                        self.logger.debug(
                            f"Skipping event for stream {self.fully_qualified_name}. "
                            f"It is not an INSERT, UPDATE, or DELETE",
                        )

                    # Update bookmark to the last processed binlog event
                    self.update_binlog_bookmarks(
                        self.stream_state,
                        binlog_event.next_binlog,
                        binlog_event.position,
                    )

            # BinLogStreamReader doesn't implement the `with` methods
            finally:
                if binlog_events:
                    binlog_events.close()

