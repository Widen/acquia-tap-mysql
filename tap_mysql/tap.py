"""MySQL tap class."""

from __future__ import annotations

from typing import final

from singer_sdk import SQLTap, Stream
from singer_sdk import typing as th

from tap_mysql.client import MySQLStream


class TapMySQL(SQLTap):
    """MySQL tap class."""

    name = "tap-mysql"
    default_stream_class = MySQLStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="The hostname of the MySQL instance.",
        ),
        th.Property(
            "port",
            th.StringType,
            default="3306",
            description="The port number of the MySQL instance.",
        ),
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The username",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,
            description="The password for the user",
        ),
    ).to_dict()

    # not supposed to do this but the logs of deselected streams are a drag
    @final
    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: Stream
        for stream in self.streams.values():
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.debug(f"Skipping deselected stream '{stream.name}'.")
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation.",
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()
            stream._write_state_message()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()


if __name__ == "__main__":
    TapMySQL.cli()
