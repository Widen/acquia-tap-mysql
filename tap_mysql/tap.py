"""MySQL tap class."""

from __future__ import annotations

from typing import final

from singer_sdk import SQLTap, Stream
from singer_sdk import typing as th

from tap_mysql.client import CustomMySQLStream, MySQLStream


class TapMySQL(SQLTap):
    """MySQL tap class."""

    name = "tap-mysql"
    default_stream_class = MySQLStream
    custom_stream_class = CustomMySQLStream

    custom_stream_config = th.PropertiesList(
        th.Property(
            "name",
            th.StringType,
            required=False,
            description="The name of the custom stream",
        ),
        th.Property(
            "db_schemas",
            th.ArrayType(th.StringType),
            required=False,
            description="An array of schema names of the MySQL instance that is being "
            "queried. The same query will be run against each schema.",
        ),
        th.Property(
            "sql",
            th.StringType,
            required=False,
            description="The custom sql query to use for this stream. If provided, the "
            "string `{db_schema}` will be replaced with the schema name(s) "
            "from the `db_schemas` property.}`",
        ),
        th.Property(
            "primary_keys",
            th.ArrayType(th.StringType),
            default=[],
            required=False,
            description="The primary keys of the custom stream.",
        ),
    )

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
        th.Property(
            "custom_streams",
            th.ArrayType(custom_stream_config),
            required=False,
            description="An array of customized streams to use.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: list[Stream] = []
        custom_configs = self.config.get("custom_streams")
        custom_stream_names = []
        for stream in custom_configs:
            for db_schema in stream.get("db_schemas"):
                custom_stream_names.append(f"{db_schema}-{stream['name']}")

        for catalog_entry in self.catalog_dict["streams"]:
            stream_id = catalog_entry["tap_stream_id"]
            # if it's a custom stream treat it differently
            if stream_id in custom_stream_names:
                for stream in custom_configs:
                    for db_schema in stream.get("db_schemas"):
                        if stream_id == f"{db_schema}-{stream['name']}":
                            result.append(
                                self.custom_stream_class(
                                    self, catalog_entry, stream, db_schema
                                )
                            )
            else:
                result.append(self.default_stream_class(self, catalog_entry))

        return result

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
