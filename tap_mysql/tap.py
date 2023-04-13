"""MySQL tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th

# from tap_mysql.client import MySQLStream


class TapMySQL(SQLTap):
    """MySQL tap class."""

    name = "tap-mysql"

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


if __name__ == "__main__":
    TapMySQL.cli()
