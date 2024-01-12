# tap-mysql

`tap-mysql` is a Singer tap for MySQL.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

The primary advantage of this version of `tap-mysql` is that it emphasizes and completes
the `LOG_BASED` replication method, whereas other variants have buggy or incomplete
implementations of such. Other advantages include inheriting the capabilities of a tap
built on the Meltano Tap SDK.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install tap-mysql
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-mysql.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
This section can be created by copy-pasting the CLI output from:

```
tap-mysql --about --format=markdown
```
-->

| Setting              | Required | Default | Description                                                                                                                                 |
|:---------------------|:--------:|:-------:|:--------------------------------------------------------------------------------------------------------------------------------------------|
| host                 |   True   |  None   | The hostname of the MySQL instance.                                                                                                         |
| port                 |  False   |  3306   | The port number of the MySQL instance.                                                                                                      |
| user                 |   True   |  None   | The username                                                                                                                                |
| password             |   True   |  None   | The password for the user                                                                                                                   |
| custom_streams       |  False   |  None   | An array of customized streams to use.                                                                                                      |
| stream_maps          |  False   |  None   | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    |  False   |  None   | User-defined config values to be used within map expressions.                                                                               |
| flattening_enabled   |  False   |  None   | 'True' to enable schema flattening and automatically expand nested properties.                                                              |
| flattening_max_depth |  False   |  None   | The max depth to flatten schemas.                                                                                                           |
| batch_config         |  False   |  None   |                                                                                                                                             |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-mysql --about
```

#### `custom_stream` configuration

Custom streams are defined in the `custom_streams` configuration option. This option is
an array of objects, each of which defines a custom stream. Each custom stream object
has the following properties:

| Property     | Required | Default | Description                                                                                                                                                   |
|:-------------|:--------:|:-------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name         |   True   |  None   | The name of the custom stream.                                                                                                                                |
| db_schemas   |  False   |  None   | An array of schema names of the MySQL instance that is being queried. The same query will be run against each schema.                                         |
| sql          |   True   |  None   | The custom sql query to use for this stream. If provided, the string `{db_schema}` will be replaced with the schema name(s) from the `db_schemas` property.}` |
| primary_keys |  False   |  None   | The primary keys of the custom stream.                                                                                                                        |

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working
directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if
a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-mysql` by itself or in a pipeline
using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-mysql --version
tap-mysql --help
tap-mysql --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
poetry run pytest
```

You can also test the `tap-mysql` CLI interface directly using `poetry run`:

```bash
poetry run tap-mysql --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-mysql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-mysql --version
# OR run a test `elt` pipeline:
meltano elt tap-mysql target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more
instructions on how to use the SDK to
develop your own taps and targets.
