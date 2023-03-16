# Blast CLI
[![Go](https://img.shields.io/badge/--00ADD8?logo=go&logoColor=ffffff)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/datablast-analytics/blast-cli)](https://goreportcard.com/report/github.com/datablast-analytics/blast-cli)
[![GitHub Release](https://img.shields.io/github/v/release/datablast-analytics/blast-cli)](https://img.shields.io/github/v/release/datablast-analytics/blast-cli)

Blast is a command-line tool for validating and running data transformations on SQL, similar to dbt. On top, Blast can also run Python assets within the same pipeline.

- ✨ run SQL transformations on BigQuery/Snowflake
  - run pipelines in parallel, as many workers as you want
  - run only a single asset or with all of its downstream
  - more databases coming soon: Postgres, Redshift, MySQL, and more
- 🐍 run Python in isolated virtual environments
  - every asset can have different dependencies
  - closest `requirements.txt` file is used for the asset
  - easy to test Python scripts: no changes in your regular scripts needed to run in Blast
- 🚀 Jinja templating language to avoid repetition
- ✅ validate data pipelines end to end to catch issues early on
  - for BigQuery, it can also validate the queries via dry-run to validate the queries on live environment directly.
  - for Snowflake, it validates queries via `EXPLAIN` statements.
- 📐 table/view materialization
  - BigQuery-only at the moment, Snowflake and Postgres in progress
  - Blast fills the DDL statements on top of your queries, you just write a `SELECT` query
- ➕ incremental tables
  - just focus on your business logic, the rest is handled by Blast
- 💻 mix different technologies + databases in a single pipeline
  - e.g. SQL and Python in the same pipeline
  - e.g. BigQuery and Snowflake in the same pipeline
- built-in data quality checks [coming soon]
  - ensure the generated tables/views have the correct data on a per-column basis
- ⚡ written in Golang: blazing fast

![Blast CLI](./resources/blast.svg)

## Installation
You need to have Golang installed in the first place, then you can run the following command:
```shell
go install github.com/datablast-analytics/blast-cli@latest
```

Please make sure to add GOPATH to your executable path.


## Getting Started
All you need is a simple `pipeline.yml` in your Git repo:
```yaml
name: blast-example
schedule: "daily"
start_date: "2023-03-01"
```

create a new folder called `tasks` and create your first asset there `tasks/blast-test.sql`:
```sql
-- @blast.name: dataset.blast-test
-- @blast.type: bq.sql
-- @blast.materialization.type: table

SELECT 1 as result
```

Blast will take this result, and will create a `dataset.blast-test` table on BigQuery. You can also use `view` materialization type instead of `table` to create a view instead.

> **Snowflake assets**
> If you'd like to run the asset on Snowflake, simply replace the `bq.sql` with `sf.sql`.

Then let's create a Python asset `tasks/blast-test.py`:
```python
# @blast.name: hello
# @blast.type: python
# @blast.depends: dataset.blast-test

print("Hello, world!")
```


Once you are done, run the following command to validate your pipeline:
```shell
blast-cli validate .
```

You should get an output that looks like this:
```shell
Pipeline: blast-example (.)
  No issues found

✓ Successfully validated 2 tasks across 1 pipeline, all good.
```

### Query Validation
If you'd like to validate your queries against the environment or run the pipeline, the first thing you'd need to do is to define your credentials. If you have defined the credentials, Blast will use them to connect to BigQuery or Snowflake automatically.

#### BigQuery
You need to define two environment variables:
- `BIGQUERY_CREDENTIALS_FILE`: path to your service account credentials file
- `BIGQUERY_PROJECT`: the name of your BigQuery project

For ease of future use, you can put these in your `.bashrc` or `.zshrc` files:
```sh
export BIGQUERY_CREDENTIALS_FILE="path/to/your/service-account.json"
export BIGQUERY_PROJECT="project-name"
```

#### Snowflake
You need to define two environment variables:
- `SNOWFLAKE_ACCOUNT`: Snowflake account name
- `SNOWFLAKE_USERNAME`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_REGION`: Snowflake region
- `SNOWFLAKE_ROLE`: Snowflake role to run the pipeline with
- `SNOWFLAKE_DATABASE`: The database to run the pipeline in
- `SNOWFLAKE_SCHEMA`: The database schema to run the pipeline in


### Running the pipeline
Blast CLI can also run the whole pipeline or any task with the downstreams:

```shell
blast-cli run .
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.blast-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.blast-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:
```shell
blast-cli run tasks/hello.py                            
```
```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

## Upcoming Features
- Support for full range of data quality tests on a per-column basis
- Connection + config management
- Secrets for Python tasks
- More databases: Postgres, Redshift, MySQL, and more

## Disclaimer
Blast is still in its early stages, so please use it with caution. We are working on improving the documentation and adding more features.

If you are interested in a cloud data platform that does all of these & more on a hosted platform, please check out [Blast Data Platform](https://getblast.io).

