<p align="center">
  <img width="450" src="./img/logo.svg">
</p>
<p align="center"> 
Transform, validate and run your data pipelines using SQL and Python.
</p>

<p align="center">
<a href="https://golang.org/"><img src="https://img.shields.io/badge/--00ADD8?logo=go&logoColor=ffffff"></a>
<a href="https://goreportcard.com/report/github.com/datablast-analytics/blast"><img src="https://goreportcard.com/badge/github.com/datablast-analytics/blast"></a>
<a href="https://img.shields.io/github/v/release/datablast-analytics/blast"><img src="https://img.shields.io/github/v/release/datablast-analytics/blast"></a>
<a href="https://slack.getblast.io"><img src="https://img.shields.io/badge/community-%23blast-green?logo=slack&labelColor=gray&color=28A745"></a>
<a href="https://github.com/datablast-analytics/blast/blob/master/LICENSE.md"><img src="https://img.shields.io/github/actions/workflow/status/datablast-analytics/blast/build-test.yml"></a>
</p>

---

Blast is a command-line tool for validating and running data transformations on SQL, similar to dbt. On top, Blast can
also run Python assets within the same pipeline.

- ✨ run SQL transformations on BigQuery/Snowflake
- 🐍 run Python in isolated environments
- 💅 built-in data quality checks
- 🚀 Jinja templating language to avoid repetition
- ✅ validate data pipelines end-to-end to catch issues early on via dry-run on live
- 📐 table/view materialization
- ➕ incremental tables
- 💻 mix different technologies + databases in a single pipeline, e.g. SQL and Python in the same pipeline

- ⚡ blazing fast pipeline execution: Blast is written in Golang and uses concurrency at every opportunity

![Blast CLI](./resources/blast.svg)

## Installation

You need to have Golang installed in the first place, then you can run the following command:

```shell
go install github.com/datablast-analytics/blast@latest
```

Please make sure to add GOPATH to your executable path.

## Getting Started

All you need is a simple `pipeline.yml` in your Git repo:

```yaml
name: blast-example
schedule: "daily"
start_date: "2023-03-01"

default_connections:
  google_cloud_platform: "gcp"
```

create a new folder called `assets` and create your first asset there `assets/blast-test.sql`:

```sql
-- @blast.name: dataset.blast_test
-- @blast.type: bq.sql
-- @blast.materialization.type: table

SELECT 1 as result
```

Blast will take this result, and will create a `dataset.blast_test` table on BigQuery. You can also use `view`
materialization type instead of `table` to create a view instead.

> **Snowflake assets**
> If you'd like to run the asset on Snowflake, simply replace the `bq.sql` with `sf.sql`, and define `snowflake` as a
> connection instead of `google_cloud_platform`.

Then let's create a Python asset `assets/hello.py`:

```python
# @blast.name: hello
# @blast.type: python
# @blast.depends: dataset.blast_test

print("Hello, world!")
```

Once you are done, run the following command to validate your pipeline:

```shell
blast validate .
```

You should get an output that looks like this:

```shell
Pipeline: blast-example (.)
  No issues found

✓ Successfully validated 2 tasks across 1 pipeline, all good.
```

If you have defined your credentials, Blast will automatically detect them and validate all of your queries using
dry-run.

## Environments

Blast allows you to run your pipelines / assets against different environments, such as development or production. The
environments are managed in the `.blast.yml` file.

The following is an example configuration that defines two environments called `default` and `production`:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      snowflake:
        - name: "snowflake"
          username: "my-user"
          password: "my-password"
          account: "my-account"
          database: "my-database"
          warehouse: "my-warehouse"
          schema: "my-dev-schema"
  production:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/prod-key.json"
          project_id: "my-project-prod"
      snowflake:
        - name: "snowflake"
          username: "my-user"
          password: "my-password"
          account: "my-account"
          database: "my-database"
          warehouse: "my-warehouse"
          schema: "my-prod-schema" 
```

You can simply switch the environment using the `--environment` flag, e.g.:

```shell
blast validate --environment production . 
```

### Running the pipeline

Blast CLI can run the whole pipeline or any task with the downstreams:

```shell
blast run .
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
blast run assets/hello.py                            
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

- Secrets for Python assets
- More databases: Postgres, Redshift, MySQL, and more

## Disclaimer

Blast is still in its early stages, so please use it with caution. We are working on improving the documentation and
adding more features.

If you are interested in a cloud data platform that does all of these & more as a managed service check
out [Blast Data Platform](https://getblast.io).

