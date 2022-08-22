# Blast CLI
This is a CLI tool that is used to interact with Blast-powered pipelines.

## Installation
```shell
go install github.com/datablast-analytics/blast-cli@latest
```

Please make sure to add GOPATH to your executable path.

## Usage
### Validating Pipelines
```shell
blast-cli validate <path to the pipelines>
```


### Running Tasks - Beta
Blast CLI can run individual tasks, mainly for BigQuery to begin with.

#### Prerequisites
You need to define two environment variables:
- `BIGQUERY_CREDENTIALS_FILE`: path to your service account credentials file
- `BIGQUERY_PROJECT`: the name of your BigQuery project

For ease of future use, you can put these in your `.bashrc` or `.zshrc` files:
```sh
export BIGQUERY_CREDENTIALS_FILE="path/to/your/service-account.json"
export BIGQUERY_PROJECT="project-name"
```

#### Running
Once you have defined the proper environment variables, you can run the individual task as follows:
```shell
blast-cli run-task <path to the task>
```


