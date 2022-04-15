package bigquery

type Config struct {
	ProjectID           string `envconfig:"BIGQUERY_PROJECT"`
	CredentialsFilePath string `envconfig:"BIGQUERY_CREDENTIALS"`
	Location            string `envconfig:"BIGQUERY_LOCATION"`
}
