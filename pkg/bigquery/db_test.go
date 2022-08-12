package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/datablast-analytics/blast-cli/pkg/query"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestDB_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		query      string
		response   any
		statusCode int
		want       bool
		err        error
	}{
		{
			name:       "bad request",
			query:      "select * from users",
			response:   &bigquery2.Job{},
			statusCode: http.StatusBadRequest,
			err: &googleapi.Error{
				Code: 400,
				Body: "{}",
			},
		},
		{
			name:  "some validation errors returned",
			query: "select * from users",
			response: &bigquery2.Job{
				JobReference: &bigquery2.JobReference{
					JobId: "job-id",
				},
				Status: &bigquery2.JobStatus{
					ErrorResult: &bigquery2.ErrorProto{
						DebugInfo: "Some debug info",
						Location:  "some location",
						Message:   "some message",
						Reason:    "some reason",
					},
					State:           "DONE",
					ForceSendFields: nil,
					NullFields:      nil,
				},
			},
			statusCode: http.StatusOK,
			err: &bigquery.Error{
				Location: "some location",
				Message:  "some message",
				Reason:   "some reason",
			},
		},
		{
			name:  "Google API returns 404",
			query: "select * from users",
			response: map[string]interface{}{
				"error": googleapi.Error{
					Code:    404,
					Message: "not found: Table project:schema.table was not found in location ABC",
					Errors: []googleapi.ErrorItem{
						{
							Reason:  "notFound",
							Message: "not found: Table project:schema.table was not found in location ABC",
						},
					},
				},
			},
			statusCode: http.StatusNotFound,
			err:        errors.New("not found: Table project:schema.table was not found in location ABC"),
		},
		{
			name:  "no error returned",
			query: "select * from users",
			response: &bigquery2.Job{
				JobReference: &bigquery2.JobReference{
					JobId: "job-id",
				},
				Status: &bigquery2.JobStatus{
					State:  "DONE",
					Errors: nil,
				},
			},
			statusCode: http.StatusOK,
			want:       true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response, err := json.Marshal(tt.response)
				assert.NoError(t, err)

				w.WriteHeader(tt.statusCode)
				_, err = w.Write(response)
				assert.NoError(t, err)
			}))
			defer server.Close()

			client, err := bigquery.NewClient(
				context.Background(),
				"some-project-id",
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: "some-project-id",
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
						AccessToken: "some-token",
					}),
				}),
			)
			assert.NoError(t, err)
			client.Location = "US"

			d := DB{client: client}

			got, err := d.IsValid(context.Background(), &query.Query{Query: tt.query})
			if tt.err == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.err.Error())
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDB_RunQueryWithoutResult(t *testing.T) {
	t.Parallel()

	projectID := "test-project"
	jobID := "test-job"

	type jobSubmitResponse struct {
		response   any
		statusCode int
	}

	type queryResultResponse struct {
		response   *bigquery2.GetQueryResultsResponse
		statusCode int
	}

	tests := []struct {
		name                string
		query               string
		jobSubmitResponse   jobSubmitResponse
		queryResultResponse queryResultResponse
		err                 error
	}{
		{
			name:  "bad request",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response:   &bigquery2.Job{},
				statusCode: http.StatusBadRequest,
			},
			err: &googleapi.Error{
				Code: 400,
				Body: "{}",
			},
		},
		{
			name:  "Google API returns 404",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: map[string]interface{}{
					"error": googleapi.Error{
						Code:    404,
						Message: "not found: Table project:schema.table was not found in location ABC",
						Errors: []googleapi.ErrorItem{
							{
								Reason:  "notFound",
								Message: "not found: Table project:schema.table was not found in location ABC",
							},
						},
					},
				},
				statusCode: http.StatusNotFound,
			},
			err: errors.New("not found: Table project:schema.table was not found in location ABC"),
		},
		{
			name:  "no error returned",
			query: "select * from users",
			jobSubmitResponse: jobSubmitResponse{
				response: &bigquery2.Job{
					Configuration: &bigquery2.JobConfiguration{
						Query: &bigquery2.JobConfigurationQuery{
							Query: "select * from users",
							DestinationTable: &bigquery2.TableReference{
								ProjectId: projectID,
								DatasetId: "test-dataset",
							},
						},
					},
					JobReference: &bigquery2.JobReference{
						JobId:     jobID,
						ProjectId: projectID,
					},
					Status: &bigquery2.JobStatus{
						State:  "DONE",
						Errors: nil,
					},
				},
				statusCode: http.StatusOK,
			},
			queryResultResponse: queryResultResponse{
				response: &bigquery2.GetQueryResultsResponse{
					JobReference: &bigquery2.JobReference{
						JobId: "job-id",
					},
					JobComplete: true,
				},
				statusCode: http.StatusOK,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" && strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/queries/%s?", projectID, jobID)) {
					w.WriteHeader(tt.queryResultResponse.statusCode)

					response, err := json.Marshal(tt.queryResultResponse.response)
					assert.NoError(t, err)

					_, err = w.Write(response)
					assert.NoError(t, err)
					return
				} else if r.Method == "POST" && strings.HasPrefix(r.RequestURI, fmt.Sprintf("/projects/%s/jobs", projectID)) {
					w.WriteHeader(tt.jobSubmitResponse.statusCode)

					response, err := json.Marshal(tt.jobSubmitResponse.response)
					assert.NoError(t, err)

					_, err = w.Write(response)
					assert.NoError(t, err)
					return
				}

				w.WriteHeader(http.StatusInternalServerError)
				_, err := w.Write([]byte("there is no test definition found for the given request"))
				assert.NoError(t, err)
			}))
			defer server.Close()

			client, err := bigquery.NewClient(
				context.Background(),
				projectID,
				option.WithEndpoint(server.URL),
				option.WithCredentials(&google.Credentials{
					ProjectID: projectID,
					TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
						AccessToken: "some-token",
					}),
				}),
			)
			assert.NoError(t, err)
			client.Location = "US"

			d := DB{client: client}

			err = d.RunQueryWithoutResult(context.Background(), &query.Query{Query: tt.query})
			if tt.err == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.err.Error())
			}
		})
	}
}
