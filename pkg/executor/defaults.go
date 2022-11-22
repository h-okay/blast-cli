package executor

const (
	TaskTypePython         = "python"
	TaskTypeSnowflakeQuery = "sf.sql"
	TaskTypeBigqueryQuery  = "bq.sql"
)

// DefaultExecutors are set to NoOp executors by default. It serves both as a safe default and a list of supported task types.
var DefaultExecutors = map[string]Operator{
	TaskTypeBigqueryQuery:                  NoOpOperator{},
	"bq.sensor.table":                      NoOpOperator{},
	"bq.sensor.query":                      NoOpOperator{},
	"bq.cost_tracker":                      NoOpOperator{},
	"bash":                                 NoOpOperator{},
	"bq.transfer":                          NoOpOperator{},
	"bq.sensor.partition":                  NoOpOperator{},
	"gcs.from.s3":                          NoOpOperator{},
	"gcs.sensor.object_sensor_with_prefix": NoOpOperator{},
	"gcs.sensor.object":                    NoOpOperator{},
	"empty":                                NoOpOperator{},
	"athena.sql":                           NoOpOperator{},
	"athena.sensor.query":                  NoOpOperator{},
	TaskTypePython:                         NoOpOperator{},
	"python.beta":                          NoOpOperator{},
	"python.legacy":                        NoOpOperator{},
	"s3.sensor.key_sensor":                 NoOpOperator{},
	TaskTypeSnowflakeQuery:                 NoOpOperator{},
	"adjust.export.bq":                     NoOpOperator{},
}
