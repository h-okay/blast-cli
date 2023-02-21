package pipeline

import (
	"bufio"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const configMarker = "@blast."

var commentMarkers = map[string]string{
	".sql": "--",
	".py":  "#",
}

func CreateTaskFromFileComments(fs afero.Fs) TaskCreator {
	return func(filePath string) (*Task, error) {
		extension := filepath.Ext(filePath)
		commentMarker, ok := commentMarkers[extension]
		if !ok {
			return nil, nil
		}

		file, err := fs.Open(filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open file %s", filePath)
		}
		defer file.Close()

		var allRows []string
		var commentRows []string
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			rowText := scanner.Text()
			allRows = append(allRows, rowText)

			if !strings.HasPrefix(rowText, commentMarker) {
				continue
			}

			commentValue := strings.TrimSpace(strings.TrimPrefix(rowText, commentMarker))
			if strings.HasPrefix(commentValue, configMarker) {
				commentRows = append(commentRows, strings.TrimPrefix(commentValue, configMarker))
			}
		}

		if err := scanner.Err(); err != nil {
			return nil, errors.Wrapf(err, "failed to read file %s", filePath)
		}

		if len(commentRows) == 0 {
			return nil, nil
		}

		absFilePath, err := filepath.Abs(filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get absolute path for file %s", filePath)
		}

		task := commentRowsToTask(commentRows)
		task.ExecutableFile = ExecutableFile{
			Name:    filepath.Base(filePath),
			Path:    absFilePath,
			Content: strings.Join(allRows, "\n"),
		}

		return task, nil
	}
}

func commentRowsToTask(commentRows []string) *Task {
	task := Task{
		Parameters:  make(map[string]string),
		Connections: make(map[string]string),
		DependsOn:   []string{},
		Schedule:    TaskSchedule{},
	}
	for _, row := range commentRows {
		key, value, found := strings.Cut(row, ":")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "name":
			task.Name = value

			continue
		case "description":
			task.Description = value

			continue
		case "type":
			task.Type = value

			continue
		case "depends":
			values := strings.Split(value, ",")
			for _, v := range values {
				task.DependsOn = append(task.DependsOn, strings.TrimSpace(v))
			}

			continue
		}

		if strings.HasPrefix(key, "parameters.") {
			parameters := strings.Split(key, ".")
			if len(parameters) != 2 {
				continue
			}

			task.Parameters[parameters[1]] = value
			continue
		}

		if strings.HasPrefix(key, "connections.") {
			connections := strings.Split(key, ".")
			if len(connections) != 2 {
				continue
			}

			task.Connections[connections[1]] = value
		}

		if strings.HasPrefix(key, "schedule.") {
			schedule := strings.Split(key, ".")
			if len(schedule) != 2 {
				continue
			}

			valueArray := strings.Split(value, ",")
			for _, a := range valueArray {
				task.Schedule.Days = append(task.Schedule.Days, strings.TrimSpace(a))
			}
		}

		if strings.HasPrefix(key, "materialization.") {
			materializationKeys := strings.Split(key, ".")
			if len(materializationKeys) != 2 {
				continue
			}

			materializationConfigKey := strings.ToLower(materializationKeys[1])
			switch materializationConfigKey {
			case "type":
				task.Materialization.Type = MaterializationType(strings.ToLower(value))
				continue
			case "strategy":
				task.Materialization.Strategy = MaterializationStrategy(strings.ToLower(value))
				continue
			case "partition_by":
				task.Materialization.PartitionBy = value
				continue
			case "incremental_key":
				task.Materialization.IncrementalKey = value
				continue
			case "cluster_by":
				values := strings.Split(value, ",")
				for _, v := range values {
					task.Materialization.ClusterBy = append(task.Materialization.ClusterBy, strings.TrimSpace(v))
				}
				continue
			}
		}
	}

	return &task
}
