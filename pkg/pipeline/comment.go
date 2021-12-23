package pipeline

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const configMarker = "@blast."

var commentMarkers = map[string]string{
	".sql": "--",
	".py":  "#",
}

func CreateTaskFromFileComments(filePath string) (*Task, error) {
	extension := filepath.Ext(filePath)
	commentMarker, ok := commentMarkers[extension]
	if !ok {
		return nil, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %s", filePath, err)
	}
	defer file.Close()

	var commentRows []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rowText := scanner.Text()
		if strings.HasPrefix(rowText, commentMarker) {
			commentValue := strings.TrimSpace(strings.TrimPrefix(rowText, commentMarker))
			if strings.HasPrefix(commentValue, configMarker) {
				commentRows = append(commentRows, strings.TrimPrefix(commentValue, configMarker))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read file %s: %s", filePath, err)
	}

	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for file %s: %s", filePath, err)
	}

	task := Task{
		ExecutableFile: ExecutableFile{
			Name: filepath.Base(filePath),
			Path: absFilePath,
		},
		Parameters:  make(map[string]string),
		Connections: make(map[string]string),
		DependsOn:   []string{},
	}
	for _, row := range commentRows {
		keyValue := strings.Split(row, ":")
		if len(keyValue) != 2 {
			continue
		}

		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])

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
	}

	return &task, nil
}
