package pipeline

type schedule string

type ExecutableFile struct {
	Name string
	Path string
}

type Task struct {
	Name           string
	Description    string
	Type           string
	ExecutableFile ExecutableFile
	Parameters     map[string]string
	Connections    map[string]string
	DependsOn      []string
}

type Pipeline struct {
	Name               string            `yaml:"name" validate:"required,alphanum"`
	Schedule           schedule          `yaml:"schedule" validate:"required"`
	DefaultParameters  map[string]string `yaml:"defaultParameters"`
	DefaultConnections map[string]string `yaml:"defaultConnections"`
	Tasks              []*Task
}
