package path

import (
	"fmt"
	"io/ioutil"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

func readYaml(path string, out interface{}) error {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(buf, out)
	if err != nil {
		return fmt.Errorf("cannot read the pipeline definition at '%s': %v", path, err)
	}

	validate := validator.New()
	err = validate.Struct(out)
	if err != nil {
		return fmt.Errorf("cannot validate the YAML file at '%s': %v", path, err)
	}

	return nil
}
