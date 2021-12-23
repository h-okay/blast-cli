package path

import (
	"github.com/pkg/errors"
	"io/ioutil"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

func ReadYaml(path string, out interface{}) error {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(buf, out)
	if err != nil {
		return errors.Wrapf(err, "cannot read the pipeline definition at '%s'", path)
	}

	validate := validator.New()
	err = validate.Struct(out)
	if err != nil {
		return errors.Wrapf(err, "cannot validate the YAML file at '%s'", path)
	}

	return nil
}
