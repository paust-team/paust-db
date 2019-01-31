package util

import (
	"bufio"
	"encoding/json"
	"github.com/paust-team/paust-db/client"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func GetInputDataFromStdin() ([]client.InputDataObj, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		return nil, errors.Wrap(err, "read data of stdin failed")
	}

	var inputDataObjs []client.InputDataObj
	if err := json.Unmarshal(bytes, &inputDataObjs); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	return inputDataObjs, nil
}

func GetInputDataFromFile(file string) ([]client.InputDataObj, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "readFile failed")
	}

	var inputDataObjs []client.InputDataObj
	if err := json.Unmarshal(bytes, &inputDataObjs); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	return inputDataObjs, nil
}

func GetInputDataFromDir(dir string, recursive bool) (map[string][]client.InputDataObj, error) {
	inputDataObjMap := make(map[string][]client.InputDataObj)
	if recursive == true {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrap(err, "filepath walk err")
			}

			if info.IsDir() == false && ".json" == filepath.Ext(path) {
				bytes, err := ioutil.ReadFile(path)
				if err != nil {
					return errors.Wrap(err, "readFile failed")
				}

				var inputDataObjs []client.InputDataObj
				if err := json.Unmarshal(bytes, &inputDataObjs); err != nil {
					return errors.Wrap(err, "unmarshal failed")
				}
				inputDataObjMap[path] = inputDataObjs
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		return inputDataObjMap, err
	} else {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrap(err, "filepath walk err")
			}

			switch {
			case info.IsDir() == true && path != dir:
				return filepath.SkipDir
			case info.IsDir() == false && ".json" == filepath.Ext(path):
				bytes, err := ioutil.ReadFile(path)
				if err != nil {
					return errors.Wrap(err, "readFile failed")
				}

				var inputDataObjs []client.InputDataObj
				if err := json.Unmarshal(bytes, &inputDataObjs); err != nil {
					return errors.Wrap(err, "unmarshal failed")
				}
				inputDataObjMap[path] = inputDataObjs

				return nil
			default:
				return nil
			}
		})
		if err != nil {
			return nil, err
		}

		return inputDataObjMap, err
	}
}

func GetInputQueryFromStdin() (*client.InputQueryObj, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		return nil, errors.Wrap(err, "read data of stdin failed")
	}

	var inputQueryObj client.InputQueryObj
	if err := json.Unmarshal(bytes, &inputQueryObj); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	return &inputQueryObj, nil
}

func GetInputQueryFromFile(file string) (*client.InputQueryObj, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "readFile failed")
	}

	var inputQueryObj client.InputQueryObj
	if err := json.Unmarshal(bytes, &inputQueryObj); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	return &inputQueryObj, nil
}
