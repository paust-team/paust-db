// Package util은 paust-db/client package를 사용함에 있어서 편리한 tool을 제공함.
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

// GetInputDataObjFromStdin는 STDIN에서 client.InputDataObj의 형식으로 구성된 JSON 데이터를 read하여 client.InputDataObj의 slice로 변환해 return.
// STDIN은 EOF가 입력될 때까지 읽음.
func GetInputDataObjFromStdin() ([]client.InputDataObj, error) {
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

// GetInputDataObjFromFile는 given file의 client.InputDataObj의 형식으로 구성된 JSON 데이터를 read하여 client.InputDataObj의 slice로 변환해 return.
func GetInputDataObjFromFile(file string) ([]client.InputDataObj, error) {
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

// GetInputDataObjFromDir는 given dir내의 client.InputDataObj의 형식으로 구성된 모든 *.json 파일에 대해 file path를 key로, read하여 변환한 client.InputDataObj slice를 value로 갖는 map을 return.
// recursive가 true일 경우 given dir의 모든 sub directory를 traverse하면서 *.json 파일을 read함.
func GetInputDataObjFromDir(dir string, recursive bool) (map[string][]client.InputDataObj, error) {
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

// GetInputFetchObjFromStdin는 STDIN에서 client.InputFetchObj의 형식으로 JSON 데디터를 read하여 client.InputFetchObj로 변환해 return.
// STDIN은 EOF가 입력될 때 까지 읽음.
func GetInputFetchObjFromStdin() (*client.InputFetchObj, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		return nil, errors.Wrap(err, "read data of stdin failed")
	}

	var inputFetchObj client.InputFetchObj
	if err := json.Unmarshal(bytes, &inputFetchObj); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	return &inputFetchObj, nil
}

// GetInputFetchObjFromFile는 given file의 client.InputFetchObj의 형식으로 구성된 JSON 데이터를 read하여 client.InputFetchObj로 변환해 return.
func GetInputFetchObjFromFile(file string) (*client.InputFetchObj, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "readFile failed")
	}

	var inputFetchObj client.InputFetchObj
	if err := json.Unmarshal(bytes, &inputFetchObj); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	return &inputFetchObj, nil
}
