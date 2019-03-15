package util_test

import (
	"encoding/json"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/client/util"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

const (
	TestWriteFile = "../../test/write_file.json"
	TestDirectory = "../../test/write_directory"
	TestReadFile  = "../../test/read_file.json"
)

func TestGetInputDataFromStdin(t *testing.T) {
	require := require.New(t)

	inputData := `[
        {"timestamp":1544772882435375000,"ownerId":"owner1","qualifier":"{\"type\":\"temperature\"}","data":"YWJj"},
        {"timestamp":1544772960049177000,"ownerId":"owner2","qualifier":"{\"type\":\"speed\"}","data":"ZGVm"},
        {"timestamp":1544772967331458000,"ownerId":"owner3","qualifier":"{\"type\":\"price\"}","data":"Z2hp"}
]`
	var dataObjs []client.InputDataObj
	err := json.Unmarshal([]byte(inputData), &dataObjs)
	require.Nil(err, "json unmarshal err: %+v", err)

	stdin := os.Stdin
	defer func() {
		os.Stdin = stdin
	}()

	r, w, _ := os.Pipe()
	os.Stdin = r

	_, err = w.Write([]byte(inputData))
	require.Nil(err, "pipe write err: %+v", err)
	err = w.Close()
	require.Nil(err, "pipe close err: %+v", err)

	inputDataObjs, err := util.GetInputDataObjFromStdin()
	require.Nil(err, "err: %+v", err)

	require.EqualValues(dataObjs, inputDataObjs)
}

func TestGetInputDataFromFile(t *testing.T) {
	require := require.New(t)

	bytes, err := ioutil.ReadFile(TestWriteFile)
	require.Nil(err, "file read err: %+v", err)

	var dataObjs []client.InputDataObj

	err = json.Unmarshal(bytes, &dataObjs)
	require.Nil(err, "json unmarshal err: %+v", err)

	inputDataObjs, err := util.GetInputDataObjFromFile(TestWriteFile)
	require.Nil(err, "err: %+v", err)

	require.EqualValues(dataObjs, inputDataObjs)
}

func TestGetInputDataFromDir(t *testing.T) {
	require := require.New(t)

	dataObjMap := make(map[string][]client.InputDataObj)
	err := filepath.Walk(TestDirectory, func(path string, info os.FileInfo, err error) error {
		require.Nil(err, "directory traverse err: %+v", err)
		switch {
		case info.IsDir() == true && path != TestDirectory:
			return filepath.SkipDir
		case info.IsDir() == false && ".json" == filepath.Ext(path):
			bytes, err := ioutil.ReadFile(path)
			require.Nil(err, "file read err: %+v", err)

			var inputDataObjs []client.InputDataObj

			err = json.Unmarshal(bytes, &inputDataObjs)
			require.Nil(err, "json unmarshal err: %+v", err)

			dataObjMap[path] = inputDataObjs
			return nil
		default:
			return nil
		}
	})
	require.Nil(err, "directory traverse err: %+v", err)

	inputDataObjMap, err := util.GetInputDataObjFromDir(TestDirectory, false)
	require.Nil(err, "err: %+v", err)

	require.EqualValues(dataObjMap, inputDataObjMap)

	dataObjMap = make(map[string][]client.InputDataObj)
	err = filepath.Walk(TestDirectory, func(path string, info os.FileInfo, err error) error {
		require.Nil(err, "directory traverse err: %+v", err)
		if info.IsDir() == false && ".json" == filepath.Ext(path) {
			bytes, err := ioutil.ReadFile(path)
			require.Nil(err, "file read err: %+v", err)

			var inputDataObjs []client.InputDataObj

			err = json.Unmarshal(bytes, &inputDataObjs)
			require.Nil(err, "json unmarshal err: %+v", err)

			dataObjMap[path] = inputDataObjs
		}
		return nil
	})
	require.Nil(err, "directory traverse err: %+v", err)

	inputDataObjMap, err = util.GetInputDataObjFromDir(TestDirectory, true)
	require.Nil(err, "err: %+v", err)

	require.EqualValues(dataObjMap, inputDataObjMap)
}

func TestGetInputFetchFromStdin(t *testing.T) {
	require := require.New(t)

	inputFetch := `{
  "ids":[
    "eyJ0aW1lc3RhbXAiOjE1NDc3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjB9",
    "eyJ0aW1lc3RhbXAiOjE1NDc3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjB9",
    "eyJ0aW1lc3RhbXAiOjE1NDc3NzI5NjczMzE0NTgwMDAsInNhbHQiOjB9"
  ]
}`
	var fetchObj client.InputFetchObj
	err := json.Unmarshal([]byte(inputFetch), &fetchObj)

	stdin := os.Stdin
	defer func() {
		os.Stdin = stdin
	}()

	r, w, _ := os.Pipe()
	os.Stdin = r

	_, err = w.Write([]byte(inputFetch))
	require.Nil(err, "pipe write err: %+v", err)
	err = w.Close()
	require.Nil(err, "pipe close err: %+v", err)

	inputFetchObj, err := util.GetInputFetchObjFromStdin()
	require.Nil(err, "err: %+v", err)

	require.EqualValues(fetchObj, *inputFetchObj)
}

func TestGetInputFetchFromFile(t *testing.T) {
	require := require.New(t)

	bytes, err := ioutil.ReadFile(TestReadFile)
	require.Nil(err, "file read err: %+v", err)

	var fetchObj client.InputFetchObj

	err = json.Unmarshal(bytes, &fetchObj)
	require.Nil(err, "json unmarshal err: %+v", err)

	inputFetchObj, err := util.GetInputFetchObjFromFile(TestReadFile)
	require.Nil(err, "err: %+v", err)

	require.EqualValues(fetchObj, *inputFetchObj)
}
