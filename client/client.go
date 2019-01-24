package client

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/consts"
	"github.com/paust-team/paust-db/types"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/abci/example/code"
	nm "github.com/tendermint/tendermint/node"
	"github.com/tendermint/tendermint/rpc/client"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	"golang.org/x/crypto/ed25519"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	client client.Client
}

func NewClient(remote string) *Client {
	c := client.NewHTTP(remote, "/websocket")

	return &Client{
		client: c,
	}
}

func NewLocalClient(node *nm.Node) *Client {
	c := client.NewLocal(node)

	return &Client{
		client: c,
	}
}

func (client *Client) WriteData(time time.Time, ownerKey string, qualifier string, data []byte) (*ctypes.ResultBroadcastTx, error) {
	ownerKeyBytes, err := base64.StdEncoding.DecodeString(ownerKey)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if len(ownerKeyBytes) != consts.OwnerKeyLen {
		fmt.Printf("public key: ed25519 public key must be %d bytes\n", consts.OwnerKeyLen)
		os.Exit(1)
	}

	jsonString, _ := json.Marshal(types.WRealDataObjs{types.WRealDataObj{Timestamp: uint64(time.UnixNano()), OwnerKey: ownerKeyBytes, Qualifier: []byte(qualifier), Data: data}})

	bres, err := client.client.BroadcastTxSync(jsonString)
	return bres, err
}

func (client *Client) ReadData(rowKeys []string) (*ctypes.ResultABCIQuery, error) {
	var rRealDataQueryObj = types.RRealDataQueryObj{}
	for _, rowKey := range rowKeys {
		rRealDataQueryObj.Keys = append(rRealDataQueryObj.Keys, []byte(rowKey))
	}
	jsonString, _ := json.Marshal(rRealDataQueryObj)

	res, err := client.client.ABCIQuery("/realdata", jsonString)
	return res, err
}

func (client *Client) ReadDataOfStdin() (*ctypes.ResultABCIQuery, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		errors.Wrap(err, "read data of stdin")
		return nil, err
	}

	serializedBytes, err := SerializeKeyObj(bytes)
	if err != nil {
		errors.Wrap(err, "SerializeKeyObj")
		return nil, err
	}

	res, err := client.client.ABCIQuery("/realdata", serializedBytes)
	return res, err
}

func (client *Client) ReadDataOfFile(file string) (*ctypes.ResultABCIQuery, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		errors.Wrap(err, "read data of file")
		return nil, err
	}

	serializedBytes, err := SerializeKeyObj(bytes)
	if err != nil {
		errors.Wrap(err, "SerializeKeyObj")
		return nil, err
	}

	res, err := client.client.ABCIQuery("/realdata", serializedBytes)
	return res, err
}

// TODO: implement split large size data to many transactions.
func (client *Client) WriteFile(file string) (*ctypes.ResultBroadcastTx, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	bres, err := client.client.BroadcastTxSync(bytes)
	return bres, err
}

func (client *Client) WriteFilesInDir(dir string, recursive bool) {
	if recursive == true {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Printf("directory traverse err: %v\n", err)
				os.Exit(1)
			}

			if info.IsDir() == false && ".json" == filepath.Ext(path) {
				bres, err := client.WriteFile(path)
				if err != nil {
					fmt.Printf("WriteFile: %v\n", err)
					os.Exit(1)
				}
				if bres.Code == code.CodeTypeOK {
					fmt.Printf("%s: write success.\n", path)
				} else {
					fmt.Printf("%s: write fail.\n", path)
					fmt.Println(bres.Log)
				}
				return nil
			} else {
				return nil
			}
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	} else {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Printf("directory traverse err: %v\n", err)
				os.Exit(1)
			}

			switch {
			case info.IsDir() == true && path != dir:
				return filepath.SkipDir
			case info.IsDir() == false && ".json" == filepath.Ext(path):
				bres, err := client.WriteFile(path)
				if err != nil {
					fmt.Printf("WriteFile: %v\n", err)
					os.Exit(1)
				}
				if bres.Code == code.CodeTypeOK {
					fmt.Printf("%s: write success.\n", path)
				} else {
					fmt.Printf("%s: write fail.\n", path)
					fmt.Println(bres.Log)
				}
				return nil
			default:
				return nil
			}
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

func (client *Client) WriteStdin() (*ctypes.ResultBroadcastTx, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		fmt.Println(err)
		os.Exit(1)
	}

	bres, err := client.client.BroadcastTxSync(bytes)
	return bres, err
}

func (client *Client) ReadMetaData(start uint64, end uint64) (*ctypes.ResultABCIQuery, error) {
	jsonString, _ := json.Marshal(types.RMetaDataQueryObj{Start: start, End: end})

	res, err := client.client.ABCIQuery("/metadata", jsonString)
	return res, err
}

func DeSerializeKeyObj(obj []byte, isMeta bool) ([]byte, error) {
	if isMeta == true {
		var rMetaDataResObjs = types.RMetaDataResObjs{}
		err := json.Unmarshal(obj, &rMetaDataResObjs)
		if err != nil {
			errors.Wrap(err, "unmarshal failed")
			return nil, err
		}

		var rClientMetaDataResObjs = types.RClientMetaDataResObjs{}
		for _, resObj := range rMetaDataResObjs {
			var keyObj = types.KeyObj{}
			err := json.Unmarshal(resObj.RowKey, &keyObj)
			if err != nil {
				errors.Wrap(err, "unmarshal failed")
				return nil, err
			}
			rClientMetaDataResObjs = append(rClientMetaDataResObjs, types.RClientMetaDataResObj{RowKey: keyObj, OwnerKey: resObj.OwnerKey, Qualifier: resObj.Qualifier})
		}
		deserializedObj, err := json.Marshal(rClientMetaDataResObjs)
		if err != nil {
			errors.Wrap(err, "marshal failed")
			return nil, err
		}
		return deserializedObj, nil
	} else {
		var rRealDataResObjs = types.RRealDataResObjs{}
		err := json.Unmarshal(obj, &rRealDataResObjs)
		if err != nil {
			errors.Wrap(err, "unmarshal failed")
			return nil, err
		}

		var rClientRealDataResObjs = types.RClientRealDataResObjs{}
		for _, resObj := range rRealDataResObjs {
			var keyObj = types.KeyObj{}
			err := json.Unmarshal(resObj.RowKey, &keyObj)
			if err != nil {
				errors.Wrap(err, "unmarshal failed")
				return nil, err
			}
			rClientRealDataResObjs = append(rClientRealDataResObjs, types.RClientRealDataResObj{RowKey: keyObj, Data: resObj.Data})
		}

		deserializedObj, err := json.Marshal(rClientRealDataResObjs)
		if err != nil {
			errors.Wrap(err, "marshal failed")
			return nil, err
		}
		return deserializedObj, nil
	}
}

func SerializeKeyObj(obj []byte) ([]byte, error) {
	var rClientRealDataQueryObj = types.RClientRealDataQueryObj{}
	err := json.Unmarshal(obj, &rClientRealDataQueryObj)
	if err != nil {
		errors.Wrap(err, "unmarshal failed")
		return nil, err
	}

	var rRealDataQueryObj = types.RRealDataQueryObj{}
	for _, keyObj := range rClientRealDataQueryObj.Keys {
		rowKey, err := json.Marshal(keyObj)
		if err != nil {
			errors.Wrap(err, "marshal failed")
			return nil, err
		}
		rRealDataQueryObj.Keys = append(rRealDataQueryObj.Keys, rowKey)
	}
	serializedObj, err := json.Marshal(rRealDataQueryObj)
	if err != nil {
		errors.Wrap(err, "marshal failed")
		return nil, err
	}
	return serializedObj, nil
}

var ownerKey, qualifier, filePath, queryFilePath, directoryPath string

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "Paust DB Client Application",
}

var writeCmd = &cobra.Command{
	Use:   "write [data to write]",
	Short: "Run DB Write",
	Run: func(cmd *cobra.Command, args []string) {
		stdin, err := cmd.Flags().GetBool("stdin")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		recursive, err := cmd.Flags().GetBool("recursive")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if stdin == false && filePath == "" && directoryPath == "" && len(args) == 0 {
			fmt.Println("data: you should specify data to write")
			os.Exit(1)
		}

		client := NewClient("http://localhost:26657")

		var bres *ctypes.ResultBroadcastTx

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			bres, err = client.WriteStdin()
		case filePath != "":
			fmt.Printf("Read json data from file: %s\n", filePath)
			bres, err = client.WriteFile(filePath)
		case directoryPath != "":
			fmt.Printf("Read json data from files in directory: %s\n", directoryPath)
			client.WriteFilesInDir(directoryPath, recursive)
		default:
			fmt.Println("Read data from cli arguments")
			bres, err = client.WriteData(time.Now(), ownerKey, qualifier, []byte(strings.Join(args, " ")))
		}
		if directoryPath == "" {
			if err != nil {
				fmt.Printf("err: %v\n", err)
				os.Exit(1)
			}
			if bres.Code == code.CodeTypeOK {
				fmt.Println("Write success.")
			} else {
				fmt.Println("Write fail.")
				fmt.Println(bres.Log)
			}
		}
	},
}

var writeTestCmd = &cobra.Command{
	Use:   "writeTest",
	Short: "Run DB Write Test",
	Run: func(cmd *cobra.Command, args []string) {
		client := NewClient("http://localhost:26657")

		for i := 0; i < 3; i++ {
			client.WriteData(time.Now(), "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE=", qualifier, []byte(fmt.Sprintf("test-%d", i)))
		}
	},
}

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate ED25519 Key Pair",
	Run: func(cmd *cobra.Command, args []string) {
		pubKey, priKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Printf("Private Key(base64 encoding): %v\n", base64.StdEncoding.EncodeToString(priKey))
		fmt.Printf("Public Key(base64 encoding): %v\n", base64.StdEncoding.EncodeToString(pubKey))
	},
}

var realdataCmd = &cobra.Command{
	Use:   "realdata [rowKey...]",
	Short: "Query DB for real data",
	Long: `Query DB for real data.
'rowKey' is a JSON object without spaces.
ex) {timestamp:1544772882435375000}`,
	Run: func(cmd *cobra.Command, args []string) {
		stdin, err := cmd.Flags().GetBool("stdin")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := NewClient("http://localhost:26657")

		var res *ctypes.ResultABCIQuery

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			res, err = client.ReadDataOfStdin()
		case queryFilePath != "":
			fmt.Printf("Read json data from file: %s\n", queryFilePath)
			res, err = client.ReadDataOfFile(queryFilePath)
		default:
			fmt.Println("Read data from cli arguments")
			res, err = client.ReadData(args)
		}

		if err != nil {
			fmt.Printf("err: %v\n", err)
			os.Exit(1)
		}

		deserializedBytes, err := DeSerializeKeyObj(res.Response.Value, false)
		fmt.Println(string(deserializedBytes))
	},
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query something to DB",
}

var metadataCmd = &cobra.Command{
	Use:   "metadata start end",
	Args:  cobra.ExactArgs(2),
	Short: "Query DB for metadata",
	Long: `Query DB for metadata.
'start' and 'end' are unix timestamp in nanosecond.`,
	Run: func(cmd *cobra.Command, args []string) {
		start, err := strconv.ParseUint(args[0], 0, 64)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		end, err := strconv.ParseUint(args[1], 0, 64)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := NewClient("http://localhost:26657")

		res, err := client.ReadMetaData(start, end)
		if err != nil {
			fmt.Printf("err: %v\n", err)
			os.Exit(1)
		}

		deserializedBytes, err := DeSerializeKeyObj(res.Response.Value, true)
		fmt.Println(string(deserializedBytes))
	},
}

func init() {
	writeCmd.Flags().StringVarP(&ownerKey, "ownerKey", "o", "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE=", "Base64 encoded ED25519 public key")
	writeCmd.Flags().StringVarP(&qualifier, "qualifier", "q", "test", "Data qualifier")
	writeCmd.Flags().StringVarP(&filePath, "file", "f", "", "File path")
	writeCmd.Flags().StringVarP(&directoryPath, "directory", "d", "", "Directory path")
	writeCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	writeCmd.Flags().BoolP("recursive", "r", false, "Write all files and folders recursively")
	realdataCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	realdataCmd.Flags().StringVarP(&queryFilePath, "file", "f", "", "File path")
	Cmd.AddCommand(writeCmd)
	Cmd.AddCommand(writeTestCmd)
	Cmd.AddCommand(generateCmd)
	Cmd.AddCommand(queryCmd)
	queryCmd.AddCommand(metadataCmd)
	queryCmd.AddCommand(realdataCmd)
}
