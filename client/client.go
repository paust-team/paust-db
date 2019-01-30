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
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type InputDataObj struct {
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
	Data      []byte `json:"data"`
}

type InputQueryObj struct {
	Ids [][]byte `json:"ids"`
}

type OutputMetaDataObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type OutputRealDataObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	Data      []byte `json:"data"`
}

type Client struct {
	client client.Client
}

func NewClient(remote string) *Client {
	c := client.NewHTTP(remote, consts.WsEndpoint)
	rand.Seed(time.Now().UnixNano())

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

func (client *Client) WriteData(timestamp uint64, ownerKey []byte, qualifier []byte, data []byte) (*ctypes.ResultBroadcastTx, error) {
	if len(ownerKey) != consts.OwnerKeyLen {
		return nil, fmt.Errorf("wrong ownerKey length. Expected %v, got %v", consts.OwnerKeyLen, len(ownerKey))
	}

	rowKey, err := json.Marshal(types.KeyObj{Timestamp: timestamp, Salt: uint8(rand.Intn(256))})
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	jsonString, err := json.Marshal([]types.BaseDataObj{{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: ownerKey, Qualifier: qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: data}}})
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	bres, err := client.client.BroadcastTxSync(jsonString)
	return bres, err
}

func (client *Client) ReadData(ids [][]byte) (*ctypes.ResultABCIQuery, error) {
	var realDataQueryObj = types.RealDataQueryObj{RowKeys: ids}

	jsonString, err := json.Marshal(realDataQueryObj)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.client.ABCIQuery(consts.RealDataQueryPath, jsonString)
	return res, err
}

func (client *Client) ReadDataOfStdin() (*ctypes.ResultABCIQuery, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		return nil, errors.Wrap(err, "read data of stdin failed")
	}

	var queryObj InputQueryObj
	if err := json.Unmarshal(bytes, &queryObj); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	var realDataQueryObj types.RealDataQueryObj
	for _, id := range queryObj.Ids {
		realDataQueryObj.RowKeys = append(realDataQueryObj.RowKeys, id)
	}

	jsonString, err := json.Marshal(realDataQueryObj)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.client.ABCIQuery(consts.RealDataQueryPath, jsonString)
	return res, err
}

func (client *Client) ReadDataOfFile(file string) (*ctypes.ResultABCIQuery, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "readFile failed")
	}

	var queryObj InputQueryObj
	if err := json.Unmarshal(bytes, &queryObj); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	var realDataQueryObj types.RealDataQueryObj
	for _, id := range queryObj.Ids {
		realDataQueryObj.RowKeys = append(realDataQueryObj.RowKeys, id)
	}

	jsonString, err := json.Marshal(realDataQueryObj)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.client.ABCIQuery(consts.RealDataQueryPath, jsonString)
	return res, err
}

// TODO: implement split large size data to many transactions.
func (client *Client) WriteFile(file string) (*ctypes.ResultBroadcastTx, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "readFile failed")
	}

	var inputDataObjs []InputDataObj

	if err := json.Unmarshal(bytes, &inputDataObjs); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	var baseDataObjs []types.BaseDataObj

	for _, inputDataObj := range inputDataObjs {
		rowKey, err := json.Marshal(types.KeyObj{Timestamp: inputDataObj.Timestamp, Salt: uint8(rand.Intn(256))})
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: inputDataObj.OwnerKey, Qualifier: inputDataObj.Qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: inputDataObj.Data}})
	}

	jsonString, err := json.Marshal(baseDataObjs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	bres, err := client.client.BroadcastTxSync(jsonString)
	return bres, err
}

func (client *Client) WriteFilesInDir(dir string, recursive bool) {
	if recursive == true {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrap(err, "filepath walk err")
			}

			if info.IsDir() == false && ".json" == filepath.Ext(path) {
				bres, err := client.WriteFile(path)
				if err != nil {
					fmt.Printf("WriteFile(%s): %v\n", path, err)
					return nil
				}
				if bres.Code == code.CodeTypeOK {
					fmt.Printf("%s: write success.\n", path)
				} else {
					fmt.Printf("%s: write fail.\n", path)
					fmt.Println(bres.Log)
				}
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
		}
	} else {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrap(err, "filepath walk err")
			}

			switch {
			case info.IsDir() == true && path != dir:
				return filepath.SkipDir
			case info.IsDir() == false && ".json" == filepath.Ext(path):
				bres, err := client.WriteFile(path)
				if err != nil {
					fmt.Printf("WriteFile(%s): %v\n", path, err)
					return nil
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
		}
	}
}

func (client *Client) WriteStdin() (*ctypes.ResultBroadcastTx, error) {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		return nil, errors.Wrap(err, "read data of stdin failed")
	}

	var inputDataObjs []InputDataObj

	if err := json.Unmarshal(bytes, &inputDataObjs); err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	var baseDataObjs []types.BaseDataObj

	for _, inputDataObj := range inputDataObjs {
		rowKey, err := json.Marshal(types.KeyObj{Timestamp: inputDataObj.Timestamp, Salt: uint8(rand.Intn(256))})
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		baseDataObjs = append(baseDataObjs, types.BaseDataObj{MetaData: types.MetaDataObj{RowKey: rowKey, OwnerKey: inputDataObj.OwnerKey, Qualifier: inputDataObj.Qualifier}, RealData: types.RealDataObj{RowKey: rowKey, Data: inputDataObj.Data}})
	}

	jsonString, err := json.Marshal(baseDataObjs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	bres, err := client.client.BroadcastTxSync(jsonString)
	return bres, err
}

func (client *Client) ReadMetaData(start uint64, end uint64, ownerKey []byte, qualifier []byte) (*ctypes.ResultABCIQuery, error) {
	if len(ownerKey) != 0 && len(ownerKey) != consts.OwnerKeyLen {
		return nil, fmt.Errorf("wrong ownerKey length. Expected %v, got %v", consts.OwnerKeyLen, len(ownerKey))
	}

	jsonString, err := json.Marshal(types.MetaDataQueryObj{Start: start, End: end, OwnerKey: ownerKey, Qualifier: qualifier})
	if err != nil {
		return nil, errors.Wrap(err, "marshal failed")
	}

	res, err := client.client.ABCIQuery(consts.MetaDataQueryPath, jsonString)
	return res, err
}

func DeSerializeKeyObj(obj []byte, isMeta bool) ([]byte, error) {
	if isMeta == true {
		var metaDataObjs []types.MetaDataObj
		if err := json.Unmarshal(obj, &metaDataObjs); err != nil {
			return nil, errors.Wrap(err, "unmarshal failed")
		}

		var deserializedMeta []OutputMetaDataObj
		for _, metaDataObj := range metaDataObjs {
			var keyObj = types.KeyObj{}
			if err := json.Unmarshal(metaDataObj.RowKey, &keyObj); err != nil {
				return nil, errors.Wrap(err, "unmarshal failed")
			}
			deserializedMeta = append(deserializedMeta, OutputMetaDataObj{Id: metaDataObj.RowKey, Timestamp: keyObj.Timestamp, OwnerKey: metaDataObj.OwnerKey, Qualifier: metaDataObj.Qualifier})
		}
		deserializedObj, err := json.Marshal(deserializedMeta)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		return deserializedObj, nil
	} else {
		var realDataObjs []types.RealDataObj
		if err := json.Unmarshal(obj, &realDataObjs); err != nil {
			return nil, errors.Wrap(err, "unmarshal failed")
		}

		var deserializedReal []OutputRealDataObj
		for _, realDataObj := range realDataObjs {
			var keyObj = types.KeyObj{}
			if err := json.Unmarshal(realDataObj.RowKey, &keyObj); err != nil {
				return nil, errors.Wrap(err, "unmarshal failed")
			}
			deserializedReal = append(deserializedReal, OutputRealDataObj{Id: realDataObj.RowKey, Timestamp: keyObj.Timestamp, Data: realDataObj.Data})
		}
		deserializedObj, err := json.Marshal(deserializedReal)
		if err != nil {
			return nil, errors.Wrap(err, "marshal failed")
		}
		return deserializedObj, nil
	}
}

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

		filePath, err := cmd.Flags().GetString("file")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		directoryPath, err := cmd.Flags().GetString("directory")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ownerKey, err := cmd.Flags().GetBytesBase64("ownerKey")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		qualifier, err := cmd.Flags().GetBytesBase64("qualifier")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if stdin == false && filePath == "" && directoryPath == "" && len(args) == 0 {
			fmt.Println("data: you should specify data to write")
			os.Exit(1)
		}

		client := NewClient(consts.Remote)

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
			bres, err = client.WriteData(uint64(time.Now().UnixNano()), ownerKey, qualifier, []byte(strings.Join(args, " ")))
		}
		if directoryPath == "" {
			if err != nil {
				fmt.Printf("write err: %v\n", err)
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
	Use:   "realdata [id...]",
	Short: "Query DB for real data",
	Long: `Query DB for real data.
'id' is a base64 encoded byte array.`,
	Run: func(cmd *cobra.Command, args []string) {
		stdin, err := cmd.Flags().GetBool("stdin")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		filePath, err := cmd.Flags().GetString("file")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := NewClient(consts.Remote)

		var res *ctypes.ResultABCIQuery

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			res, err = client.ReadDataOfStdin()
		case filePath != "":
			fmt.Printf("Read json data from file: %s\n", filePath)
			res, err = client.ReadDataOfFile(filePath)
		default:
			fmt.Println("Read data from cli arguments")
			var ids [][]byte
			for _, arg := range args {
				id, err := base64.StdEncoding.DecodeString(arg)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				ids = append(ids, id)
			}
			res, err = client.ReadData(ids)
		}

		if err != nil {
			fmt.Printf("realdata err: %v\n", err)
			os.Exit(1)
		}

		deserializedBytes, err := DeSerializeKeyObj(res.Response.Value, false)
		if err != nil {
			fmt.Printf("deserialize key object err: %v\n", err)
			os.Exit(1)
		}
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

		ownerKey, err := cmd.Flags().GetBytesBase64("ownerKey")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		qualifier, err := cmd.Flags().GetBytesBase64("qualifier")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := NewClient(consts.Remote)

		res, err := client.ReadMetaData(start, end, ownerKey, qualifier)
		if err != nil {
			fmt.Printf("metadata err: %v\n", err)
			os.Exit(1)
		}

		deserializedBytes, err := DeSerializeKeyObj(res.Response.Value, true)
		if err != nil {
			fmt.Printf("deserialize key object err: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(deserializedBytes))
	},
}

func init() {
	writeCmd.Flags().BytesBase64P("ownerKey", "o", nil, "Base64 encoded ED25519 public key")
	writeCmd.Flags().BytesBase64P("qualifier", "q", nil, "Base64 encoded data qualifier")
	writeCmd.Flags().StringP("file", "f", "", "File path")
	writeCmd.Flags().StringP("directory", "d", "", "Directory path")
	writeCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	writeCmd.Flags().BoolP("recursive", "r", false, "Write all files and folders recursively")
	realdataCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	realdataCmd.Flags().StringP("file", "f", "", "File path")
	metadataCmd.Flags().BytesBase64P("ownerKey", "o", nil, "Base64 encoded ED25519 public key")
	metadataCmd.Flags().BytesBase64P("qualifier", "q", nil, "Base64 encoded data qualifier")
	Cmd.AddCommand(writeCmd)
	Cmd.AddCommand(generateCmd)
	Cmd.AddCommand(queryCmd)
	queryCmd.AddCommand(metadataCmd)
	queryCmd.AddCommand(realdataCmd)
}
