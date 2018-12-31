package client

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/abci/example/code"
	nm "github.com/tendermint/tendermint/node"
	"github.com/tendermint/tendermint/rpc/client"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	"golang.org/x/crypto/ed25519"
	"io"
	"io/ioutil"
	"log"
	"os"
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

func (client *Client) WriteData(time time.Time, pubKey string, dataType string, data []byte) (*ctypes.ResultBroadcastTx, error) {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if len(pubKeyBytes) != 32 {
		fmt.Println("public key: ed25519 public key must be 32bytes")
		os.Exit(1)
	}

	jsonString, _ := json.Marshal(types.DataSlice{types.Data{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Type: dataType, Data: data}})

	bres, err := client.client.BroadcastTxSync(jsonString)
	return bres, err
}

func (client *Client) ReadData(start int64, end int64, pubKey string, dataType string) (*ctypes.ResultABCIQuery, error) {
	var pubKeyBytes []byte
	if pubKey != "" {
		var err error
		pubKeyBytes, err = base64.StdEncoding.DecodeString(pubKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if len(pubKeyBytes) != 32 {
			fmt.Println("public key: ed25519 public key must be 32bytes")
			os.Exit(1)
		}
	}

	if len(dataType) > 20 {
		fmt.Printf("type: \"%v\" is bigger than 20 bytes", dataType)
		os.Exit(1)
	}

	jsonString, _ := json.Marshal(types.DataQuery{Start: start, End: end, UserKey: pubKeyBytes, Type: dataType})

	res, err := client.client.ABCIQuery("/realdata", jsonString)
	return res, err
}

// TODO: implement write all files in specific directory.
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

func (client *Client) ReadMetaData(start int64, end int64, pubKey string, dataType string) (*ctypes.ResultABCIQuery, error) {
	var pubKeyBytes []byte
	if pubKey != "" {
		var err error
		pubKeyBytes, err = base64.StdEncoding.DecodeString(pubKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if len(pubKeyBytes) != 32 {
			fmt.Println("public key: ed25519 public key must be 32bytes")
			os.Exit(1)
		}
	}
	if len(dataType) > 20 {
		fmt.Printf("type: \"%v\" is bigger than 20 bytes", dataType)
		os.Exit(1)
	}

	jsonString, _ := json.Marshal(types.DataQuery{Start: start, End: end, UserKey: pubKeyBytes, Type: dataType})

	res, err := client.client.ABCIQuery("/metadata", jsonString)
	return res, err
}

var writePubKey, writeDataType, pubKey, dataType, filePath string

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "Paust DB Client Application",
}

var writeCmd = &cobra.Command{
	Use:   "write [data to write]",
	Short: "Run DB Write",
	Run: func(cmd *cobra.Command, args []string) {
		stdin, _ := cmd.Flags().GetBool("stdin")

		if len(dataType) > 20 {
			log.Fatalf("type: \"%v\" is bigger than 20 bytes", dataType)
		}

		if stdin == false && filePath == "" && len(args) == 0 {
			fmt.Println("data: you should specify data to write")
			os.Exit(1)
		}

		client := NewClient("http://localhost:26657")

		switch {
		case stdin == true:
			bres, err := client.WriteStdin()
			if err != nil {
				fmt.Printf("err: %+v", err)
				os.Exit(1)
			}
			if bres.Code == code.CodeTypeOK {
				fmt.Println("Write success.")
			} else {
				fmt.Println("Write fail.")
			}
		case filePath != "":
			bres, err := client.WriteFile(filePath)
			if err != nil {
				fmt.Printf("err: %+v", err)
				os.Exit(1)
			}
			if bres.Code == code.CodeTypeOK {
				fmt.Println("Write success.")
			} else {
				fmt.Println("Write fail.")
			}
		default:
			bres, err := client.WriteData(time.Now(), writePubKey, writeDataType, []byte(strings.Join(args, " ")))
			if err != nil {
				fmt.Printf("err: %+v", err)
				os.Exit(1)
			}
			if bres.Code == code.CodeTypeOK {
				fmt.Println("Write success.")
			} else {
				fmt.Println("Write fail.")
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
			client.WriteData(time.Now(), "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE=", dataType, []byte(fmt.Sprintf("test-%d", i)))
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
	Use:   "realdata start end",
	Args:  cobra.ExactArgs(2),
	Short: "Query DB for real data",
	Long: `Query DB for real data.
'start' and 'end' are essential. '-p' and '-t' flags are optional.
If you want to query for only one timestamp, make 'start' and 'end' equal.`,
	Run: func(cmd *cobra.Command, args []string) {
		start, err := strconv.ParseInt(args[0], 0, 64)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		end, err := strconv.ParseInt(args[1], 0, 64)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := NewClient("http://localhost:26657")

		res, err := client.ReadData(start, end, pubKey, dataType)
		if err != nil {
			fmt.Printf("err: %+v", err)
			os.Exit(1)
		}

		fmt.Println(string(res.Response.Value))
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
'start' and 'end' are essential. '-p' and '-t' flags are optional.
If you want to query for only one timestamp, make 'start' and 'end' equal.`,
	Run: func(cmd *cobra.Command, args []string) {
		start, err := strconv.ParseInt(args[0], 0, 64)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		end, err := strconv.ParseInt(args[1], 0, 64)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		client := NewClient("http://localhost:26657")

		res, err := client.ReadMetaData(start, end, pubKey, dataType)
		if err != nil {
			fmt.Printf("err: %+v", err)
			os.Exit(1)
		}

		fmt.Println(string(res.Response.Value))
	},
}

func init() {
	writeCmd.Flags().StringVarP(&writePubKey, "pubkey", "p", "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE=", "Base64 encoded ED25519 public key")
	writeCmd.Flags().StringVarP(&writeDataType, "type", "t", "test", "Data type (max 20 bytes)")
	writeCmd.Flags().StringVarP(&filePath, "file", "f", "", "File path")
	writeCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	Cmd.AddCommand(writeCmd)
	Cmd.AddCommand(writeTestCmd)
	Cmd.AddCommand(generateCmd)
	Cmd.AddCommand(queryCmd)
	queryCmd.AddCommand(metadataCmd)
	metadataCmd.Flags().StringVarP(&pubKey, "pubkey", "p", "", "user's public key (base64)")
	metadataCmd.Flags().StringVarP(&dataType, "type", "t", "", "data type (max 20 bytes)")
	queryCmd.AddCommand(realdataCmd)
	realdataCmd.Flags().StringVarP(&pubKey, "pubkey", "p", "", "user's public key (base64)")
	realdataCmd.Flags().StringVarP(&dataType, "type", "t", "", "data type (max 20 bytes)")
}
