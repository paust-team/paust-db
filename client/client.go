package client

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/rpc/client"
	"golang.org/x/crypto/ed25519"
	"io"
	"io/ioutil"
	"log"
	"os"
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

func (client *Client) WriteData(time time.Time, pubKey string, dataType string, data []byte) {
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

	client.client.BroadcastTxSync(jsonString)
}

func (client *Client) ReadData(start time.Time, stop time.Time) {
	jsonString, _ := json.Marshal(types.BetweenQuery{Start: start.Unix(), Stop: stop.Unix()})

	client.client.ABCIQuery("/between", jsonString)
}

// TODO: implement write all files in specific directory.
// TODO: implement split large size data to many transactions.
func (client *Client) WriteFile(file string) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	client.client.BroadcastTxSync(bytes)
}

func (client *Client) WriteStdin() {
	in := bufio.NewReader(os.Stdin)
	bytes, err := in.ReadBytes(0x00)
	if err != io.EOF {
		fmt.Println(err)
		os.Exit(1)
	}

	client.client.BroadcastTxSync(bytes)
}

var pubKey, dataType, filePath string

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

		if stdin == true {
			client.WriteStdin()
		} else if filePath != "" {
			client.WriteFile(filePath)
		} else {
			client.WriteData(time.Now(), pubKey, dataType, []byte(strings.Join(args, " ")))
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

func init() {
	writeCmd.Flags().StringVarP(&pubKey, "pubkey", "p", "Pe8PPI4Mq7kJIjDJjffoTl6s5EezGQSyIcu5Y2KYDaE=", "Base64 encoded ED25519 public key")
	writeCmd.Flags().StringVarP(&dataType, "type", "t", "test", "Data type (max 20 bytes)")
	writeCmd.Flags().StringVarP(&filePath, "file", "f", "", "File path")
	writeCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	Cmd.AddCommand(writeCmd)
	Cmd.AddCommand(writeTestCmd)
	Cmd.AddCommand(generateCmd)
}
