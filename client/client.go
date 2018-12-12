package client

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/rpc/client"
	"golang.org/x/crypto/ed25519"
	"log"
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
		panic(err)
	}
	//TODO: length 체크
	jsonString, _ := json.Marshal(types.Data{Timestamp: time.UnixNano(), UserKey: pubKeyBytes, Type: dataType, Data: data})

	client.client.BroadcastTxSync(jsonString)
}

func (client *Client) ReadData(start time.Time, stop time.Time) {
	jsonString, _ := json.Marshal(types.BetweenQuery{Start: start.Unix(), Stop: stop.Unix()})

	client.client.ABCIQuery("/between", jsonString)
}

var pubKey, dataType string

var Cmd = &cobra.Command{
	Use: "client",
	Short: "Paust DB Client Application",
}

var writeCmd = &cobra.Command{
	Use: "write [data to write]",
	Args: cobra.MinimumNArgs(1),
	Short: "Run DB Write",
	Run: func(cmd *cobra.Command, args []string) {
		if len(dataType) > 20 {
			log.Fatalf("type: \"%v\" is bigger than 20 bytes", dataType)
		}
		client := NewClient("http://localhost:26657")
		client.WriteData(time.Now(), pubKey, dataType, []byte(strings.Join(args, " ")))
	},
}

var writeTestCmd = &cobra.Command{
	Use: "writeTest",
	Short: "Run DB Write Test",
	Run: func(cmd *cobra.Command, args []string) {
		client := NewClient("http://localhost:26657")

		for i := 0; i < 3; i++ {
			client.WriteData(time.Now(), "Krc92XkJ+LhkDMO+Qe1utVg1KNGbXdhri3Ol9u5dIAY97w88jgyruQkiMMmN9+hOXqzkR7MZBLIhy7ljYpgNoQ==", dataType, []byte(fmt.Sprintf("test-%d", i)))
		}
	},
}

var generateCmd = &cobra.Command{
	Use: "generate",
	Short: "Generate ED25519 Key Pair",
	Run: func(cmd *cobra.Command, args []string) {
		priKey, pubKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Private Key(base64 encoding): %v\n", base64.StdEncoding.EncodeToString(priKey))
		fmt.Printf("Public Key(base64 encoding): %v\n", base64.StdEncoding.EncodeToString(pubKey))
	},
}

func init() {
	writeCmd.Flags().StringVarP(&pubKey, "pubkey", "p", "Krc92XkJ+LhkDMO+Qe1utVg1KNGbXdhri3Ol9u5dIAY97w88jgyruQkiMMmN9+hOXqzkR7MZBLIhy7ljYpgNoQ==", "Base64 encoded ED25519 public key")
	writeCmd.Flags().StringVarP(&dataType, "type", "t", "test", "Data type (max 20 bytes)")
	Cmd.AddCommand(writeCmd)
	Cmd.AddCommand(writeTestCmd)
	Cmd.AddCommand(generateCmd)
}



