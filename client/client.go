package client

import (
	"encoding/json"
	"fmt"
	"github.com/paust-team/paust-db/types"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/rpc/client"
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

func (client *Client) WriteData(time time.Time, data []byte) {
	jsonString, _ := json.Marshal(types.Data{Timestamp: time.Unix(), Data: data})

	client.client.BroadcastTxSync(jsonString)
}

func (client *Client) ReadData(start time.Time, stop time.Time) {
	jsonString, _ := json.Marshal(types.BetweenQuery{Start: start.Unix(), Stop: stop.Unix()})

	client.client.ABCIQuery("/between", jsonString)
}

var Cmd = &cobra.Command{
	Use: "client",
	Short: "Paust DB Client Application",
}

var writeCmd = &cobra.Command{
	Use: "write",
	Short: "Run DB Write",
	Run: func(cmd *cobra.Command, args []string) {
		client := NewClient("http://localhost:26657")

		for i := 0; i < 3; i++ {
			client.WriteData(time.Now(), []byte(fmt.Sprintf("test-%d", i)))
		}
	},
}

func init() {
	Cmd.AddCommand(writeCmd)
}