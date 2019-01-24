package master

import (
	"fmt"
	"github.com/paust-team/paust-db/consts"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/abci/server"
	"github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"os"
)

var dir string

func Serve() error {
	app := NewMasterApplication(true, dir)
	logger := log.NewTMLogger(log.NewSyncWriter(os.Stdout))

	srv, err := server.NewServer(consts.ProtoAddr, consts.Transport, app)

	if err != nil {
		return err
	}

	srv.SetLogger(logger.With("module", "abci-server"))
	if err := srv.Start(); err != nil {
		return err
	}

	common.TrapSignal(func() {
		srv.Stop()
	})

	return nil
}

var Cmd = &cobra.Command{
	Use:   "master",
	Short: "Paust DB Master Application",
	Run: func(cmd *cobra.Command, args []string) {
		err := Serve()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	Cmd.Flags().StringVarP(&dir, "dir", "d", "/tmp", "directory for rocksdb")
}
