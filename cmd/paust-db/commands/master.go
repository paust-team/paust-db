package commands

import (
	"fmt"
	"github.com/paust-team/paust-db/consts"
	"github.com/paust-team/paust-db/libs/log"
	"github.com/paust-team/paust-db/master"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/abci/server"
	"github.com/tendermint/tendermint/libs/common"
	tmlog "github.com/tendermint/tendermint/libs/log"
	"os"
)

var dir, level string

func Serve() error {
	option, err := log.AllowLevel(level)
	if err != nil {
		fmt.Printf("level parsing err: %v\n", err)
		os.Exit(1)
	}

	app := master.NewMasterApplication(true, dir, option)
	logger := tmlog.NewTMLogger(tmlog.NewSyncWriter(os.Stdout))

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

var MasterCmd = &cobra.Command{
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
	MasterCmd.Flags().StringVarP(&dir, "dir", "d", "/tmp", "directory for rocksdb")
	MasterCmd.Flags().StringVarP(&level, "level", "l", "info", "set log level [debug|info|error|none]")
}
