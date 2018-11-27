package cmd

import (
	"fmt"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/master"
	"github.com/spf13/cobra"
	"os"
)

var PaustDBCmd = &cobra.Command{
	Use: "paust-db",
	Short: "Paust DB",
}

func Execute() {
	PaustDBCmd.AddCommand(master.Cmd, client.Cmd)

	if err := PaustDBCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}