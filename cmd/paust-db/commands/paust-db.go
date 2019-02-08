package commands

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var PaustDBCmd = &cobra.Command{
	Use:   "paust-db",
	Short: "Paust DB",
}

func Execute() {
	PaustDBCmd.AddCommand(MasterCmd)

	if err := PaustDBCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
