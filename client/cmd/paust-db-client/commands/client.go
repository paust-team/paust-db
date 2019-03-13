package commands

import (
	"encoding/base64"
	"fmt"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/client/util"
	"github.com/paust-team/paust-db/consts"
	"github.com/spf13/cobra"
	"os"
	"strconv"
	"strings"
	"time"
)

func Execute() {
	if err := ClientCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var ClientCmd = &cobra.Command{
	Use:   "paust-db-client",
	Short: "Paust DB Client Application",
}

var putCmd = &cobra.Command{
	Use:   "put [data to put]",
	Short: "Put data to DB",
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

		ownerId, err := cmd.Flags().GetString("ownerId")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		qualifier, err := cmd.Flags().GetString("qualifier")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if stdin == false && filePath == "" && directoryPath == "" && len(args) == 0 {
			fmt.Println("you should specify data to put")
			os.Exit(1)
		}

		var inputDataObjs []client.InputDataObj
		var inputDataObjMap map[string][]client.InputDataObj

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			inputDataObjs, err = util.GetInputDataObjFromStdin()
			if err != nil {
				fmt.Printf("GetInputDataObjFromStdin err: %v\n", err)
				os.Exit(1)
			}
		case filePath != "":
			fmt.Printf("Read json data from file: %s\n", filePath)
			inputDataObjs, err = util.GetInputDataObjFromFile(filePath)
			if err != nil {
				fmt.Printf("GetInputDataObjFromFile err: %v\n", err)
				os.Exit(1)
			}
		case directoryPath != "":
			fmt.Printf("Read json data from files in directory: %s\n", directoryPath)
			inputDataObjMap, err = util.GetInputDataObjFromDir(directoryPath, recursive)
			if err != nil {
				fmt.Printf("GetInputDataObjFromDir err: %v\n", err)
				os.Exit(1)
			}
		default:
			fmt.Println("Read data from cli arguments")
			if len(ownerId) >= consts.OwnerIdLenLimit || len(ownerId) == 0{
				fmt.Printf("wrong ownerId length. Expected %v, got %v\n", consts.OwnerIdLenLimit, len(ownerId))
				os.Exit(1)
			}
			inputDataObjs = append(inputDataObjs, client.InputDataObj{Timestamp: uint64(time.Now().UnixNano()), OwnerId: ownerId, Qualifier: qualifier, Data: []byte(strings.Join(args, " "))})
		}

		HTTPClient := client.NewHTTPClient(endpoint)
		if inputDataObjMap != nil {
			for path, inputDataObj := range inputDataObjMap {
				startTime := time.Now()
				res, err := HTTPClient.Put(inputDataObj)
				endTime := time.Now()
				if err != nil {
					fmt.Printf("%s: Put err: %v\n", path, err)
					continue
				}
				switch {
				case res.CheckTx.IsErr():
					fmt.Printf("%s: put fail.\n", path)
					fmt.Println(res.CheckTx.Log)
				case res.DeliverTx.IsErr():
					fmt.Printf("%s: put fail.\n", path)
					fmt.Println(res.DeliverTx.Log)
				default:
					fmt.Printf("%s: put success. elapsed time: %v\n", path, endTime.Sub(startTime).Round(time.Millisecond).String())
				}
			}
		} else {
			startTime := time.Now()
			res, err := HTTPClient.Put(inputDataObjs)
			endTime := time.Now()
			if err != nil {
				fmt.Printf("Put err: %v\n", err)
				os.Exit(1)
			}
			switch {
			case res.CheckTx.IsErr():
				fmt.Println("put fail.")
				fmt.Println(res.CheckTx.Log)
			case res.DeliverTx.IsErr():
				fmt.Println("put fail.")
				fmt.Println(res.DeliverTx.Log)
			default:
				fmt.Printf("put success. elapsed time: %v\n", endTime.Sub(startTime).Round(time.Millisecond).String())
			}
		}
	},
}

var queryCmd = &cobra.Command{
	Use:   "query start end",
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

		ownerId, err := cmd.Flags().GetString("ownerId")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		qualifier, err := cmd.Flags().GetString("qualifier")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		HTTPClient := client.NewHTTPClient(endpoint)
		startTime := time.Now()
		res, err := HTTPClient.Query(client.InputQueryObj{Start: start, End: end, OwnerId: ownerId, Qualifier: qualifier})
		endTime := time.Now()
		if err != nil {
			fmt.Printf("Query err: %v\n", err)
			os.Exit(1)
		}
		if res.Response.IsErr() {
			fmt.Println("query fail.")
			fmt.Println(res.Response.Log)
			os.Exit(1)
		}

		fmt.Printf("query success. elapsed time: %v\n", endTime.Sub(startTime).Round(time.Millisecond).String())
		fmt.Println(string(res.Response.Value))
	},
}

var fetchCmd = &cobra.Command{
	Use:   "fetch [id...]",
	Short: "Fetch DB for real data",
	Long: `Fetch DB for real data.
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

		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		var inputFetchObj *client.InputFetchObj

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			inputFetchObj, err = util.GetInputFetchObjFromStdin()
			if err != nil {
				fmt.Printf("GetInputFetchObjFromStdin err: %v\n", err)
				os.Exit(1)
			}
		case filePath != "":
			fmt.Printf("Read json data from file: %s\n", filePath)
			inputFetchObj, err = util.GetInputFetchObjFromFile(filePath)
			if err != nil {
				fmt.Printf("GetInputFetchObjFromFile err: %v\n", err)
				os.Exit(1)
			}
		default:
			if len(args) == 0 {
				fmt.Println("id: you must enter at least one id")
				os.Exit(1)
			}
			fmt.Println("Read data from cli arguments")
			inputFetchObj = new(client.InputFetchObj)
			for _, arg := range args {
				id, err := base64.StdEncoding.DecodeString(arg)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				inputFetchObj.Ids = append(inputFetchObj.Ids, id)
			}
		}

		HTTPClient := client.NewHTTPClient(endpoint)
		startTime := time.Now()
		res, err := HTTPClient.Fetch(*inputFetchObj)
		endTime := time.Now()
		if err != nil {
			fmt.Printf("Fetch err: %v\n", err)
			os.Exit(1)
		}
		if res.Response.IsErr() {
			fmt.Println("fetch fail.")
			fmt.Println(res.Response.Log)
			os.Exit(1)
		}

		fmt.Printf("fetch success. elapsed time: %v\n", endTime.Sub(startTime).Round(time.Millisecond).String())
		fmt.Println(string(res.Response.Value))
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of paust-db",
	Run: func(cmd *cobra.Command, args []string) {
		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		HTTPClient := client.NewHTTPClient(endpoint)
		_, err = HTTPClient.Query(client.InputQueryObj{Start: 1, End: 2, OwnerId: "", Qualifier: ""})
		if err != nil {
			fmt.Println("not running")
		} else {
			fmt.Println("running")
		}
	},
}

func init() {
	putCmd.Flags().StringP("ownerId", "o", "", "Data owner id below 64 characters")
	putCmd.Flags().StringP("qualifier", "q", "", "Data qualifier(JSON object)")
	putCmd.Flags().StringP("file", "f", "", "File path")
	putCmd.Flags().StringP("directory", "d", "", "Directory path")
	putCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	putCmd.Flags().BoolP("recursive", "r", false, "Write all files and folders recursively")
	putCmd.Flags().StringP("endpoint", "e", "localhost:26657", "Endpoint of paust-db")
	fetchCmd.Flags().BoolP("stdin", "s", false, "Input json data from standard input")
	fetchCmd.Flags().StringP("file", "f", "", "File path")
	fetchCmd.Flags().StringP("endpoint", "e", "localhost:26657", "Endpoint of paust-db")
	queryCmd.Flags().StringP("ownerId", "o", "", "Data owner id below 64 characters")
	queryCmd.Flags().StringP("qualifier", "q", "", "Data qualifier(JSON object)")
	queryCmd.Flags().StringP("endpoint", "e", "localhost:26657", "Endpoint of paust-db")
	statusCmd.Flags().StringP("endpoint", "e", "localhost:26657", "Endpoint of paust-db")
	ClientCmd.AddCommand(putCmd)
	ClientCmd.AddCommand(queryCmd)
	ClientCmd.AddCommand(fetchCmd)
	ClientCmd.AddCommand(statusCmd)
}
