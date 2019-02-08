package commands

import (
	"encoding/base64"
	"fmt"
	"github.com/paust-team/paust-db/client"
	"github.com/paust-team/paust-db/client/util"
	"github.com/paust-team/paust-db/consts"
	"github.com/spf13/cobra"
	"github.com/tendermint/tendermint/abci/example/code"
	"golang.org/x/crypto/ed25519"
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

		var inputDataObjs []client.InputDataObj
		var inputDataObjMap map[string][]client.InputDataObj

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			inputDataObjs, err = util.GetInputDataFromStdin()
			if err != nil {
				fmt.Printf("GetInputDataFromStdin err: %v\n", err)
			}
			os.Exit(1)
		case filePath != "":
			fmt.Printf("Read json data from file: %s\n", filePath)
			inputDataObjs, err = util.GetInputDataFromFile(filePath)
			if err != nil {
				fmt.Printf("GetInputDataFromFile err: %v\n", err)
			}
			os.Exit(1)
		case directoryPath != "":
			fmt.Printf("Read json data from files in directory: %s\n", directoryPath)
			inputDataObjMap, err = util.GetInputDataFromDir(directoryPath, recursive)
			if err != nil {
				fmt.Printf("GetInputDataFromDir err: %v\n", err)
			}
			os.Exit(1)
		default:
			fmt.Println("Read data from cli arguments")
			if len(ownerKey) != consts.OwnerKeyLen {
				fmt.Printf("wrong ownerKey length. Expected %v, got %v\n", consts.OwnerKeyLen, len(ownerKey))
				os.Exit(1)
			}
			inputDataObjs = append(inputDataObjs, client.InputDataObj{Timestamp: uint64(time.Now().UnixNano()), OwnerKey: ownerKey, Qualifier: qualifier, Data: []byte(strings.Join(args, " "))})
		}

		HTTPClient := client.NewHTTPClient(consts.Remote)
		if inputDataObjMap != nil {
			for path, inputDataObj := range inputDataObjMap {
				res, err := HTTPClient.WriteData(inputDataObj)
				if err != nil {
					fmt.Printf("%s: WriteData err: %v\n", path, err)
					continue
				}
				if res.Code == code.CodeTypeOK {
					fmt.Printf("%s: write success.\n", path)
				} else {
					fmt.Printf("%s: write fail.\n", path)
					fmt.Println(res.Log)
				}
			}
		} else {
			res, err := HTTPClient.WriteData(inputDataObjs)
			if err != nil {
				fmt.Printf("WriteData err: %v\n", err)
				os.Exit(1)
			}
			if res.Code == code.CodeTypeOK {
				fmt.Println("Write success.")
			} else {
				fmt.Println("Write fail.")
				fmt.Println(res.Log)
			}
		}
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

		HTTPClient := client.NewHTTPClient(consts.Remote)
		res, err := HTTPClient.Query(start, end, ownerKey, qualifier)
		if err != nil {
			fmt.Printf("Query err: %v\n", err)
			os.Exit(1)
		}

		fmt.Println(string(res.Response.Value))
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

		var inputQueryObj *client.InputQueryObj

		switch {
		case stdin == true:
			fmt.Println("Read json data from STDIN")
			inputQueryObj, err = util.GetInputQueryFromStdin()
			if err != nil {
				fmt.Printf("GetInputQueryFromStdin err: %v\n", err)
			}
			os.Exit(1)
		case filePath != "":
			fmt.Printf("Read json data from file: %s\n", filePath)
			inputQueryObj, err = util.GetInputQueryFromFile(filePath)
			if err != nil {
				fmt.Printf("GetInputQueryFromFile err: %v\n", err)
			}
			os.Exit(1)
		default:
			if len(args) == 0 {
				fmt.Println("id: you must enter at least one id")
				os.Exit(1)
			}
			fmt.Println("Read data from cli arguments")
			inputQueryObj = new(client.InputQueryObj)
			for _, arg := range args {
				id, err := base64.StdEncoding.DecodeString(arg)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				inputQueryObj.Ids = append(inputQueryObj.Ids, id)
			}
		}

		HTTPClient := client.NewHTTPClient(consts.Remote)
		res, err := HTTPClient.Fetch(*inputQueryObj)
		if err != nil {
			fmt.Printf("Fetch err: %v\n", err)
			os.Exit(1)
		}

		fmt.Println(string(res.Response.Value))
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
	ClientCmd.AddCommand(writeCmd)
	ClientCmd.AddCommand(generateCmd)
	ClientCmd.AddCommand(queryCmd)
	queryCmd.AddCommand(metadataCmd)
	queryCmd.AddCommand(realdataCmd)
}
