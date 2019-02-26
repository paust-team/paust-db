# Paust-DB Client
* [godoc for client](https://godoc.org/github.com/paust-team/paust-db/client)
## Client Interface
```go
// Client는 paust-db와 communicate하는 기본적인 client임
type Client interface {
	// Put는 InputDataObj slice의 데이터를 write하고 그 결과를 tendermint의 ResultBroadcastTx로 return.
	Put(dataObjs []InputDataObj) (*ctypes.ResultBroadcastTx, error)

	// Query는 start와 end사이에 있는 데이터의 metadata를 ResultABCIQuery에 담아서 return.
	// ownerKey와 qualifier가 명시된 경우 해당 ownerKey, qualifier와 일치하는 데이터만을 read.
	// ResultABCIQuery.Response.Value에 실제 read한 데이터가 OutputQueryObj의 slice로 담겨있음.
	Query(start uint64, end uint64, ownerKey []byte, qualifier []byte) (*ctypes.ResultABCIQuery, error)

	// Fetch는 InputFetchObj와 일치하는 데이터를 tendermint의 ResultABCIQuery에 담아서 return.
	// ResultABCIQuery.Response.Value에 실제 read한 데이터가 OutputFetchObj의 slice로 담겨있음.
	Fetch(fetchObj InputFetchObj) (*ctypes.ResultABCIQuery, error)
}
```

### Example
paust-db client API를 사용하기 위해서는 client package를 import해야함
```go
import "github.com/paust-team/paust-db/client"
```
- `Put(dataObjs []InputDataObj) (*ctypes.ResultBroadcastTx, error)`
```go
// Example
inputDataObjs := []client.InputDataObj{{Timestamp: time.Now().UnixNano(), OwnerKey: ownerKey, Qualifier: qualifier, Data: data}}
HTTPClient := client.NewHTTPClient("http://localhost:26657")
res, err := HTTPClient.Put(inputDataObjs)
if err != nil {
	fmt.Println(err)
	os.Exit(1)
}
if res.Code != 0 {
	fmt.Println(res.Log)
	os.Exit(1)
}
```
- `Query(start uint64, end uint64, ownerKey []byte, qualifier []byte) (*ctypes.ResultABCIQuery, error)`
```go
// Example
HTTPClient := client.NewHTTPClient("http://localhost:26657")
res, err := HTTPClient.Query(start, end, ownerKey, qualifier)
if err != nil {
	fmt.Println(err)
	os.Exit(1)
}
if res.Code != 0 {
	fmt.Println(res.Log)
	os.Exit(1)
}

fmt.Println(string(res.Response.Value))
```

- `Fetch(fetchObj InputFetchObj) (*ctypes.ResultABCIQuery, error)`
```go
// Example
inputFetchObj := client.InputFetchObj{Ids: [][]byte{id1, id2, id3}}
HTTPClient := client.NewHTTPClient("http://localhost:26657")
res, err := HTTPClient.Fetch(inputFetchObj)
if err != nil {
	fmt.Println(err)
	os.Exit(1)
}
if res.Code != 0 {
	fmt.Println(res.Log)
	os.Exit(1)
}

fmt.Println(string(res.Response.Value))
```
* 자세한 example은 [client/cmd/paust-db-client/commands/client.go](https://github.com/paust-team/paust-db/blob/master/client/cmd/paust-db-client/commands/client.go) 참고

## CLI usage
### Paust-db-client install
paust-db의 put, query, fetch등의 기능을 쉽게 테스트 하기 위한 Client CLI 를 제공함

```
$ go get github.com/paust-team/paust-db/client/cmd/paust-db-client
$ paust-db-client
Paust DB Client Application

Usage:
  paust-db-client [command]

Available Commands:
  fetch       Fetch DB for real data
  generate    Generate ED25519 Key Pair
  help        Help about any command
  put         Put data to DB
  query       Query DB for metadata
  status      Check status of paust-db

Flags:
  -h, --help   help for paust-db-client

Use "paust-db-client [command] --help" for more information about a command.
```

### Put data
paust-db-client put command 를 이용하여 여러 방법으로 데이터를 time series db에 쓸 수 있음
put data 구조는 `client.InputDataObj` 를 따름
- Stdin 방식
cli 상에서 `client.InputDataObj`형식을 가진 JSON object의 array를 사용하여 put 할 수 있음
```
# put data of STDIN
$ echo '[
        {"timestamp":1544772882435375000,"ownerKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","qualifier":"{\"type\":\"temperature\"}","data":"YWJj"},
        {"timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"{\"type\":\"speed\"}","data":"ZGVm"},
        {"timestamp":1544772967331458000,"ownerKey":"aFw+o2z13LFCXzk7HptFoOY54s7VGDeQQVo32REPFCU=","qualifier":"{\"type\":\"price\"}","data":"Z2hp"}
]' | paust-db-client put -s
Read json data from STDIN
put success.
```
- File 방식
쓸 데이터가 많은 경우 File 을 통하여 put 할 수 있음
파일에 작성되는 data형태는 `client.InputDataObj`형식을 가진 JSON object의 array로 [test/write_file.json](https://github.com/paust-team/paust-db/blob/master/test/write_file.json) 참고
```
# put data of file
$ paust-db-client put -f something_to_write.json
Read json data from file: something_to_write.json
put success.
```

- Directory 방식
recursive option(-r)을 사용하여 nested directory를 탐색하여 file들을 찾아 put 할 수 있음
```
# put data of files in directory
$ paust-db-client put -d /root/writeDirectory -r
Read json data from files in directory: /root/writeDirectory
/root/wirteDirectory/test1.json: put success.
/root/wirteDirectory/test2.json: put success.
/root/wirteDirectory/recursiveDirectory/test3.json: put success.
```

- Cli argument 방식
```
# put data of cli arguments
$ paust-db-client put 123456 -o mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY= -q '{"type":"temperature"}'
Read data from cli arguments
put success.
```
기타 put 에 관련된 usage 를 --help 를 통해 확인할 수 있음 
```
$ paust-db-client put --help
Put data to DB

Usage:
  paust-db-client put [data to put] [flags]

Flags:
  -d, --directory string       Directory path
  -e, --endpoint string        Endpoint of paust-db (default "localhost:26657")
  -f, --file string            File path
  -h, --help                   help for put
  -o, --ownerKey bytesBase64   Base64 encoded ED25519 public key
  -q, --qualifier string       Data qualifier(JSON object)
  -r, --recursive              Write all files and folders recursively
  -s, --stdin                  Input json data from standard input
```

### Query data
paust-db-client query command 를 이용하여 start, end timestamp 사이에 있는 time series 데이터의 metadata를 가져올 수 있음
flag를 통해 ownerKey, qualifier를 명시하면 특정 ownerKey, qualifier와 일치하는 데이터만 가져 옴
- start, end timestamp명시
```
# Query with start, end
$ paust-db-client query 1544772882435375000 1544772882435375001
query success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjQ1fQ==","timestamp":1544772882435375000,"ownerKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","qualifier":"{\"type\":\"temperature\"}"}]
```
- start, end timestamp와 ownerKey 명시
```
# Query with start, end, ownerKey
$ paust-db-client query 1544772882435375000 1544772967331458001 -o mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=
query success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"{\"type\":\"speed\"}"}]
```
- start, end timestamp와 qualifier 명시
```
# Query with start, end, qualifier
$ paust-db-client query 1544772882435375000 1544772967331458001 -q '{"type":"price"}'
query success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjczMzE0NTgwMDAsInNhbHQiOjM5fQ==","timestamp":1544772967331458000,"ownerKey":"aFw+o2z13LFCXzk7HptFoOY54s7VGDeQQVo32REPFCU=","qualifier":"{\"type\":\"price\"}"}]
```
- start, end timestamp와 ownerKey, qualifier 명시
```
# Query with start, end, ownerKey, qualifier
$ paust-db-client query 1544772882435375000 1544772967331458001 -o mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY= -q '{"type":"speed"}'
query success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"{\"type\":\"speed\"}"}]
```

기타 query에 관련된 usage를 --help를 통해 확인할 수 있음
```
$ paust-db-client query --help
Query DB for metadata.
'start' and 'end' are unix timestamp in nanosecond.

Usage:
  paust-db-client query start end [flags]

Flags:
  -e, --endpoint string        Endpoint of paust-db (default "localhost:26657")
  -h, --help                   help for query
  -o, --ownerKey bytesBase64   Base64 encoded ED25519 public key
  -q, --qualifier string       Data qualifier(JSON object)
```

### Fetch Data
paust-db-client fetch command 를 이용하여 여러 방법으로 time series db의 데이터를 읽을 수 있음
fetch object 구조는 `client.InputFetchObj` 를 따름
- STDIN 방식
cli 상에서 `client.InputFetchObj`형식을 가진 JSON object의 array를 사용하여 특정 id를 가진 데이터를 fetch 할 수 있음
```
# Fetch with STDIN
$ echo '{
  "ids":[
    "eyJ0aW1lc3RhbXAiOjE1NDQ3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjQ1fQ==",
    "eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0="
  ]
}' | paust-db-client fetch -s
Read json data from STDIN
fetch success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjQ1fQ==","timestamp":1544772882435375000,"data":"YWJj"},{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"data":"ZGVm"}]
```
- File 방식
읽을 데이터가 많은 경우 File 을 통하여 특정 id를 가진 데이터를 fetch 할 수 있음 
파일에 작성되는 data형태는 client.InputFetchObj형식을 가진 JSON object의 array로 [test/read_file.json](https://github.com/paust-team/paust-db/blob/master/test/read_file.json) 참고
```
# Fetch with file
$ paust-db-client fetch -f ids_to_read.json
Read json data from file: ids_to_read.json
fetch success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjQ1fQ==","timestamp":1544772882435375000,"data":"YWJj"}]
```
- Cli argument 방식
```
# Fetch with cli arguments
$ paust-db-client fetch eyJ0aW1lc3RhbXAiOjE1NDQ3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjQ1fQ== eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=
Read data from cli arguments
fetch success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI4ODI0MzUzNzUwMDAsInNhbHQiOjQ1fQ==","timestamp":1544772882435375000,"data":"YWJj"},{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"data":"ZGVm"}]
```
기타 fetch에 관련된 usage를 --help를 통해 확인할 수 있음
```
$ paust-db-client fetch --help
Fetch DB for real data.
'id' is a base64 encoded byte array.

Usage:
  paust-db-client fetch [id...] [flags]

Flags:
  -e, --endpoint string   Endpoint of paust-db (default "localhost:26657")
  -f, --file string       File path
  -h, --help              help for fetch
  -s, --stdin             Input json data from standard input
```

### Check status of pasut-db
paust-db-client status command 를 이용하여 paust-db의 health를 체크할 수 있음
```
$ paust-db-client status -e localhost:26657
running

$ paust-db-client status --help
Check status of paust-db

Usage:
  paust-db-client status [flags]

Flags:
  -e, --endpoint string   Endpoint of paust-db (default "localhost:26657")
  -h, --help              help for status
```

### Generate ED25519 key pair
paust-db-client generate command 를 이용하여 ED25519 key pair를 생성할 수 있음
```
$ paust-db-client generate
Private Key(base64 encoding): clHDfdXUGVbE52H3jnhBVtnSD0qdCPSou26Ptl3SOGxHJlqBZzZAMlH1lxXFT3JishRccC9OaqNlTs0mEWmG3g==
Public Key(base64 encoding): RyZagWc2QDJR9ZcVxU9yYrIUXHAvTmqjZU7NJhFpht4=
```
