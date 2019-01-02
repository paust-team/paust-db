# paust-db

paust-db

## Getting Started

These instructions will get you a copy of the project up and running on your local machine(MacOS 10.14.2) for development and testing purposes.

### Prerequisites

You'll need `go` [installed](https://golang.org/doc/install) and `tendermint` [installed](https://tendermint.com/docs/introduction/install.html).

To install RocksDB library, run

```bash
brew install snappy zlib bzip2 lz4 zstd
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
make static_lib
make install-static
```

### Installing

A step by step series of examples that tell you how to get a development env running

To get source code, run
```bash
go get -u github.com/paust-team/paust-db
cd $GOPATH/src/github.com/paust-team/paust-db
```

To compile and put the binary in `$GOPATH/bin`, run

```bash
CGO_CFLAGS="-I/usr/local/include/rocksdb" \
CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
go install
```

`paust-db` is now installed.

```bash
$ paust-db
Paust DB

Usage:
  paust-db [command]

Available Commands:
  client      Paust DB Client Application
  help        Help about any command
  master      Paust DB Master Application

Flags:
  -h, --help   help for paust-db

Use "paust-db [command] --help" for more information about a command.
```

To compile easily without flags, set environment variables in your shell start script like

```bash
export CGO_CFLAGS="-I/usr/local/include/rocksdb"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
```

## Running the tests

Start paust-db master application at one terminal: 
```bash
paust-db master
```

Start tendermint node at another terminal:

```bash
tendermint init
tendermint node
```

### Write test

Write 3 data set:

```bash
echo "[
        {"timestamp":1544772882435375000,"userKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","type":"cpu","data":"YWJj"},
        {"timestamp":1544772960049177000,"userKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","type":"mem","data":"ZGVm"},
        {"timestamp":1544772967331458000,"userKey":"aFw+o2z13LFCXzk7HptFoOY54s7VGDeQQVo32REPFCU=","type":"speed","data":"Z2hp"}
]" | paust-db client write -s
```

### Read test

Read all data from `1544770000000000000` to `1544773000000000000`:

```bash
$ paust-db client query realdata 1544770000000000000 1544773000000000000
{
	"response": {
		"value": "W3sidGltZXN0YW1wIjoxNTQ0NzcyODgyNDM1Mzc1MDAwLCJ1c2VyS2V5IjoiTndkVGYrUzkrSDVsc0I2VXMrczVZMUNoZEIxYUtFQ0E2Z3N5R0NhOFNDTT0iLCJ0eXBlIjoiY3B1IiwiZGF0YSI6IllXSmoifSx7InRpbWVzdGFtcCI6MTU0NDc3Mjk2MDA0OTE3NzAwMCwidXNlcktleSI6Im1uaEtjVVduUjFpWVRtNm80U0ovWDBGVjY3UUZJeXRwTEIwM0VtV00xQ1k9IiwidHlwZSI6Im1lbSIsImRhdGEiOiJaR1ZtIn0seyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjczMzE0NTgwMDAsInVzZXJLZXkiOiJhRncrbzJ6MTNMRkNYems3SHB0Rm9PWTU0czdWR0RlUVFWbzMyUkVQRkNVPSIsInR5cGUiOiJzcGVlZCIsImRhdGEiOiJaMmhwIn1d"
	}
}
```

Decode value from base64 format:

```json
[{"timestamp":1544772882435375000,"userKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","type":"cpu","data":"YWJj"},{"timestamp":1544772960049177000,"userKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","type":"mem","data":"ZGVm"},{"timestamp":1544772967331458000,"userKey":"aFw+o2z13LFCXzk7HptFoOY54s7VGDeQQVo32REPFCU=","type":"speed","data":"Z2hp"}]
```

***

Read all data which `type` is `cpu` from `1544770000000000000` to `1544773000000000000`:

```bash
$ paust-db client query realdata 1544770000000000000 1544773000000000000 -t cpu
{
	"response": {
		"value": "W3sidGltZXN0YW1wIjoxNTQ0NzcyODgyNDM1Mzc1MDAwLCJ1c2VyS2V5IjoiTndkVGYrUzkrSDVsc0I2VXMrczVZMUNoZEIxYUtFQ0E2Z3N5R0NhOFNDTT0iLCJ0eXBlIjoiY3B1IiwiZGF0YSI6IllXSmoifV0="
	}
}
```

Decode value from base64 format:

```json
[{"timestamp":1544772882435375000,"userKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","type":"cpu","data":"YWJj"}]
```

***

Read all data which `userKey` is `mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=` from `1544770000000000000` to `1544773000000000000`:

```bash
$ paust-db client query realdata 1544770000000000000 1544773000000000000 -p mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=
{
	"response": {
		"value": "W3sidGltZXN0YW1wIjoxNTQ0NzcyOTYwMDQ5MTc3MDAwLCJ1c2VyS2V5IjoibW5oS2NVV25SMWlZVG02bzRTSi9YMEZWNjdRRkl5dHBMQjAzRW1XTTFDWT0iLCJ0eXBlIjoibWVtIiwiZGF0YSI6IlpHVm0ifV0="
	}
}
```

Decode value from base64 format:

```json
[{"timestamp":1544772960049177000,"userKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","type":"mem","data":"ZGVm"}]
```

## License

GPLv3