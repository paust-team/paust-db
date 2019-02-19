# Paust DB

Paust DB is a blockchain based a decentralized database platform for continuous timeseries.

Paust DB는 하나의 연속적인 Timeseries를 블록체인 내에서 관리한다. 각 부분적인 Timeseries에 대해서 사용자가 자신의 데이터를 기록하고 있고 사용자는 권한에 따라 허가된 부분적인 Timeseries에 접근하여 데이터를 조회하고 가져올 수 있다. DApp이 이 플랫폼을 이용한다면 블록체인 환경내에서 실시간 데이터 처리를 위해 상태를 저장하거나 다시 이전에 있었던 시계열 데이터를 가져와서 처리하기에 용이하다. 

## Features
- 하나의 연속적인 Timeseries를 실시간으로 관리
- Timeseries에 대해 Data를 기록하고 조회 가능
- (TBD) 대용량의 Timeseries에 대하여 빠른 데이터 조회 가능
- (TBD) 정책에 따라 자신에게 맞는 시계열 데이터베이스 구축

## Installation(Mac OS)
linux(ubuntu, alpine 등) 지원 예정
### Install go
안정성을 위해 1.11.5 설치 추천(https://golang.org/doc/install)
* Set env for go
```shell
mkdir ~/go
echo 'export GOPATH="$HOME/go"' >> ~/.bash_profile
echo 'export PATH="$PATH:$GOPATH/bin"' >> ~/.bash_profile
source ~/.bash_profile
```

### Install rocksdb
* rocksdb dependency install using homebrew
```
brew install snappy zlib bzip2 lz4 zstd cmake
```
* 5.17.2 version의 rocksdb를 clone한 후 cmake를 이용해 build
```
cd ~
git clone https://github.com/facebook/rocksdb.git -b v5.17.2
mkdir ~/rocksdb/build && cd ~/rocksdb/build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DWITH_GFLAGS=OFF -DWITH_TESTS=OFF ..
make install
ln -s /usr/local/lib64/librocksdb.so.5 /usr/local/lib/librocksdb.so.5
```
### Set env for gorocksdb
```shell
echo 'export CGO_CFLAGS="-I/usr/local/include"' >> ~/.bash_profile
echo 'export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"' >> ~/.bash_profile
source ~/.bash_profile
```

### Install paust-db
```
go get github.com/paust-team/paust-db/cmd/paust-db
```

### Install tendermint
```
cd $GOPATH/src/github.com/tendermint/tendermint
git checkout v0.30.0
make get_tools
make get_vendor_deps
make install
```

### Run
* run paust-db
```
paust-db master
```
* run tendermint
```
tendermint unsafe_reset_all
tendermint init
tendermint node
```

## Quick start
### Install client cli
다음 명령어를 통해서 paust-db-client 를 install 하여 local 환경에서 cli 테스트를 할 수 있음 
자세한 cli 명령어는 [client](https://github.com/paust-team/paust-db/tree/master/client)에서 확인할 수 있음
```
go get github.com/paust-team/paust-db/client/cmd/paust-db-client
```
### Put
스트림을 이용한 Json data를 Put 하는 example
```
$ echo '[
        {"timestamp":1544772882435375000,"ownerKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","qualifier":"Y3B1","data":"YWJj"},
        {"timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"bWVt","data":"ZGVm"},
        {"timestamp":1544772967331458000,"ownerKey":"aFw+o2z13LFCXzk7HptFoOY54s7VGDeQQVo32REPFCU=","qualifier":"c3BlZWQ=","data":"Z2hp"}
]' | paust-db-client put -s
Read json data from STDIN
put success.
```

### Query
time range 사이의 ownerkey가 mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY= 이고, qualifier가 bWVt인 item을 Query하는 example
```
$ paust-db-client query 1544772882435375000 1544772967331458001 -o mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY= -q bWVt
query success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"bWVt"}]
```

### Fetch
query를 통하여 받은 id인 eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0= 를 이용해 실제 data를 fetch하는 example
```
$ paust-db-client fetch eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=
Read data from cli arguments
fetch success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"data":"ZGVm"}]
```

## Clustering
### Setup
#### Run paust-db
```
paust-db master
```
#### Network configuration
validators를 genesis.json에 설정하고, config.toml에 통신을 위한 seeds를 설정함
```
# n : validator의 수
tendermint testnet -v n
```
`./mytestnet`에 있는 n개의 node 정보를 n개의 node에 각각 Copy한 후(ex. node0 directory는 첫 번째 node, node1 directory는 두 번째 node, ...) 각 node에서 아래의 command 실행
```
tendermint node
```

### Node 추가
non-validator인 node 추가를 위해 초기 설정을 생성한 후 genesis.json 파일과 seeds를 추가하여야 한다.
```
tendermint init
```
초기 구축 node의 `ip:port/genesis`의 http response로 genesis.json파일을 수정한다.
초기 구축 node의 `ip:port/status`의 http response에서 node_info object의 id를 얻는다.
```
tendermint node --p2p.seeds ID@IP:PORT
```

### Docker Support
- [docker guide](/docker/README.md)
- 위의 Installation 과정을 최소화 할 수 있도록 Docker Image 제공
- Docker를 통한 multi node clustering 을 테스트 할 수 있도록 localnet 테스트 지원


## Contributing
- Welcome PR
- Owner 명시(PAUST Inc.) www.paust.io

## License
- PaustDB(master) is GPLv3-style licensed, as found in the [LICENSE](https://github.com/paust-team/paust-db/LICENSE) file.
- PaustDB(client) is LGPL-style licensed, as found in the [LICENSE](https://github.com/paust-team/paust-db/client/LICENSE) file.

## Third-party

* go(https://github.com/golang/go)
* tendermint(https://github.com/tendermint/tendermint)
* gorocksdb(https://github.com/tecbot/gorocksdb)
* testify(https://github.com/stretchr/testify)
* corbra(https://github.com/spf13/cobra)
* kit(https://github.com/go-kit/kit)
* logfmt(https://github.com/go-logfmt/logfmt)
* errors(https://github.com/pkg/errors)
