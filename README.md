![Alt text](white_logo_color_background.jpg)

[![Build Status](https://travis-ci.org/paust-team/paust-db.svg?branch=master)](https://travis-ci.org/paust-team/paust-db)

# Paust DB

Paust DB is a blockchain based decentralized database platform for continuous timeseries.

PAUST DB 는 시계열 데이터에 특화된 탈중앙화 데이터 베이스 인프라 입니다.

최근 데이터 시대에는 이전보다 몇 배나 더 많은 양의 데이터들이 디바이스나 개인으로부터 발생하고 있습니다. 하지만 기존 데이터의 형태와 달리 연속적이고 정형화되지 않은 시계열 데이터와 중앙화된 인프라 구조의 한계로 시계열 데이터를 제대로 활용하지 못하고 있습니다.

앞으로는 성능이나 자원 효율의 관점에서 위와 같은 문제를 해결할 수 있는 새로운 탈중앙화 데이터베이스가 필요합니다. 우리는 시계열 데이터에 특화된 탈중앙화 데이터베이스로 PAUST DB를 만들어 네트워크 데이터 공유 인프라를 구축하는 것을 목표로 합니다.

## 주요 특징
- 탈중앙화 네트워크 기반 데이터 저장소(Decentralized Date Store)
- 소유권 정보 불변성(Immutable Ownership)
- 시계열 분산데이터 조회 최적화(Timeseries Query Optimizer)
- 참여형 오픈 데이터베이스(Open Database for Participation)

## Features
- 하나의 연속적인 대용량 Timeseries 데이터를 실시간으로 관리 가능
- 시계열 쿼리 전용 인터페이스
- 다양한 정책에 따른 데이터베이스 지원

## Prerequisite
Requirement|Version
---|---
Golang | 1.11.5 or higher
Rocksdb | 5.17.2 or higher
Tendermint | 0.29.0 or higher

### Install go
[Install](https://golang.org/doc/install)

[Setting environments](https://github.com/golang/go/wiki/SettingGOPATH)


### Install rocksdb
#### Rocksdb dependency install
zlib, bzip2, lz4, zstandard, snappy

* Mac OS
```shell
$ brew install snappy zlib bzip2 lz4 zstd cmake
```
* Ubuntu
```shell
$ sudo apt-get install libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
```

#### 5.17.2 version의 rocksdb를 clone한 후 cmake를 이용해 build
```shell
$ cd ~
$ git clone https://github.com/facebook/rocksdb.git -b v5.17.2
$ mkdir ~/rocksdb/build && cd ~/rocksdb/build
$ cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DWITH_GFLAGS=OFF -DWITH_TESTS=OFF ..
$ make install
mac os의 경우 아래의 과정을 할 필요가 없음.
$ ln -s /usr/local/lib64/librocksdb.so.5 /usr/local/lib/librocksdb.so.5
```
### Set env for gorocksdb
```shell
$ echo 'export CGO_CFLAGS="-I/usr/local/include"' >> ~/.bash_profile
$ echo 'export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"' >> ~/.bash_profile
$ source ~/.bash_profile
```

### Install tendermint
```shell
$ cd $GOPATH/src/github.com/tendermint/tendermint
$ git checkout v0.30.0
$ make get_tools
$ make get_vendor_deps
$ make install
```

## Installation
### Install paust-db
```shell
$ go get github.com/paust-team/paust-db/cmd/paust-db
```

### Run
* run paust-db
```shell
$ paust-db master
```
* run tendermint
```shell
$ tendermint unsafe_reset_all
$ tendermint init
$ tendermint node
```

### Status
#### paust-db-client status
아래 Quick start guide 를 따라 paust-db-client 를 설치하여 status command 로 health check 가능
```shell
$ paust-db-client status -e localhost:26657
```

#### tm-monitor 
tm-monitor를 이용해 직접 구성한 node 들의 상태를 확인 할 수 있음

- [Install](https://github.com/tendermint/tendermint/tree/master/tools/tm-monitor)

- Run

```shell
usage: tm-monitor ip:port

$ tm-monitor localhost:26657
```
- Result
```
2019-02-20 20:13:14.50251 +0900 KST m=+0.074827320 up 100.00%

Height: 49
Avg block time: 1000.000 ms
Avg tx throughput: 0 per sec
Avg block latency: 0.154 ms
Active nodes: 1/1 (health: moderate) Validators: 1

NAME                HEIGHT     BLOCK LATENCY     ONLINE     VALIDATOR     
localhost:26657     49         0.221 ms          true       true  

```


## Quick start
### Install client cli
다음 명령어를 통해서 paust-db-client 를 install 하여 local 환경에서 cli 테스트를 할 수 있음 
자세한 cli 명령어는 [client](https://github.com/paust-team/paust-db/tree/master/client)에서 확인할 수 있음
```shell
$ go get github.com/paust-team/paust-db/client/cmd/paust-db-client
```

### Data 

Name|Description
---|---
timestamp | Unix timestamp(nanosec)
ownerKey | Base64 encoded ED2519 public key
qulifier | Schemeless json string
data | Base64 encoded data 


### Put
스트림을 이용한 Json data를 Put 하는 example. 
```shell
$ echo '[
        {"timestamp":1544772882435375000,"ownerKey":"NwdTf+S9+H5lsB6Us+s5Y1ChdB1aKECA6gsyGCa8SCM=","qualifier":"{\"userId\":\"paust_kevin\"}","data":"YWJj"},
        {"timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"{\"userId\":\"paust_andrew\"}","data":"ZGVm"},
        {"timestamp":1544772967331458000,"ownerKey":"aFw+o2z13LFCXzk7HptFoOY54s7VGDeQQVo32REPFCU=","qualifier":"{\"userId\":\"paust_elon\"}","data":"Z2hp"}
]' | paust-db-client put -s
Read json data from STDIN
put success.
```

### Query
Time range 사이의 ownerkey가 mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY= 이고, qualifier가 bWVt인 item을 Query하는 example
```
$ paust-db-client query 1544772882435375000 1544772967331458001 -o mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY= -q '{"userId":"paust_kevin"}'
query success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"ownerKey":"mnhKcUWnR1iYTm6o4SJ/X0FV67QFIytpLB03EmWM1CY=","qualifier":"{\"userId\":\"paust_kevin\"}"}]
```

### Fetch
Query를 통하여 받은 id인 eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0= 를 이용해 실제 data를 fetch하는 example
```shell
$ paust-db-client fetch eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=
Read data from cli arguments
fetch success.
[{"id":"eyJ0aW1lc3RhbXAiOjE1NDQ3NzI5NjAwNDkxNzcwMDAsInNhbHQiOjIxNX0=","timestamp":1544772960049177000,"data":"ZGVm"}]
```

## Docker Support
[Docker Guide](/docker/README.md)

- Single Node (for test)
- Multi-Node Clustering on Single Host (for test)
- Multi-Node Clustering


## Contributing
- Welcome PR
- Owner 명시(PAUST Inc.) www.paust.io

## License
- PaustDB(master) is GPLv3-style licensed, as found in the [LICENSE](LICENSE) file.
- PaustDB(client) is LGPL-style licensed, as found in the [LICENSE](/client/LICENSE) file.

## Third-party

* go(https://github.com/golang/go)
* tendermint(https://github.com/tendermint/tendermint)
* gorocksdb(https://github.com/tecbot/gorocksdb)
* testify(https://github.com/stretchr/testify)
* corbra(https://github.com/spf13/cobra)
* kit(https://github.com/go-kit/kit)
* logfmt(https://github.com/go-logfmt/logfmt)
* errors(https://github.com/pkg/errors)
