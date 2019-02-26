# Docker
With Docker and Docker Compose, you can spin up local testnets with a single command

## Dockerfile
you can find paust-db Dockerfile in project root directory

## Requirements
- [Install docker](https://docs.docker.com/install)
- [Install docker-compose](https://docs.docker.com/compose/install/)

## Build paust-db docker image
```
cd $GOPATH/src/github.com/paust-team/paust-db/docker
make build-image
```
## How to use this image

### Single Node
for test 
```
docker run --rm -v /tmp:/tendermint:Z paust-db init
docker run -p "26656-26657":"26656-26657" --rm -v /tmp:/tendermint:Z paust-db 
```

### Multi-Node Clustering on Single Host
docker-compose를 이용해 4개의 local cluster 구성(bridge network로 통신) for test

```
cd $GOPATH/src/github.com/paust-team/paust-db/docker
make localnet-start
```

### Multi-Node Clustering
4개 혹은 그 이상의 host machine에서 각각을 validator로 하여 clustering을 구축할 수 있음

아래는 ubuntu 18.04환경에서 4개의 node clustering 구축하는 가이드임.

편의상 각각의 host machine을 node0, node1, node2, node3이라 명명하고 네트워크 주소를 ip0, ip1, ip2, ip3이라 명명 
#### Node별 초기 설정 및 genesis.json 공유
##### Node0
tendermint testnet command를 이용하여 cluster 설정 자동 생성
```shell
docker run --rm -v ~/build:/tendermint:Z paust-db testnet --v 4 --o /tendermint/cluster
```

~/build/cluster/node0/config/config.toml 수정

1. moniker field - "node0"으로 수정
2. persistent_peers field 수정

persistent_peers는 node_id@ip:port의 형식을 가지는데 초기 설정에서 이미 node_id와 port를 설정함

그러므로 ip 부분만 node0 -> ip0, node1 -> ip1, node2 -> ip2, node3 -> ip3으로 수정

~/build/cluster/node1/config/config.toml 수정
1. moniker field - "node1"으로 수정
2. node0과 동일하게 persistent_peers수정

~/build/cluster/node2/config/config.toml 수정
1. moniker field - "node2"으로 수정
2. node0과 동일하게 persistent_peers수정

~/build/cluster/node3/config/config.toml 수정
1. moniker field - "node3"으로 수정
2. node0과 동일하게 persistent_peers수정

 
```shell
sudo rm -rf ~/build/node0
sudo cp -r ~/build/cluster/node0 ~/build/node0
sudo ssh account@ip1 "mkdir -p ~/build" && sudo scp -r ~/build/cluster/node1 account@ip1:~/build/node0
sudo ssh account@ip2 "mkdir -p ~/build" && sudo scp -r ~/build/cluster/node2 account@ip2:~/build/node0
sudo ssh account@ip3 "mkdir -p ~/build" && sudo scp -r ~/build/cluster/node3 account@ip3:~/build/node0
sudo rm -rf ~/build/cluster
```
#### Run paust-db
##### Node0 
```shell
docker run --rm -p "26656-26657":"26656-26657" --name node0 -v ~/build:/tendermint:Z paust-db
```

##### Node1
```shell
docker run --rm -p "26656-26657":"26656-26657" --name node1 -v ~/build:/tendermint:Z paust-db
```

##### Node2
```shell
docker run --rm -p "26656-26657":"26656-26657" --name node2 -v ~/build:/tendermint:Z paust-db
```

##### Node3
```shell
docker run --rm -p "26656-26657":"26656-26657" --name node3 -v ~/build:/tendermint:Z paust-db
```

#### Non-validator Node 추가
##### Node4
- 초기 설정 생성
```shell
docker run --rm -v ~/build:/tendermint:Z paust-db init
```
- 초기 구축 node에서 genesis.json 파일 얻기
```shell
curl ip0:26657/genesis | jq .result.genesis > ~/build/node0/config/genesis.json
```
- 통신할 seed(초기 구축 node)를 추가

~/build/node0/config/config.toml 파일의 seeds field를 "node_id@ip0:26656" 형식으로 추가

node_id는 해당 노드의 config.toml의 persistent_peers(node_id@ip:port)에서 얻을 수 있음

- Run paust-db on node4 as a non-validator
```shell
docker run --rm -p "26656-26657":"26656-26657" --name node4 -v ~/build:/tendermint:Z paust-db 
```
