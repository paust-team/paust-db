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
docker run --rm -v ~/build:/tendermint:Z paust-db init
docker run -p "26656-26657":"26656-26657" --rm -v /tmp:/tendermint:Z paust-db 
```

### Multi-Node Clustering on Single Host
docker-compose를 이용해 4개의 local cluster 구성(bridge network로 통신) for test

```
cd $GOPATH/src/github.com/paust-team/paust-db/docker
make localnet-start
```

### Multi-Node Clustering

