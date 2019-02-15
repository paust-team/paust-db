# Docker
With Docker and Docker Compose, you can spin up local testnets with a single command

## Dockerfile
you can find paust-db Dockerfile in project root directory

## Requirements
- Install docker
- Install docker-compose

## How to use this image
### Start one instance
```
docker run --rm -v /tmp:/tendermint:Z paust-db init
docker run -p "26656-26657":"26656-26657" --rm -v /tmp:/tendermint:Z paust-db 
```
### Local Cluster
docker-compose를 이용해 4개의 local cluster 구성(bridge network로 통신)
```
cd $GOPATH/src/github.com/paust-team/paust-db/docker
make build-image
make localnet-start
```
rocksdb build로 인해 image-build가 오래 걸릴 수 있음.

## Deployment
https://tendermint.com/docs/networks/terraform-and-ansible.html#ansible