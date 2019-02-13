# Make docker image
build-image:
	docker build --no-cache -t paust-db ../.

# Init tendermint config
init:
	docker run --rm -v ${CURDIR}/build:/tendermint:Z paust-db init

# Config for Clustering
cluster:
	docker run --rm -v ${CURDIR}/build:/tendermint:Z paust-db testnet --o .

# Run a 4-node testnet locally
node-start: node-stop
	docker-compose up

# Stop testnet
node-stop:
	docker-compose down

