FROM golang:1.10-alpine3.8
MAINTAINER andrew <andrew.joo@paust.io>

#ENV for gorocksdb
ENV CGO_CFLAGS "-I/usr/local/rocksdb/include"
ENV CGO_LDFLAGS="-L/usr/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

RUN apk add --no-cache git build-base cmake bash perl linux-headers
RUN apk add --no-cache zlib zlib-dev bzip2 bzip2-dev snappy snappy-dev lz4 lz4-dev zstd zstd-dev libc6-compat

RUN apk update && \
    apk upgrade && \
    apk add --no-cache curl jq libstdc++

# install Rocksdb
WORKDIR /root
RUN git clone https://github.com/facebook/rocksdb.git -b v5.17.2
RUN mkdir /root/rocksdb/build
WORKDIR /root/rocksdb/build
RUN cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local/rocksdb -DWITH_GFLAGS=OFF -DWITH_TESTS=OFF ..
RUN make install
RUN rm -rf /root/rocksdb

RUN ln -s /usr/local/rocksdb/lib64/librocksdb.a /usr/lib/librocksdb.a

# install paust-db
COPY . /go/src/github.com/paust-team/paust-db
WORKDIR /go/src/github.com/paust-team/paust-db
RUN go get ./...
RUN go install ./...

# install tendermint v0.27.4
WORKDIR /go/src/github.com/tendermint/tendermint
RUN git checkout v0.27.4
RUN make get_tools
RUN make get_vendor_deps
RUN make install

ENV TMHOME /tendermint
VOLUME [ $TMHOME ]
WORKDIR $TMHOME
EXPOSE 26656 26657
ENTRYPOINT ["/usr/bin/wrapper.sh"] 
CMD ["node", "--log_level *:error"]
STOPSIGNAL SIGTERM

COPY docker/wrapper.sh /usr/bin/wrapper.sh
