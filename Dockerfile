FROM golang:1.11.5-alpine3.9
MAINTAINER andrew <andrew.joo@paust.io>

#ENV for gorocksdb
ENV CGO_CFLAGS="-I/usr/local/include"
ENV CGO_LDFLAGS="-L/usr/local/lib -lrocksdb"

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
RUN cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DWITH_GFLAGS=OFF -DWITH_TESTS=OFF ..
RUN make install
RUN rm -rf /root/rocksdb

RUN ln -s /usr/local/lib64/librocksdb.so.5 /usr/local/lib/librocksdb.so.5

# install paust-db
RUN go get github.com/paust-team/paust-db/cmd/paust-db

# install tendermint v0.30.0
WORKDIR /go/src/github.com/tendermint/tendermint
RUN git checkout v0.30.0
RUN make get_tools
RUN make get_vendor_deps
RUN make install

FROM alpine:3.9

RUN apk update && apk add --no-cache libstdc++

COPY --from=0 /usr/local/lib64/librocksdb.so.5 /usr/local/lib64/
RUN ln -s /usr/local/lib64/librocksdb.so.5 /usr/local/lib/librocksdb.so.5
COPY --from=0 /go/bin/tendermint /usr/bin/
COPY --from=0 /go/bin/paust-db /usr/bin/

ENV TMHOME /tendermint
VOLUME [ $TMHOME ]
WORKDIR $TMHOME
EXPOSE 26656 26657
ENTRYPOINT ["/root/wrapper.sh"] 
CMD ["node", "--log_level *:error"]
STOPSIGNAL SIGTERM

COPY docker/wrapper.sh /root/wrapper.sh
