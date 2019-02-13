#!/usr/bin/env sh

##
## Input parameters
##
TENDERMINT=$GOPATH/bin/tendermint
PAUSTDB=$GOPATH/bin/paust-db

$PAUSTDB master -d $TMHOME &

$TENDERMINT $@
