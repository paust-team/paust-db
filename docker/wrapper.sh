#!/usr/bin/env sh

##
## Input parameters
##
ID=${ID:-0}
LOG=${LOG:-tendermint.log}
TENDERMINT=/usr/bin/tendermint
PAUSTDB=/usr/bin/paust-db

export TMHOME="/tendermint/node${ID}"

$PAUSTDB master -d $TMHOME &

$TENDERMINT $@
