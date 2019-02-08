#!/usr/bin/env sh

##
## Input parameters
##
ID=${ID:-0}
LOG=${LOG:-tendermint.log}
TENDERMINT=$GOPATH/bin/tendermint
PAUSTDB=$GOPATH/bin/paust-db

export TMHOME="/tendermint/node${ID}"

nohup $PAUSTDB master -d $TMHOME & > "${TMHOME}/paust-db.log"

if [ -d "`dirname ${TMHOME}/${LOG}`" ]; then
  $TENDERMINT $@ | tee "${TMHOME}/${LOG}"
else
  $TENDERMINT $@
fi
