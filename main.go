package main
import (
	"time"
	wbservice "github.com/elon0823/paust-db/service"
	BC "github.com/elon0823/paust-db/blockchain"
	P2P "github.com/elon0823/paust-db/p2p"
	"flag"

)

func main() {

	host := flag.String("h", "127.0.0.1", "host to connect")
	listenF := flag.String("l", "10000", "wait for incoming connections")
	target := flag.String("d", "", "target peer to dial")
	secio := flag.Bool("secio", false, "enable secio")
	seed := flag.Int64("seed", 0, "set random seed for id generation")
	flag.Parse()

	blockchain, _ := BC.NewBlockchain()
	
	p2pManager, error := P2P.NewP2PManager(blockchain, *host, *listenF, *secio, *seed)
	if (error == nil) {
		p2pManager.Run(*target)
	}

	webserver, error := wbservice.NewWebServer(blockchain, "localhost","3000",10 * time.Second,1 << 20)
	
	if (error == nil) {
		webserver.Run()
	}

	

}