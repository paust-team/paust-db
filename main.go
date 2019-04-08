package main
import (
	"time"
	wbservice "github.com/elon0823/paust-db/service"
)

func main() {
	
	webserver, error := wbservice.NewWebServer("localhost","3000",10 * time.Second,1 << 20)

	if (error == nil) {
		webserver.Run()
	}

}