package consts

//Data Length관련 상수
const (
	OwnerKeyLen = 32
)

//ColumnFamily위치 관련 상수
const (
	DefaultCFNum = iota
	MetaCFNum
	RealCFNum
	TotalCFNum
)

//Server, Client config 공통 상수
const (
	QueryPath = "/query"
	FetchPath = "/fetch"
)

//Client config 상수
const (
	Remote     = "http://localhost:26657"
	WsEndpoint = "/websocket"
)

//Server config 상수
const (
	ProtoAddr = "0.0.0.0:26658"
	Transport = "socket"
	DBName    = "paustdb"
)
