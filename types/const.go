package types

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

//Server config 상수
const (
	ProtoAddr = "0.0.0.0:26658"
	Transport = "socket"
	DBName = "paustdb"
)