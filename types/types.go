package types

type Data struct {
	Timestamp int64 `json:"timestamp"`
	Data []byte `json:"data"`
}

type BetweenQuery struct {
	Start int64 `json:"start"`
	Stop int64 `json:"stop"`
}