package client

// InputDataObj는 Put function의 write model.
// Timestamp는 unix timestamp이며 단위는 nano second임.
// OwnerId는 data owner id이며 64자리 미만 string
// Qualifier는 json object이며 string.
type InputDataObj struct {
	Timestamp uint64 `json:"timestamp"`
	OwnerId   string `json:"ownerId"`
	Qualifier string `json:"qualifier"`
	Data      []byte `json:"data"`
}

// InputQueryObj는 Query function의 read model.
// Start, End는 unix timestamp이며 단위는 nano second임.
// OwnerId는 data owner id이며 64자리 미만 string. OwnerId를 제한하고 싶지 않다면 empty string을 넣음.
// Qualifier는 json object이며 string. Qualifier를 제한하고 싶지 않다면 empty string을 넣음.
type InputQueryObj struct {
	Start     uint64 `json:"start"`
	End       uint64 `json:"end"`
	OwnerId   string `json:"ownerId"`
	Qualifier string `json:"qualifier"`
}

// InputFetchObj는 Fetch function의 read model.
// Id는 data의 고유한 id.
type InputFetchObj struct {
	Ids [][]byte `json:"ids"`
}

// OutputQueryObj는 Query function의 result data type.
// Id는 data의 고유한 id.
// Timestamp는 unix timestamp이며 단위는 nano second임.
// OwnerId는 data owner id이며 64자리 미만 string
// Qualifier는 json object이며 string.
type OutputQueryObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	OwnerId   string `json:"OwnerId"`
	Qualifier string `json:"qualifier"`
}

// OutputFetchObj는 Fetch function의 result data type.
// Id는 data의 고유한 id.
// Timestamp는 unix timestamp이며 단위는 nano second임.
type OutputFetchObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	Data      []byte `json:"data"`
}
