package client

// InputDataObj는 Put function의 write model.
// Timestamp는 unix timestamp이며 단위는 nano second임.
// OwnerKey는 ed25519 public key이며 32byte.
// Qualifier는 json object이며 string.
type InputDataObj struct {
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier string `json:"qualifier"`
	Data      []byte `json:"data"`
}

// InputFetchObj는 Fetch function의 read model.
// Id는 data의 고유한 id.
type InputFetchObj struct {
	Ids [][]byte `json:"ids"`
}

// OutputQueryObj는 Query function의 result data type.
// Id는 data의 고유한 id.
// Timestamp는 unix timestamp이며 단위는 nano second임.
// OwnerKey는 ed25519 public key이며 32byte.
// Qualifier는 json object이며 string.
type OutputQueryObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
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
