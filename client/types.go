package client

type InputDataObj struct {
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
	Data      []byte `json:"data"`
}

type InputQueryObj struct {
	Ids [][]byte `json:"ids"`
}

type OutputMetaDataObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type OutputRealDataObj struct {
	Id        []byte `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	Data      []byte `json:"data"`
}
