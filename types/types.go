package types

//TODO offset 추가
type KeyObj struct {
	Timestamp []byte `json:"timestamp"`
	Salt      []byte `json:"salt"`
}

type MetaDataObj struct {
	RowKey    []byte `json:"rowKey"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type RealDataObj struct {
	RowKey []byte `json:"rowKey"`
	Data   []byte `json:"data"`
}

type BaseDataObj struct {
	MetaData MetaDataObj `json:"meta"`
	RealData RealDataObj `json:"real"`
}

type QueryObj struct {
	Start     []byte `json:"start"`
	End       []byte `json:"end"`
	OwnerKey  []byte `json:"ownerKey"`
	Qualifier []byte `json:"qualifier"`
}

type FetchObj struct {
	RowKeys [][]byte `json:"rowKeys"`
}
