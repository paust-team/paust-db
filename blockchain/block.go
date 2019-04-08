package blockchain
import (
	"time"
	"github.com/elon0823/paust-db/util"
	"strconv"
)
type Block struct {
	Index     int
	Timestamp uint64
	BPM       int
	Hash      string
	PrevHash  string
}
func NewBlock(index int, bpm int, prevHash string) (*Block) {

	block := Block{
		Index: index,
		Timestamp: uint64(time.Now().UnixNano()),
		BPM: bpm,
		Hash: "",
		PrevHash: prevHash,
	}
	block.Hash = block.CalculateHash()

	return &block
}

func Genesis() (*Block) {
	return NewBlock(0, 0, "")
}

func (block *Block) CalculateHash() string {

	return util.CalculateHash(strconv.Itoa(block.Index), 
		strconv.FormatUint(block.Timestamp, 10), strconv.Itoa(block.BPM), 
		block.PrevHash)
}