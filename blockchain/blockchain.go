package blockchain

import (
	"errors"
)
type Blockchain struct {
	chain []Block
}

func NewBlockchain() (*Blockchain, error) {
	genesisBlock := Genesis()
	newChain := []Block{*genesisBlock}

	return &Blockchain{
		chain: newChain,
	}, nil
}
func (blockchain *Blockchain) isValidBlock(newBlock, oldBlock Block) bool {

	if oldBlock.Index+1 != newBlock.Index {
		return false
	}

	if oldBlock.Hash != newBlock.PrevHash {
		return false
	}

	if newBlock.CalculateHash() != newBlock.Hash {
		return false
	}

	return true

}
func (blockchain *Blockchain) GetChain() []Block {
	return blockchain.chain
}
func (blockchain *Blockchain) Length() int {
	return len(blockchain.chain)
}
func (blockchain *Blockchain) LastBlock() Block {

	return blockchain.chain[blockchain.Length()-1]
}

func (blockchain *Blockchain) AddBlock(bpm int) error {
	prevBlock := blockchain.LastBlock()
	block := NewBlock(prevBlock.Index+1, bpm, prevBlock.Hash)

	if blockchain.isValidBlock(*block, prevBlock) {
		blockchain.chain = append(blockchain.chain, *block)
		return nil
	} else {
		return errors.New("Not valid block")
	}
}


