package util

import (
	"crypto/sha256"
	"encoding/hex"
)
func CalculateHash(inputs ...string) string {

	record := ""
	for _, item := range inputs {
		record += item
	}
	
	h := sha256.New()
	h.Write([]byte(record))
	hashed := h.Sum(nil)
	return hex.EncodeToString(hashed)
}