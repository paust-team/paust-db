package db

import "github.com/tecbot/gorocksdb"

// DBs are goroutine safe.
type DB interface {
	// Get value from specific ColumnFamily
	GetDataFromColumnFamily(index int, key []byte) (*gorocksdb.Slice, error)

	// Set value In specific ColumnFamily
	SetDataInColumnFamily(index int, key, value []byte) error

	// Specific Column Family Iterator
	IteratorColumnFamily(start, end []byte, cf *gorocksdb.ColumnFamilyHandle) Iterator

	// Creates a batch for atomic updates.
	NewBatch() Batch

	// Get all ColumnFamily handles which return slice of *columnFamilyHandle
	ColumnFamilyHandles() gorocksdb.ColumnFamilyHandles

	// Iterate over a domain of keys in ascending order. End is exclusive.
	// Start must be less than end, or the Iterator is invalid.
	// A nil start is interpreted as an empty byteslice.
	// If end is nil, iterates up to the last item (inclusive).
	// CONTRACT: No writes may happen within a domain while an iterator exists over it.
	// CONTRACT: start, end readonly []byte
	Iterator(start, end []byte) Iterator

	// Closes the connection.
	Close()
}

//----------------------------------------
// Batch

type Batch interface {
	SetColumnFamily(cf *gorocksdb.ColumnFamilyHandle, key, value []byte)
	Write() (int, error)
}

//----------------------------------------
// Iterator
type Iterator interface {
	Valid() bool

	Next()

	Key() (key []byte)

	Value() (value []byte)

	Close()

	Seek(key []byte)

	assertNoError()

	assertIsValid()
}
