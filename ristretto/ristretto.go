package ristretto

import (
	"errors"
	"github.com/dgraph-io/ristretto"

	"github.com/philippgille/gokv/encoding"
	"github.com/philippgille/gokv/util"
)

var ErrPairIsNotAdded = errors.New("key-value item isn't added to the cache")
var ErrFailedToCast = errors.New("runtime sucks, type assertion failed")

// Store is a gokv.Store implementation for Ristretto.
type Store struct {
	db    *ristretto.Cache
	codec encoding.Codec
}

// Set stores the given value for the given key.
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// The key must not be "" and the value must not be nil.
func (s Store) Set(k string, v interface{}) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	data, err := s.codec.Marshal(v)
	if err != nil {
		return err
	}

	if !s.db.Set(k, data, 0) {
		return ErrPairIsNotAdded
	}
	return nil
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (s Store) Get(k string, v interface{}) (found bool, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	var data []byte
	value, ok := s.db.Get(k)

	if !ok || value == nil {
		return false, nil
	}

	if data, ok = value.([]byte); !ok {
		return false, ErrFailedToCast
	}

	return true, s.codec.Unmarshal(data, v)
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (s Store) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	s.db.Del(k)
	return nil
}

// Close closes the store.
// It must be called to make sure that all open transactions finish and to release all DB resources.
func (s Store) Close() error {
	s.db.Close()
	return nil
}

// Options are the options for the Risetto store.
type Options struct {
	// NumCounters determines the number of counters (keys) to keep that hold
	// access frequency information. It's generally a good idea to have more
	// counters than the max cache capacity, as this will improve eviction
	// accuracy and subsequent hit ratios.
	// Optional (1000 by default).
	NumCounters int64
	// MaxCost can be considered as the cache capacity, in whatever units you
	// choose to use.
	// Optional (100 by default).
	MaxCost int64
	// BufferItems determines the size of Get buffers.
	BufferItems int64
	// Metrics determines whether cache statistics are kept during the cache's
	// lifetime. There *is* some overhead to keeping statistics, so you should
	// only set this flag to true when testing or throughput performance isn't a
	// major factor.
	Metrics bool
	// Encoding format.
	// Optional (encoding.JSON by default).
	Codec encoding.Codec
}

// DefaultOptions is an Options object with default values.
// Codec: encoding.JSON
var DefaultOptions = Options{
	NumCounters: 1000,
	MaxCost:     100,
	BufferItems: 64,
	Metrics:     false,
	Codec:       encoding.JSON,
}

// NewStore creates a new Ristretto store.
//
// You must call the Close() method on the store when you're done working with it.
func NewStore(options Options) (Store, error) {
	result := Store{}

	// Set default values
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}
	if options.NumCounters == options.NumCounters {
		options.NumCounters = DefaultOptions.NumCounters
	}
	if options.MaxCost == options.MaxCost {
		options.MaxCost = DefaultOptions.MaxCost
	}
	if options.BufferItems == options.BufferItems {
		options.BufferItems = DefaultOptions.BufferItems
	}

	// Open DB
	db, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: options.NumCounters,
		MaxCost:     options.MaxCost,
		BufferItems: options.BufferItems,
		Metrics:     options.Metrics,
	})
	if err != nil {
		return result, err
	}

	result.db = db
	result.codec = options.Codec

	return result, nil
}
