package nutsdb

import (
	"github.com/xujiajun/nutsdb"

	"github.com/philippgille/gokv/encoding"
	"github.com/philippgille/gokv/util"
)

// Store is a gokv.Store implementation for NutsDB.
type Store struct {
	db         *nutsdb.DB
	bucketName string
	codec      encoding.Codec
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

	err = s.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(s.bucketName, []byte(k), data, 0)
	})

	if err != nil {
		return err
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
	err = s.db.View(func(tx *nutsdb.Tx) error {
		e, err := tx.Get(s.bucketName, []byte(k))
		if err != nil {
			return err
		}

		if e.Value != nil {
			data = make([]byte, len(e.Value))
			copy(data, e.Value)
		}
		return nil
	})
	if err != nil {
		return false, nil
	}

	// If no value was found return false
	if data == nil {
		return false, nil
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

	return s.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(s.bucketName, []byte(k))
	})
}

// Close closes the store.
// It must be called to make sure that all open transactions finish and to release all DB resources.
func (s Store) Close() error {
	return s.db.Close()
}

// Options are the options for the nuts store.
type Options struct {
	// Bucket name for storing the key-value pairs.
	// Optional ("default" by default).
	BucketName string
	// Path of the DB file.
	// Optional ("nuts.db" by default).
	Path string
	// if Sync is false, high write performance but potential data loss likely.
	// if Sync is true, slower but persistent.
	// Optional (false by default).
	Sync bool
	// NodeNum represents the node number.
	// NodeNum range [1,1023].
	// Optional (1 by default).
	NodeNum int64
	// Encoding format.
	// Optional (encoding.JSON by default).
	Codec encoding.Codec
}

// DefaultOptions is an Options object with default values.
// BucketName: "default", Path: "nuts.db", Codec: encoding.JSON
var DefaultOptions = Options{
	BucketName: "default",
	Path:       "nuts.db",
	Sync:       false,
	NodeNum:    1,
	Codec:      encoding.JSON,
}

// NewStore creates a new NutsDB store.
//
// You must call the Close() method on the store when you're done working with it.
func NewStore(options Options) (Store, error) {
	result := Store{}

	// Set default values
	if options.BucketName == "" {
		options.BucketName = DefaultOptions.BucketName
	}
	if options.Path == "" {
		options.Path = DefaultOptions.Path
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}
	if options.NodeNum == 0 {
		options.NodeNum = DefaultOptions.NodeNum
	}

	ops := nutsdb.DefaultOptions
	ops.Dir = options.Path
	ops.SyncEnable = options.Sync
	ops.NodeNum = options.NodeNum

	// Open DB
	db, err := nutsdb.Open(ops)
	if err != nil {
		return result, err
	}

	result.db = db
	result.bucketName = options.BucketName
	result.codec = options.Codec

	return result, nil
}
