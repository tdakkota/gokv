package test

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/go-test/deep"

	"github.com/philippgille/gokv"
)

// Foo is just some struct for common tests.
type Foo struct {
	Bar        string
	privateBar string
}

type privateFoo struct {
	Bar        string
	privateBar string
}

// TestStore tests if reading from, writing to and deleting from the store works properly.
// A struct is used as value. See TestTypes() for a test that is simpler but tests all types.
func TestStore(store gokv.Store, t *testing.T) {
	key := strconv.FormatInt(rand.Int63(), 10)

	// Initially the key shouldn't exist
	found, err := store.Get(key, new(Foo))
	if err != nil {
		t.Error(err)
	}
	if found {
		t.Error("A value was found, but no value was expected")
	}

	// Deleting a non-existing key-value pair should NOT lead to an error
	err = store.Delete(key)
	if err != nil {
		t.Error(err)
	}

	// Store an object
	val := Foo{
		Bar: "baz",
	}
	err = store.Set(key, val)
	if err != nil {
		t.Error(err)
	}

	// Retrieve the object
	expected := val
	actualPtr := new(Foo)
	found, err = store.Get(key, actualPtr)
	if err != nil {
		t.Error(err)
	}
	if !found {
		t.Error("No value was found, but should have been")
	}
	actual := *actualPtr
	if actual != expected {
		t.Errorf("Expected: %v, but was: %v", expected, actual)
	}

	// Delete
	err = store.Delete(key)
	if err != nil {
		t.Error(err)
	}
	// Key-value pair shouldn't exist anymore
	found, err = store.Get(key, new(Foo))
	if err != nil {
		t.Error(err)
	}
	if found {
		t.Error("A value was found, but no value was expected")
	}
}

// TestTypes tests if setting and getting values works with all Go types.
func TestTypes(store gokv.Store, t *testing.T) {
	boolVar := true
	// Omit byte
	// Omit error - it's a Go builtin type but marshalling and then unmarshalling doesn't lead to equal objects
	floatVar := 1.2
	intVar := 1
	runeVar := '⚡'
	stringVar := "foo"

	structVar := Foo{
		Bar: "baz",
	}
	privateStructVar := privateFoo{
		Bar: "baz",
	}

	sliceOfBool := []bool{true, false}
	sliceOfByte := []byte("foo")
	// Omit slice of float
	sliceOfInt := []int{1, 2}
	// Omit slice of rune
	sliceOfString := []string{"foo", "bar"}

	sliceOfSliceOfString := [][]string{[]string{"foo", "bar"}}

	sliceOfStruct := []Foo{Foo{Bar: "baz"}}
	sliceOfPrivateStruct := []privateFoo{privateFoo{Bar: "baz"}}

	testVals := []struct {
		subTestName string
		val         interface{}
		testGet     func(*testing.T, gokv.Store, string, interface{})
	}{
		{"bool", boolVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(bool)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"float", floatVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(float64)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"int", intVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(int)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"rune", runeVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(rune)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"string", stringVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(string)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"struct", structVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(Foo)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"private struct", privateStructVar, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new(privateFoo)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if actual != expected {
				t.Errorf("Expected: %v, but was: %v", expected, actual)
			}
		}},
		{"slice of bool", sliceOfBool, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([]bool)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
		{"slice of byte", sliceOfByte, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([]byte)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
		{"slice of int", sliceOfInt, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([]int)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
		{"slice of string", sliceOfString, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([]string)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
		{"slice of slice of string", sliceOfSliceOfString, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([][]string)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
		{"slice of struct", sliceOfStruct, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([]Foo)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
		{"slice of private struct", sliceOfPrivateStruct, func(t *testing.T, store gokv.Store, key string, expected interface{}) {
			actualPtr := new([]privateFoo)
			found, err := store.Get(key, actualPtr)
			handleGetError(t, err, found)
			actual := *actualPtr
			if diff := deep.Equal(actual, expected); diff != nil {
				t.Error(diff)
			}
		}},
	}

	for _, testVal := range testVals {
		t.Run(testVal.subTestName, func(t2 *testing.T) {
			key := strconv.FormatInt(rand.Int63(), 10)
			err := store.Set(key, testVal.val)
			if err != nil {
				t.Error(err)
			}
			testVal.testGet(t, store, key, testVal.val)
		})
	}
}

func handleGetError(t *testing.T, err error, found bool) {
	if err != nil {
		t.Error(err)
	}
	if !found {
		t.Error("No value was found, but should have been")
	}
}

// InteractWithStore reads from and writes to the DB. Meant to be executed in a goroutine.
// Does NOT check if the DB works correctly (that's done elsewhere),
// only checks for errors that might occur due to concurrent access.
func InteractWithStore(store gokv.Store, key string, t *testing.T, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	// Read
	_, err := store.Get(key, new(Foo))
	if err != nil {
		t.Error(err)
	}
	// Write
	err = store.Set(key, Foo{})
	if err != nil {
		t.Error(err)
	}
	// Read
	_, err = store.Get(key, new(Foo))
	if err != nil {
		t.Error(err)
	}
}