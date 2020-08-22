package redipo

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestStruct struct {
	Name string `json:"name"`
}

func TestReadWrite(t *testing.T) {

	uuid1 := uuid.New()

	manager := New()
	repo := manager.LoadRepo("testing")
	repo.SetFactory(func() interface{} { return &TestStruct{} })

	err := repo.Save(uuid1, &TestStruct{Name: "test1"})
	assert.Nil(t, err)

	result, err := repo.Get(uuid1)
	assert.Nil(t, err)
	assert.Equal(t, "test1", result.(*TestStruct).Name)

}

func TestListingDeleting(t *testing.T) {

	manager := New()
	repo := manager.LoadRepo("testing")
	repo.SetFactory(func() interface{} { return &TestStruct{} })

	list, err := repo.List()
	assert.Nil(t, err)
	assert.NotNil(t, list)
	fmt.Println(list)

	for _, key := range list {
		err = repo.Delete(key)
		assert.Nil(t, err)
	}

	list, err = repo.List()
	assert.Nil(t, err)
	assert.Zero(t, len(list))
}

func TestIndexing(t *testing.T) {

	uuid1 := uuid.New()

	manager := New()
	repo := manager.LoadRepo("testing")
	repo.SetFactory(func() interface{} { return &TestStruct{} })

	err := repo.AddToIndex("test_index", uuid1)
	assert.Nil(t, err)

	items, err := repo.GetIndex("test_index")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(items))
	assert.Equal(t, uuid1, items[0])

	err = repo.RemoveFromIndex("test_index", uuid1)
	assert.Nil(t, err)

	items, err = repo.GetIndex("test_index")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(items))
}
