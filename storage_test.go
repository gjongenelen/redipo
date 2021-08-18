package redipo

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	Name string `json:"name"`
}

func clearRepo(repo RepoInterface) {
	keys, _ := repo.List()
	for _, key := range keys {
		repo.Delete(key)
	}
}

func TestReadWrite(t *testing.T) {

	uuid1 := uuid.New()

	manager := New()
	repo := manager.LoadRepo("testing")
	repo.SetFactory(func() interface{} { return &TestStruct{} })
	clearRepo(repo)

	err := repo.Save(uuid1, &TestStruct{Name: "test1"})
	assert.Nil(t, err)

	result, err := repo.Get(uuid1)
	assert.Nil(t, err)
	assert.Equal(t, "test1", result.(*TestStruct).Name)

}

func TestListReadWrite(t *testing.T) {

	uuid1 := uuid.New()
	uuid2 := uuid.New()

	manager := New()
	repo := manager.LoadRepo("testing")
	repo.SetFactory(func() interface{} { return &TestStruct{} })
	clearRepo(repo)

	err := repo.Save(uuid1, &TestStruct{Name: "test1"})
	assert.Nil(t, err)
	err = repo.Save(uuid2, &TestStruct{Name: "test2"})
	assert.Nil(t, err)

	results, err := repo.GetAll()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(results))

	assert.Equal(t, "test", results[0].(*TestStruct).Name[0:4])
	assert.Equal(t, "test", results[1].(*TestStruct).Name[0:4])

}

func TestListingDeleting(t *testing.T) {

	manager := New()
	repo := manager.LoadRepo("testing")
	repo.SetFactory(func() interface{} { return &TestStruct{} })
	clearRepo(repo)

	err := repo.Save(uuid.New(), &TestStruct{Name: "test1"})
	assert.Nil(t, err)

	list, err := repo.List()
	assert.Nil(t, err)
	assert.NotNil(t, list)
	assert.Equal(t, 1, len(list))

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
	clearRepo(repo)

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
