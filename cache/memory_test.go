package cache

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestReadWrite(t *testing.T) {

	cacher := NewMemoryCache()
	uuid1 := uuid.New()

	err := cacher.Set(uuid1.String(), "name1")
	assert.Nil(t, err)

	result, err := cacher.Get(uuid1.String())
	assert.Nil(t, err)
	assert.Equal(t, "name1", result)

	err = cacher.Delete(uuid1.String())
	assert.Nil(t, err)

	result, err = cacher.Get(uuid1.String())
	assert.Nil(t, err)
	assert.Equal(t, "", result)

}

func TestExpiration(t *testing.T) {

	cacher := NewMemoryCache().SetExpiration(1 * time.Second)
	uuid1 := uuid.New()

	err := cacher.Set(uuid1.String(), "name1")
	assert.Nil(t, err)

	result, err := cacher.Get(uuid1.String())
	assert.Nil(t, err)
	assert.Equal(t, "name1", result)

	time.Sleep(2 * time.Second)

	result, err = cacher.Get(uuid1.String())
	assert.Nil(t, err)
	assert.Equal(t, "", result)

}
