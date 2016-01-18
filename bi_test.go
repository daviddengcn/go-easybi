package bi

import (
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golangplus/testing/assert"
)

func TestSimpleFlow(t *testing.T) {
	DataPath = "./tmp.bolt"
	FlushPeriod = time.Second
	err := os.RemoveAll(DataPath)
	if err != nil && err != os.ErrNotExist {
		assert.NoError(t, err)
		return
	}

	now := time.Now()
	AddValue(Average, "abc", 123)
	Flush()
	Process()
	assert.Equal(t, "gBoltDBBox.count", gBoltDBBox.count, 0)
	assert.Equal(t, "gBoltDBBox.db", gBoltDBBox.db, (*bolt.DB)(nil))

	names, err := ReadNames()
	assert.NoError(t, err)
	assert.Equal(t, "names", names, []string{"abc"})

	data, err := ReadDataOfName("daily", "abc")
	assert.NoError(t, err)
	assert.Equal(t, "data", data, []LabeledCounter{{
		Counter{
			Sum: 123,
			Div: 1,
		},
		now.Format("2006-01-02"),
	}})

	// Try update
	AddValue(Max, "abc", 1000)
	Flush()
	Process()
	data, err = ReadDataOfName("daily", "abc")
	assert.NoError(t, err)
	assert.Equal(t, "data", data, []LabeledCounter{{
		Counter{
			Sum: 1000,
			Div: 1,
		},
		now.Format("2006-01-02"),
	}})

	// Test of auto flusing
	AddValue(Average, "def", 456)
	time.Sleep(time.Second * 2)
	Process()

	names, err = ReadNames()
	assert.NoError(t, err)
	assert.Equal(t, "names", names, []string{"abc", "def"})

	data, err = ReadDataOfName("monthly", "def")
	assert.NoError(t, err)
	assert.Equal(t, "data", data, []LabeledCounter{{
		Counter: Counter{
			Sum: 456,
			Div: 1,
		},
		Label: now.Format("2006-01"),
	}})
}
