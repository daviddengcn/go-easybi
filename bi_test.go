package bi

import (
	"fmt"
	"os"
	"testing"
	"time"

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

func TestInc(t *testing.T) {
	DataPath = "./tmp.bolt"
	FlushPeriod = time.Second
	err := os.RemoveAll(DataPath)
	if err != nil && err != os.ErrNotExist {
		assert.NoError(t, err)
		return
	}

	now := time.Now()
	Inc("abc")
	Flush()
	Process()

	names, err := ReadNames()
	assert.NoError(t, err)
	assert.Equal(t, "names", names, []string{"abc"})

	data, err := ReadDataOfName("daily", "abc")
	assert.NoError(t, err)
	assert.Equal(t, "data", data, []LabeledCounter{{
		Counter{
			Sum: 1,
			Div: 1,
		},
		now.Format("2006-01-02"),
	}})
}

func TestMoveData(t *testing.T) {
	DataPath = "./tmp.bolt"
	FlushPeriod = time.Second
	err := os.RemoveAll(DataPath)
	if err != nil && err != os.ErrNotExist {
		assert.NoError(t, err)
		return
	}

	now := time.Now()
	checkValue := func(tp, name string, sum, div int) {
		data, err := ReadDataOfName(tp, name)
		assert.NoError(t, err)
		if sum == 0 && div == 0 {
			assert.Equal(t, fmt.Sprintf("%v:%v", name, tp), len(data), 0)
			return
		}
		assert.Equal(t, fmt.Sprintf("%v:%v", name, tp), data, []LabeledCounter{{
			Counter{Sum: sum, Div: div},
			now.Format("2006-01-02"),
		}})
	}
	checkNames := func(exp []string) {
		names, err := ReadNames()
		assert.NoError(t, err)
		assert.Equal(t, "names", names, exp)
	}

	AddValue(Average, "TestMoveData-abc", 123)
	AddValue(Average, "TestMoveData-def", 456)
	Flush()
	Process()

	t.Log("before move")
	checkNames([]string{"TestMoveData-abc", "TestMoveData-def"})
	checkValue(Daily, "TestMoveData-abc", 123, 1)
	checkValue(Daily, "TestMoveData-def", 456, 1)

	assert.NoError(t, MoveData("TestMoveData-abc", "TestMoveData-def", Average))
	t.Log("after move from abc to def")
	checkNames([]string{"TestMoveData-def"})
	checkValue(Daily, "TestMoveData-abc", 0, 0)
	checkValue(Daily, "TestMoveData-def", 579, 2)

	assert.NoError(t, MoveData("TestMoveData-def", "TestMoveData-none", Average))
	t.Log("after move from def to none")
	checkNames([]string{"TestMoveData-none"})
	checkValue(Daily, "TestMoveData-abc", 0, 0)
	checkValue(Daily, "TestMoveData-def", 0, 0)
	checkValue(Daily, "TestMoveData-none", 579, 2)
}
