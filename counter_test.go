package bi

import (
	"testing"

	"github.com/golangplus/testing/assert"
)

func TestCounter_Count(t *testing.T) {
	assert.Equal(t, "Count()", (&Counter{Sum: 11, Div: 4}).Count(), 3)
	assert.Equal(t, "Count()", (&Counter{Sum: 11, Div: 1}).Count(), 11)
	assert.Equal(t, "Count()", (&Counter{Sum: 11, Div: 0}).Count(), 0)
}

func TestCounter_Append(t *testing.T) {
	tests := []struct {
		sum       int
		div       int
		aggr      AggregateMethod
		value     int
		final_sum int
		final_div int
	}{
		{sum: 11, div: 4, aggr: Sum, value: 2, final_sum: 5, final_div: 1},
		{sum: 11, div: 4, aggr: Min, value: 2, final_sum: 2, final_div: 1},
		{sum: 11, div: 4, aggr: Max, value: 5, final_sum: 5, final_div: 1},
		{sum: 11, div: 4, aggr: Average, value: 2, final_sum: 13, final_div: 5},
	}
	for _, test := range tests {
		t.Logf("test: %v, %v", test, test.aggr)
		c := Counter{
			Sum: test.sum,
			Div: test.div,
		}
		c.Append(test.aggr, test.value)
		assert.Equal(t, "sum", c.Sum, test.final_sum)
		assert.Equal(t, "div", c.Div, test.final_div)
	}
}

func TestAggregateMethod_String(t *testing.T) {
	assert.Equal(t, "String()", Sum.String(), "Sum")
	assert.Equal(t, "String()", Max.String(), "Max")
	assert.Equal(t, "String()", Min.String(), "Min")
	assert.Equal(t, "String()", Average.String(), "Average")
	assert.Equal(t, "String()", AggregateMethod(-1).String(), "-1(invalid)")
}
