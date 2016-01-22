package bi

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/golangplus/math"
)

type AggregateMethod int

func (a AggregateMethod) String() string {
	switch a {
	case Sum:
		return "Sum"
	case Min:
		return "Min"
	case Max:
		return "Max"
	case Average:
		return "Average"
	}
	return fmt.Sprintf("%d(invalid)", a)
}

const (
	Sum = AggregateMethod(iota)
	Min
	Max
	Average
)

type Counter struct {
	Sum int
	Div int
}

func (c *Counter) Count() int {
	if c.Div == 0 {
		return 0
	}
	if c.Div == 1 {
		return c.Sum
	}
	return (c.Sum + c.Div/2) / c.Div
}

func (c *Counter) Append(aggr AggregateMethod, v int) {
	switch aggr {
	case Sum:
		c.Sum, c.Div = c.Count()+v, 1
	case Min:
		c.Sum, c.Div = mathp.MinI(c.Count(), v), 1
	case Max:
		c.Sum, c.Div = mathp.MaxI(c.Count(), v), 1
	case Average:
		c.Sum, c.Div = c.Sum+v, c.Div+1
	}
}

func (c *Counter) append(aggr AggregateMethod, cc Counter) {
	switch aggr {
	case Sum:
		c.Sum, c.Div = c.Count()+cc.Count(), 1
	case Min:
		c.Sum, c.Div = mathp.MinI(c.Count(), cc.Count()), 1
	case Max:
		c.Sum, c.Div = mathp.MaxI(c.Count(), cc.Count()), 1
	case Average:
		c.Sum, c.Div = c.Sum+cc.Sum, c.Div+cc.Div
	}
}

func (c *Counter) ToJSON() []byte {
	j, _ := json.Marshal(c)
	return j
}

// Returns the zero value of Counter if parsing failed.
func counterFromJSON(j []byte) Counter {
	var c Counter
	if err := json.Unmarshal(j, &c); err != nil {
		if len(j) > 0 {
			log.Printf("Unmarshal %v failed: %v", j, err)
		}
		c = Counter{}
	}
	return c
}
