package bi

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golangplus/bytes"
	"github.com/golangplus/encoding/json"
	"github.com/golangplus/errors"

	"github.com/daviddengcn/bolthelper"
)

var (
	DataPath    = "/tmp/bi.bolt"
	FlushPeriod = time.Minute
)

const (
	Daily   = "daily"
	Weekly  = "weekly"
	Monthly = "monthly"
	Yearly  = "yearly"
)

const (
	aWeek = 7 * 24 * time.Hour
)

type timedValue struct {
	aggr  AggregateMethod
	value int
	when  time.Time
}

type valueItem struct {
	name string
	timedValue
}

var (
	gCounterChan = make(chan interface{}, 1000)
)

func itob(v uint64) (b [8]byte) {
	binary.BigEndian.PutUint64(b[:], v)
	return
}

type kv struct {
	Aggr  AggregateMethod
	Key   string
	Value int
}

func collectValues() {
	nextFlushTime := time.Now().Add(FlushPeriod)
	counters := make(map[string][]timedValue)
	flush := func() {
		if len(counters) > 0 {
			func() {
				if err := updateDB(func(tx bh.Tx) error {
					b, err := tx.CreateBucketIfNotExists([][]byte{[]byte("active")})
					if err != nil {
						return errorsp.WithStacks(err)
					}
					for name, items := range counters {
						for _, item := range items {
							ts := itob(uint64(item.when.UnixNano()))
							if err := b.Put([][]byte{ts[:]}, jsonp.MarshalIgnoreError(kv{
								Aggr:  item.aggr,
								Key:   name,
								Value: item.value,
							})); err != nil {
								log.Printf("Put failed: %v", err)
							}
						}
					}
					return nil
				}); err != nil {
					log.Printf("db.Update failed: %v", err)
				}
			}()

			counters = make(map[string][]timedValue)
		}
		nextFlushTime = time.Now().Add(FlushPeriod)
	}
	for {
		dueToFlush := nextFlushTime.Sub(time.Now())
		if dueToFlush < 0 {
			flush()
			continue
		}
		select {
		case <-time.After(dueToFlush):
			flush()
		case item := <-gCounterChan:
			switch it := item.(type) {
			case valueItem:
				counters[it.name] = append(counters[it.name], it.timedValue)
			case chan struct{}:
				flush()
				it <- struct{}{}
			}
		}
	}
}

func init() {
	go collectValues()
}

func AddValue(aggr AggregateMethod, name string, value int) {
	gCounterChan <- valueItem{
		name: name,
		timedValue: timedValue{
			aggr:  aggr,
			value: value,
			when:  time.Now(),
		},
	}
}

func Inc(name string) {
	AddValue(Sum, name, 1)
}

func Flush() {
	done := make(chan struct{})
	gCounterChan <- done
	<-done
}

var gBoltDBBox = bh.RefCountBox{
	DataPath: func() string { return DataPath },
}

func updateDB(f func(bh.Tx) error) error {
	db, err := gBoltDBBox.Alloc()
	if err != nil {
		return err
	}
	defer gBoltDBBox.Free()

	return db.Update(f)
}

func viewDB(f func(bh.Tx) error) error {
	db, err := gBoltDBBox.Alloc()
	if err != nil {
		return err
	}
	defer gBoltDBBox.Free()

	return db.View(f)
}

func ReadNames() ([]string, error) {
	var names []string
	if err := viewDB(func(tx bh.Tx) error {
		return tx.ForEach([][]byte{[]byte("names")}, func(_ bh.Bucket, k bytesp.Slice, _ bytesp.Slice) error {
			if len(k) == 0 {
				// Ignore empty names.
				return nil
			}
			names = append(names, string(k))
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return names, nil
}

type LabeledCounter struct {
	Counter
	Label string
}

func ReadDataOfName(tp, name string) ([]LabeledCounter, error) {
	var counters []LabeledCounter
	if err := viewDB(func(tx bh.Tx) error {
		return tx.ForEach([][]byte{[]byte(tp), []byte(name)}, func(_ bh.Bucket, k bytesp.Slice, v bytesp.Slice) error {
			counters = append(counters, LabeledCounter{
				Label:   string(k),
				Counter: counterFromJSON(v),
			})
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return counters, nil
}

func MoveData(from, to string, aggr AggregateMethod) error {
	if from == to {
		return nil
	}
	return errorsp.WithStacks(updateDB(func(tx bh.Tx) error {
		if err := tx.Delete([][]byte{[]byte("names"), []byte(from)}); err != nil {
			return err
		}
		if err := tx.Put([][]byte{[]byte("names"), []byte(to)}, []byte{}); err != nil {
			return err
		}

		for _, tp := range []string{Daily, Weekly, Monthly, Yearly} {
			toB, err := tx.CreateBucketIfNotExists([][]byte{[]byte(tp), []byte(to)})
			if err != nil {
				return err
			}
			if err := tx.ForEach([][]byte{[]byte(tp), []byte(from)}, func(fromB bh.Bucket, k bytesp.Slice, fromV bytesp.Slice) error {
				if err := fromB.Delete([][]byte{k}); err != nil {
					return err
				}
				toV := toB.Get(k)
				if toV == nil {
					return toB.Put([][]byte{k}, fromV)
				}
				c := counterFromJSON(toV)
				c.append(aggr, counterFromJSON(fromV))
				return toB.Put([][]byte{k}, c.ToJSON())
			}); err != nil {
				return err
			}
		}
		return nil
	}))
}

func Process() {
	if err := updateDB(func(tx bh.Tx) error {
		return tx.ForEach([][]byte{[]byte("active")}, func(activeB bh.Bucket, k bytesp.Slice, v bytesp.Slice) error {
			ts := int64(binary.BigEndian.Uint64(k))
			var kv kv
			if err := json.Unmarshal(v, &kv); err != nil {
				return err
			}
			name, value := kv.Key, kv.Value
			tx.Put([][]byte{[]byte("names"), []byte(name)}, []byte{})

			when := time.Unix(0, ts)

			appendValue := func(tp, label string) error {
				return tx.Update([][]byte{[]byte(tp), []byte(name), []byte(label)}, func(v bytesp.Slice) (bytesp.Slice, error) {
					c := counterFromJSON(v)
					c.Append(kv.Aggr, value)
					return c.ToJSON(), nil
				})
			}
			if err := appendValue(Daily, when.Format("2006-01-02")); err != nil {
				return err
			}
			week_start := when.Truncate(aWeek)
			week_label := fmt.Sprintf("%s~%s", week_start.Format("2006-01-02"), week_start.Add(aWeek).Format("2006-01-02"))
			if err := appendValue(Weekly, week_label); err != nil {
				return err
			}
			if err := appendValue(Monthly, when.Format("2006-01")); err != nil {
				return err
			}
			if err := appendValue(Yearly, when.Format("2006")); err != nil {
				return err
			}
			return activeB.Delete([][]byte{k})
		})
	}); err != nil {
		log.Printf("updateDB failed: %v", err)
	}
}
