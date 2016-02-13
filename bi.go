package bi

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/golangplus/encoding/json"
	"github.com/golangplus/errors"

	"github.com/boltdb/bolt"
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
				if err := updateDB(func(tx *bolt.Tx) error {
					b, err := tx.CreateBucketIfNotExists([]byte("active"))
					if err != nil {
						return errorsp.WithStacks(err)
					}
					for name, items := range counters {
						for _, item := range items {
							ts := itob(uint64(item.when.UnixNano()))
							if err := b.Put(ts[:], jsonp.MarshalIgnoreError(kv{
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

type boltDBBox struct {
	sync.Mutex

	db    *bolt.DB
	count int
}

var gBoltDBBox boltDBBox

func (box *boltDBBox) alloc() (*bolt.DB, error) {
	box.Lock()
	defer box.Unlock()

	if box.db == nil {
		db, err := bolt.Open(DataPath, 0644, nil)
		if err != nil {
			return nil, err
		}
		box.db, box.count = db, 0
	}
	box.count++
	return box.db, nil
}

func (box *boltDBBox) free() {
	box.Lock()
	defer box.Unlock()

	box.count--
	if box.count == 0 {
		box.db.Close()
		box.db = nil
	}
}

func updateDB(f func(*bolt.Tx) error) error {
	db, err := gBoltDBBox.alloc()
	if err != nil {
		return err
	}
	defer gBoltDBBox.free()

	return db.Update(f)
}

func viewDB(f func(*bolt.Tx) error) error {
	db, err := gBoltDBBox.alloc()
	if err != nil {
		return err
	}
	defer gBoltDBBox.free()

	return db.View(f)
}

func ReadNames() ([]string, error) {
	var names []string
	if err := viewDB(func(tx *bolt.Tx) error {
		namesB := tx.Bucket([]byte("names"))
		if namesB == nil {
			return nil
		}
		if err := namesB.ForEach(func(k, _ []byte) error {
			if len(k) == 0 {
				// Ignore empty names.
				return nil
			}
			names = append(names, string(k))
			return nil
		}); err != nil {
			return err
		}
		return nil
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
	if err := viewDB(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tp))
		if b == nil {
			return nil
		}
		namedB := b.Bucket([]byte(name))
		if namedB == nil {
			return nil
		}
		if err := namedB.ForEach(func(k, v []byte) error {
			counters = append(counters, LabeledCounter{
				Label:   string(k),
				Counter: counterFromJSON(v),
			})
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return counters, nil
}

func MoveData(from, to string, aggr AggregateMethod) error {
	if from == to {
		return nil
	}
	return errorsp.WithStacks(updateDB(func(tx *bolt.Tx) error {
		namesB, err := tx.CreateBucketIfNotExists([]byte("names"))
		if err != nil {
			return err
		}
		namesB.Delete([]byte(from))
		namesB.Put([]byte(to), []byte{})

		for _, tp := range []string{Daily, Weekly, Monthly, Yearly} {
			b := tx.Bucket([]byte(tp))
			if b == nil {
				log.Printf("Backet of %v not found!", tp)
				return nil
			}
			fromB := b.Bucket([]byte(from))
			if fromB == nil {
				log.Printf("Backet of %v in %v not found!", from, tp)
				return nil
			}
			toB, err := b.CreateBucketIfNotExists([]byte(to))
			if err != nil {
				return errorsp.WithStacks(err)
			}
			if err := fromB.ForEach(func(k, fromV []byte) error {
				if err := fromB.Delete(k); err != nil {
					return errorsp.WithStacks(err)
				}
				toV := toB.Get(k)
				if toV == nil {
					return errorsp.WithStacks(toB.Put(k, fromV))
				}
				c := counterFromJSON(toV)
				c.append(aggr, counterFromJSON(fromV))
				return errorsp.WithStacks(toB.Put(k, c.ToJSON()))
			}); err != nil {
				return errorsp.WithStacks(err)
			}
		}
		return nil
	}))
}

func Process() {
	if err := updateDB(func(tx *bolt.Tx) error {
		activeB, err := tx.CreateBucketIfNotExists([]byte("active"))
		if err != nil {
			log.Printf("CreateBucketIfNotExists: %v", err)
			return err
		}
		namesB, err := tx.CreateBucketIfNotExists([]byte("names"))
		if err != nil {
			return err
		}
		if err := activeB.ForEach(func(k, v []byte) error {
			ts := int64(binary.BigEndian.Uint64(k))
			var kv kv
			if err := json.Unmarshal(v, &kv); err != nil {
				log.Printf("Unmarshal %v failed: %v", string(v), err)
				return err
			}
			name, value := kv.Key, kv.Value
			namesB.Put([]byte(name), []byte{})

			when := time.Unix(0, ts)

			appendValue := func(tp, label string) error {
				b, err := tx.CreateBucketIfNotExists([]byte(tp))
				if err != nil {
					return err
				}
				namedB, err := b.CreateBucketIfNotExists([]byte(name))
				label_bs := []byte(label)
				c := counterFromJSON(namedB.Get(label_bs))
				c.Append(kv.Aggr, value)
				if err := namedB.Put(label_bs, c.ToJSON()); err != nil {
					log.Printf("b.Put failed: %v", err)
					return err
				}
				return nil
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
			return activeB.Delete(k)
		}); err != nil {
			log.Printf("activeB.ForEach failed: %v", err)
			return err
		}
		return nil
	}); err != nil {
		log.Printf("updateDB failed: %v", err)
	}
}
