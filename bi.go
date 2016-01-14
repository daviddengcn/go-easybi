package bi

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"path"
	"sync"
	"time"

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
	value int
	when  time.Time
}

type valueItem struct {
	name string
	timedValue
}

var (
	gCounterChan    = make(chan valueItem, 1000)
	gForceFlushChan = make(chan chan struct{})
)

func itob(v uint64) (b [8]byte) {
	binary.BigEndian.PutUint64(b[:], v)
	return
}

type kv struct {
	Key   string
	Value int
}

func collectValues(ch chan valueItem) {
	nextFlushTime := time.Now().Add(FlushPeriod)
	counters := make(map[string][]timedValue)
	flush := func() {
		if len(counters) > 0 {
			func() {
				db, err := gBoltDBBox.alloc()
				if err != nil {
					return
				}
				defer gBoltDBBox.free()

				if err := db.Update(func(tx *bolt.Tx) error {
					b, err := tx.CreateBucketIfNotExists([]byte("active"))
					if err != nil {
						log.Printf("CreateBucketIfNotExists: %v", err)
						return err
					}
					for name, items := range counters {
						for _, item := range items {
							ts := itob(uint64(item.when.UnixNano()))
							kv, _ := json.Marshal(kv{
								Key:   name,
								Value: item.value,
							})
							if err := b.Put(ts[:], kv); err != nil {
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
		case done := <-gForceFlushChan:
			flush()
			done <- struct{}{}
		case <-time.After(dueToFlush):
			flush()
		case item := <-ch:
			counters[item.name] = append(counters[item.name], item.timedValue)
		}
	}
}

func init() {
	go collectValues(gCounterChan)
}

func AddValue(name string, value int) {
	gCounterChan <- valueItem{
		name: name,
		timedValue: timedValue{
			value: value,
			when:  time.Now().AddDate(0, 0, -rand.Intn(1000)),
		},
	}
}

func Flush() {
	done := make(chan struct{})
	gForceFlushChan <- done
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
		db, err := bolt.Open(path.Join(DataPath, "bi.bolt"), 0644, nil)
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

func ReadNames() ([]string, error) {
	db, err := gBoltDBBox.alloc()
	if err != nil {
		return nil, err
	}
	defer gBoltDBBox.free()

	var names []string
	if err := db.View(func(tx *bolt.Tx) error {
		namesB := tx.Bucket([]byte("names"))
		if namesB == nil {
			return nil
		}
		if err := namesB.ForEach(func(k, _ []byte) error {
			names = append(names, string(k))
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return names, err
	}
	return names, nil
}

type Counter struct {
	Sum int
	Div int
}

func (c *Counter) Count() int {
	if c.Div == 0 {
		return 0
	}
	return (c.Sum + c.Div/2) / c.Div
}

func (c *Counter) Append(v int) {
	c.Sum += v
	c.Div++
}

func (c *Counter) ToJSON() []byte {
	j, _ := json.Marshal(c)
	return j
}

// Returns the zero value of Counter if parsing failed.
func CounterFromJSON(j []byte) Counter {
	var c Counter
	if err := json.Unmarshal(j, &c); err != nil {
		log.Printf("Parsing JSON %v failed: %v, the zero value used", string(j), err)
		c = Counter{}
	}
	return c
}

type LabeledCounter struct {
	Counter
	Label string
}

func ReadDataOfName(tp, name string) ([]LabeledCounter, error) {
	db, err := gBoltDBBox.alloc()
	if err != nil {
		return nil, err
	}
	defer gBoltDBBox.free()

	var counters []LabeledCounter
	if err := db.View(func(tx *bolt.Tx) error {
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
				Counter: CounterFromJSON(v),
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

func Process() {
	db, err := gBoltDBBox.alloc()
	if err != nil {
		return
	}
	defer gBoltDBBox.free()

	if err := db.Update(func(tx *bolt.Tx) error {
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
				c := CounterFromJSON(namedB.Get(label_bs))
				c.Append(value)
				if err := namedB.Put(label_bs, c.ToJSON()); err != nil {
					log.Printf("b.Put failed: %v", err)
					return err
				}
				log.Printf("date: %v, counter: %+v", label, c)
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
		log.Printf("db.Update failed: %v", err)
	}
}