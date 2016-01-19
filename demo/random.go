package main

import (
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/daviddengcn/go-easybi"
)

func genValues() {
	for {
		time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
		value := int(rand.Int31n(10) + 1)
		bi.AddValue(bi.Average, "a.average", value)
		bi.AddValue(bi.Max, "a.max", value)
		bi.AddValue(bi.Min, "a.min", value)
		bi.AddValue(bi.Sum, "a.sum", value)
		value = int(rand.Int31n(10) + 1)
		bi.AddValue(bi.Average, "b.average", value)
		bi.AddValue(bi.Max, "b.max", value)
		bi.AddValue(bi.Min, "b.min", value)
		bi.AddValue(bi.Sum, "b.sum", value)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	rand.Seed(time.Now().UnixNano())

	bi.DataPath = "/Users/david/tmp/bi.bolt"
	bi.FlushPeriod = time.Second
	go func() {
		for {
			bi.Process()
			time.Sleep(time.Second)
		}
	}()
	go genValues()
	bi.HandleRequest("/stat")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
