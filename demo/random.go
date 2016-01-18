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
		bi.AddValue(bi.Average, "average", value)
		bi.AddValue(bi.Max, "max", value)
		bi.AddValue(bi.Min, "min", value)
		bi.AddValue(bi.Sum, "sum", value)
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
