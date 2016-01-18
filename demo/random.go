package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/daviddengcn/go-easybi"
)

func genValues() {
	for {
		time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
		bi.AddValue(fmt.Sprintf("current-%c", 'a'+rune(rand.Int31n(26))), int(rand.Int31n(10)+1))
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
