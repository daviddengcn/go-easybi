package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/golangplus/fmt"

	"github.com/daviddengcn/go-easybi"
)

func usage(action string) {
	if action == "" {
		fmtp.Eprintfln("Usage of %s:\n", os.Args[0])
	}
	if action == "" || action == "movevalues" {
		fmtp.Eprintfln(" bi movevalues <src-name> <dst-name> <aggr>")
	}
	if action == "" || action == "readnames" {
		fmtp.Eprintfln(" bi readnames")
	}
	if action == "" || action == "showvalues" {
		fmtp.Eprintfln(" bi showvalues <name> <type>")
	}
	flag.PrintDefaults()
	os.Exit(1)
}

var (
	dataPath = flag.String("datapath", "", "Path to the data file.")
)

func checkAndSetDataPath() {
	if *dataPath == "" {
		fmtp.Eprintfln("Please specify data path with -datapath flag")
		os.Exit(1)
	}
	bi.DataPath = *dataPath
}

var nameToAggr = map[string]bi.AggregateMethod{
	"max":     bi.Max,
	"min":     bi.Min,
	"sum":     bi.Sum,
	"average": bi.Average,
}

func doMoveValues() {
	if flag.NArg() < 3 {
		usage("movevalues")
	}
	checkAndSetDataPath()

	aggr, ok := nameToAggr[strings.ToLower(flag.Arg(2))]
	if !ok {
		fmtp.Eprintfln("Unknown aggregation method: %v", flag.Arg(2))
		usage("movevalues")
	}

	src, dst := flag.Arg(0), flag.Arg(1)
	if err := bi.MoveData(src, dst, aggr); err != nil {
		log.Fatalf("MoveData %v to %v with %v failed: %v", src, dst, aggr, err)
	}
}

func doReadNames() {
	checkAndSetDataPath()
	names, err := bi.ReadNames()
	if err != nil {
		log.Fatalf("ReadNames failed: %v", err)
	}
	fmtp.Printfln("Names: %v", names)
}

func doShowValues() {
	checkAndSetDataPath()
	if flag.NArg() < 2 {
		usage("showvalues")
	}
	name, tp := flag.Args()[0], flag.Args()[1]
	vs, err := bi.ReadDataOfName(tp, name)
	if err != nil {
		log.Fatalf("ReadDataOfName(%v, %v) failed: %v", tp, name, err)
	}
	for _, v := range vs {
		fmtp.Printfln("%v -> %v", v.Label, v.Count())
	}
}

func main() {
	flag.Usage = func() {
		usage("")
	}
	if len(os.Args) < 2 {
		flag.Usage()
	}
	action := os.Args[1]
	os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
	flag.Parse()

	switch action {
	case "movevalues":
		doMoveValues()
	case "readnames":
		doReadNames()
	case "showvalues":
		doShowValues()
	default:
		fmtp.Eprintfln("Unknown action: %v\n", action)
		flag.Usage()
	}
}