package bi

import (
	"html/template"
	"log"
	"net/http"
	"strings"
)

var (
	tmpl     = template.Must(template.New("index").Parse(gIndexHtml))
	allTypes = []string{
		Daily, Weekly, Monthly, Yearly,
	}
)

func chooseDefault(vs []string, v string) string {
	for _, vv := range vs {
		if vv == v {
			return v
		}
	}
	return vs[0]
}

type prefixSubs struct {
	Prefix   string
	Included bool // True if the Prefix itself is avalid name
	Subs     []prefixSubs
}

func prefixSubsFromParts(parts []string) prefixSubs {
	ps := prefixSubs{
		Prefix:   parts[0],
		Included: len(parts) == 1,
	}
	if len(parts) > 1 {
		ps.Subs = []prefixSubs{prefixSubsFromParts(parts[1:])}
	}
	return ps
}

func (ps *prefixSubs) insert(subs []string) {
	if len(ps.Subs) == 0 || ps.Subs[len(ps.Subs)-1].Prefix != subs[0] {
		// A new prefix, add an item to ps.Subs
		ps.Subs = append(ps.Subs, prefixSubsFromParts(subs))
		return
	}
	ps.Subs[len(ps.Subs)-1].insert(subs[1:])
}

func organizeNames(names []string) []prefixSubs {
	var res prefixSubs
	for _, name := range names {
		res.insert(strings.SplitN(name, ".", 3))
	}
	return res.Subs
}

func dataDiff(data []LabeledCounter) []LabeledCounter {
	if len(data) < 2 {
		return nil
	}
	diff := make([]LabeledCounter, 0, len(data)-1)
	for i := 1; i < len(data); i++ {
		lc := data[i]
		lc.Append(Sum, -data[i-1].Count())
		diff = append(diff, lc)
	}
	return diff
}

func handler(w http.ResponseWriter, req *http.Request) {
	names, err := ReadNames()
	if err != nil {
		log.Printf("ReadNames failed: %v", err)
	}
	reqName, reqTp := req.FormValue("name"), req.FormValue("type")
	name, tp := chooseDefault(names, reqName), chooseDefault(allTypes, reqTp)
	if reqName != name || reqTp != tp {
		q := req.URL.Query()
		q["name"] = []string{name}
		q["type"] = []string{tp}
		u := *req.URL
		u.RawQuery = q.Encode()
		http.Redirect(w, req, u.String(), http.StatusMovedPermanently)
		return
	}
	var data []LabeledCounter
	if name != "" {
		var err error
		data, err = ReadDataOfName(tp, name)
		if err != nil {
			log.Printf("ReadDataOfName %v failed: %v", name, err)
		}
		if req.FormValue("diff") != "" {
			data = dataDiff(data)
		}
	}
	log.Printf("name: %v, data: %v", name, data)
	organizedNames := organizeNames(names)
	if err := tmpl.Execute(w, struct {
		Names []prefixSubs
		Name  string
		Type  string
		Data  []LabeledCounter
	}{
		Names: organizedNames,
		Name:  name,
		Type:  tp,
		Data:  data,
	}); err != nil {
		log.Printf("Execute failed: %v", err)
	}
}

func HandleRequest(path string) {
	log.Println("Easy-bi handle request at " + path)
	http.HandleFunc(path, handler)
}
