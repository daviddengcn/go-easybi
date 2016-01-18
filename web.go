package bi

import (
	"html/template"
	"log"
	"net/http"
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
		http.Redirect(w, req, u.String(), 301)
		return
	}
	var data []LabeledCounter
	if name != "" {
		var err error
		data, err = ReadDataOfName(tp, name)
		if err != nil {
			log.Printf("ReadDataOfName %v failed: %v", name, err)
		}
	}
	log.Printf("name: %v, data: %v", name, data)
	if err := tmpl.Execute(w, struct {
		Names []string
		Name  string
		Type  string
		Data  []LabeledCounter
	}{
		Names: names,
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
