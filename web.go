package bi

import (
	"html/template"
	"log"
	"net/http"
)

var tmpl = template.Must(template.New("index").Parse(gIndexHtml))

func handler(w http.ResponseWriter, req *http.Request) {
	name := req.FormValue("name")
	tp := req.FormValue("type")
	var data []LabeledCounter
	if name != "" {
		var err error
		data, err = ReadDataOfName(tp, name)
		if err != nil {
			log.Printf("ReadDataOfName %v failed: %v", name, err)
		}
	}
	log.Printf("name: %v, data: %v", name, data)
	names, err := ReadNames()
	if err != nil {
		log.Printf("ReadNames failed: %v", err)
	}
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
	http.HandleFunc(path, handler)
}
