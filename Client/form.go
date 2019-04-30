package client

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"
)

type ContactDetails struct {
	Email   string
	Subject string
	Message string
}

func ClientServer(port string, reqChan, rreqChan chan string) {
	tmpl := template.Must(template.ParseFiles("../Client/forms.html"))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			tmpl.Execute(w, nil)
			return
		}

		details := ContactDetails{
			Message: r.FormValue("password"),
		}

		// do something with details
		fmt.Println(details)
		//remove \n if any
		details.Message = strings.Join(strings.Split(details.Message, "\n"), "")

		reqChan <- details.Message
		x := <-rreqChan
		fmt.Println("Client Server: ", x)
		tmpl.Execute(w, struct{ Success bool }{true})
	})
	fmt.Println("Client server running on port: " + port)
	http.ListenAndServe(":"+port, nil)
}
