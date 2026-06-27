package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<!DOCTYPE html><html><body><a href="/checklist">Checklist</a></body></html>`)
	})
	http.HandleFunc("/checklist", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "china-packing-checklist.html")
	})
	fmt.Println("Listening on :" + port)
	http.ListenAndServe(":"+port, nil)
}
