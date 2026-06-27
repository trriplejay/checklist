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
	http.Handle("/", http.FileServer(http.Dir(".")))
	fmt.Println("Listening on :" + port)
	http.ListenAndServe(":"+port, nil)
}
