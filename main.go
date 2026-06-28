package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
)

const stateFile = "/data/state.json"

var mu sync.Mutex

// broker fans out state updates to all connected SSE clients.
var broker = newBroker()

type sseBroker struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func newBroker() *sseBroker {
	return &sseBroker{clients: make(map[chan []byte]struct{})}
}

func (b *sseBroker) subscribe() chan []byte {
	ch := make(chan []byte, 1)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *sseBroker) unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

func (b *sseBroker) broadcast(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- data:
		default:
		}
	}
}

func readState() ([]byte, error) {
	mu.Lock()
	defer mu.Unlock()
	data, err := os.ReadFile(stateFile)
	if os.IsNotExist(err) {
		return []byte("{}"), nil
	}
	return data, err
}

func writeState(body []byte) error {
	mu.Lock()
	defer mu.Unlock()
	return os.WriteFile(stateFile, body, 0644)
}

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

	http.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			data, err := readState()
			if err != nil {
				http.Error(w, "failed to read state", http.StatusInternalServerError)
				return
			}
			w.Write(data)
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			if err := writeState(body); err != nil {
				http.Error(w, "failed to write state", http.StatusInternalServerError)
				return
			}
			broker.broadcast(body)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := broker.subscribe()
		defer broker.unsubscribe(ch)

		for {
			select {
			case data := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", data)
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	fmt.Println("Listening on :" + port)
	http.ListenAndServe(":"+port, nil)
}
