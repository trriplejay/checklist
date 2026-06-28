package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
)

const (
	stateFile = "/data/state.json"
	itemsFile = "/data/items.json"
)

var fileMu sync.Mutex

type sseEvent struct {
	name string
	data []byte
}

type sseBroker struct {
	mu      sync.Mutex
	clients map[chan sseEvent]struct{}
}

func newBroker() *sseBroker {
	return &sseBroker{clients: make(map[chan sseEvent]struct{})}
}

func (b *sseBroker) subscribe() chan sseEvent {
	ch := make(chan sseEvent, 1)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *sseBroker) unsubscribe(ch chan sseEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
}

func (b *sseBroker) broadcast(name string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- sseEvent{name: name, data: data}:
		default:
		}
	}
}

var broker = newBroker()

func readFile(path string, missing []byte) ([]byte, error) {
	fileMu.Lock()
	defer fileMu.Unlock()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return missing, nil
	}
	return data, err
}

func writeFile(path string, body []byte) error {
	fileMu.Lock()
	defer fileMu.Unlock()
	return os.WriteFile(path, body, 0644)
}

func jsonEndpoint(w http.ResponseWriter, r *http.Request, path string, eventName string, missing []byte) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		data, err := readFile(path, missing)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		w.Write(data)
	case http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := writeFile(path, body); err != nil {
			http.Error(w, "write error", http.StatusInternalServerError)
			return
		}
		broker.broadcast(eventName, body)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
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
		jsonEndpoint(w, r, stateFile, "state", []byte("{}"))
	})

	http.HandleFunc("/items", func(w http.ResponseWriter, r *http.Request) {
		jsonEndpoint(w, r, itemsFile, "items", []byte("null"))
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := broker.subscribe()
		defer broker.unsubscribe(ch)

		for {
			select {
			case evt := <-ch:
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.name, evt.data)
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	})

	fmt.Println("Listening on :" + port)
	http.ListenAndServe(":"+port, nil)
}
