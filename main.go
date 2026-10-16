package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
)

const dataDir = "/data"

// Each checklist is served from /<slug>-checklist, with its API at
// /<slug>-state, /<slug>-items and /<slug>-events, and its data stored in
// <dataDir>/<slug>-state.json and <dataDir>/<slug>-items.json.
type checklist struct {
	slug  string
	title string
	html  string
}

var checklists = []checklist{
	{slug: "china", title: "China trip checklist", html: "china-packing-checklist.html"},
	{slug: "victoria", title: "Victoria BC trip checklist", html: "victoria-packing-checklist.html"},
}

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

func jsonEndpoint(w http.ResponseWriter, r *http.Request, path string, eventName string, missing []byte, b *sseBroker) {
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
		b.broadcast(eventName, body)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func sseHandler(b *sseBroker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := b.subscribe()
		defer b.unsubscribe(ch)

		for {
			select {
			case evt := <-ch:
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.name, evt.data)
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	}
}

// migrateLegacyFile moves a data file from its old un-prefixed name to its
// new slug-prefixed name, unless the new file already exists.
func migrateLegacyFile(oldPath, newPath string) {
	if _, err := os.Stat(newPath); err == nil {
		return
	}
	if _, err := os.Stat(oldPath); err != nil {
		return
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		fmt.Println("migrate " + oldPath + ": " + err.Error())
		return
	}
	fmt.Println("migrated " + oldPath + " -> " + newPath)
}

func registerChecklist(c checklist) {
	b := newBroker()
	stateFile := dataDir + "/" + c.slug + "-state.json"
	itemsFile := dataDir + "/" + c.slug + "-items.json"

	http.HandleFunc("/"+c.slug+"-checklist", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, c.html)
	})

	http.HandleFunc("/"+c.slug+"-state", func(w http.ResponseWriter, r *http.Request) {
		jsonEndpoint(w, r, stateFile, "state", []byte("{}"), b)
	})

	http.HandleFunc("/"+c.slug+"-items", func(w http.ResponseWriter, r *http.Request) {
		jsonEndpoint(w, r, itemsFile, "items", []byte("null"), b)
	})

	http.HandleFunc("/"+c.slug+"-events", sseHandler(b))
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// The China checklist's data files used to be un-prefixed.
	migrateLegacyFile(dataDir+"/state.json", dataDir+"/china-state.json")
	migrateLegacyFile(dataDir+"/items.json", dataDir+"/china-items.json")

	index := `<!DOCTYPE html><html><body>`
	for _, c := range checklists {
		index += `<a href="/` + c.slug + `-checklist">` + c.title + `</a><br>`
	}
	index += `</body></html>`

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, index)
	})

	for _, c := range checklists {
		registerChecklist(c)
	}

	fmt.Println("Listening on :" + port)
	http.ListenAndServe(":"+port, nil)
}
