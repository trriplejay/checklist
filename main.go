package main

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

var dataDir = envOr("DATA_DIR", "/data")

// In dev mode (DEV=1), every page gets a script that reloads it when the
// server restarts, which `make dev` does whenever a source file changes.
var devMode = os.Getenv("DEV") == "1"

const devReloadScript = `<script>
(function() {
  let wasDown = false;
  function connect() {
    const es = new EventSource("/dev-reload");
    es.onopen = function() { if (wasDown) location.reload(); };
    es.onerror = function() { wasDown = true; es.close(); setTimeout(connect, 300); };
  }
  connect();
})();
</script>
`

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Each checklist is served from /<slug>-checklist, with its API at
// /<slug>-state, /<slug>-items and /<slug>-events, and its data stored in
// <dataDir>/<slug>-state.json and <dataDir>/<slug>-items.json.
//
// Its page is templates/checklist.html, filled in with the fields below and
// with the trip's note and default items from checklists/<slug>.html.
type checklist struct {
	Slug     string
	Title    string
	Subtitle string
	Flag     string

	// Accent colours for light and dark mode.
	Accent, AccentBg, AccentText             string
	AccentDark, AccentBgDark, AccentTextDark string
}

func (c checklist) IconURL() template.URL { return iconURL(c.Accent) }

// iconURL returns the check-mark favicon as a data URL, in the given colour.
func iconURL(color string) template.URL {
	return template.URL("data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'>" +
		"<rect width='100' height='100' rx='18' fill='" + strings.Replace(color, "#", "%23", 1) + "'/>" +
		"<path d='M22 52 L42 72 L78 30' stroke='white' stroke-width='11' stroke-linecap='round' stroke-linejoin='round' fill='none'/></svg>")
}

// AccentStyle sets a home page card's accent colours.
func (c checklist) AccentStyle() template.CSS {
	return template.CSS("--accent:" + c.Accent + ";--accent-bg:" + c.AccentBg +
		";--accent-dark:" + c.AccentDark + ";--accent-bg-dark:" + c.AccentBgDark)
}

var checklists = []checklist{
	{
		Slug: "china", Title: "China family trip", Subtitle: "Shanghai — June 30 – July 6",
		Flag:   "🇨🇳",
		Accent: "#d4581c", AccentBg: "#fdf0e9", AccentText: "#b84510",
		AccentDark: "#e8703a", AccentBgDark: "#2a1a10", AccentTextDark: "#f08555",
	},
	{
		Slug: "victoria", Title: "Victoria, BC family trip", Subtitle: "2-day hotel getaway — 2 adults, 2 kids — pool included",
		Flag:   "🇨🇦",
		Accent: "#1565c0", AccentBg: "#e8f0fb", AccentText: "#0d4a94",
		AccentDark: "#4a90e2", AccentBgDark: "#0f1e2e", AccentTextDark: "#7ab0f0",
	},
}

type homePage struct {
	Title      string
	IconURL    template.URL
	Checklists []checklist
}

var homeTmpl = template.Must(template.ParseFiles("templates/base.html", "templates/home.html"))

// render executes a page's "base" template and writes the result.
func render(w http.ResponseWriter, t *template.Template, data any) {
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "base", data); err != nil {
		fmt.Println("render: " + err.Error())
		http.Error(w, "render error", http.StatusInternalServerError)
		return
	}
	writeHTML(w, buf.Bytes())
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

// writeHTML writes a page, adding the dev reload script in dev mode.
func writeHTML(w http.ResponseWriter, html []byte) {
	w.Header().Set("Content-Type", "text/html")
	if devMode {
		w.Header().Set("Cache-Control", "no-store")
		html = []byte(strings.Replace(string(html), "</body>", devReloadScript+"</body>", 1))
	}
	w.Write(html)
}

func registerChecklist(c checklist) {
	b := newBroker()
	t := template.Must(template.ParseFiles("templates/base.html", "templates/checklist.html", "checklists/"+c.Slug+".html"))
	stateFile := dataDir + "/" + c.Slug + "-state.json"
	itemsFile := dataDir + "/" + c.Slug + "-items.json"

	http.HandleFunc("/"+c.Slug+"-checklist", func(w http.ResponseWriter, r *http.Request) {
		render(w, t, c)
	})

	http.HandleFunc("/"+c.Slug+"-state", func(w http.ResponseWriter, r *http.Request) {
		jsonEndpoint(w, r, stateFile, "state", []byte("{}"), b)
	})

	http.HandleFunc("/"+c.Slug+"-items", func(w http.ResponseWriter, r *http.Request) {
		jsonEndpoint(w, r, itemsFile, "items", []byte("null"), b)
	})

	http.HandleFunc("/"+c.Slug+"-events", sseHandler(b))
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// The China checklist's data files used to be un-prefixed.
	migrateLegacyFile(dataDir+"/state.json", dataDir+"/china-state.json")
	migrateLegacyFile(dataDir+"/items.json", dataDir+"/china-items.json")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		render(w, homeTmpl, homePage{Title: "checklist", IconURL: iconURL("#1a1916"), Checklists: checklists})
	})

	if devMode {
		// Held open until the server stops; the page reloads on reconnect.
		http.HandleFunc("/dev-reload", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		})
	}

	for _, c := range checklists {
		registerChecklist(c)
	}

	fmt.Println("Listening on :" + port)
	http.ListenAndServe(":"+port, nil)
}
