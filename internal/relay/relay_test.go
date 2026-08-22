package relay

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/karen-m361/sse-relay/internal/hub"
)

func TestValidStreamID(t *testing.T) {
	cases := map[string]bool{
		"":                       false,
		"a":                      true,
		"Chat_123.log-1":         true,
		"has a space":            false,
		"has/slash":              false,
		strings.Repeat("a", 128): true,
		strings.Repeat("a", 129): false,
	}
	for id, want := range cases {
		if got := validStreamID(id); got != want {
			t.Errorf("validStreamID(%q) = %v, want %v", id, got, want)
		}
	}
}

func TestLastEventID(t *testing.T) {
	r := httptest.NewRequest("GET", "/streams/x/events", nil)
	if got := lastEventID(r); got != 0 {
		t.Fatalf("no header or query: got %d, want 0", got)
	}

	r = httptest.NewRequest("GET", "/streams/x/events?last_event_id=7", nil)
	if got := lastEventID(r); got != 7 {
		t.Fatalf("query fallback: got %d, want 7", got)
	}

	r = httptest.NewRequest("GET", "/streams/x/events?last_event_id=7", nil)
	r.Header.Set("Last-Event-ID", "42")
	if got := lastEventID(r); got != 42 {
		t.Fatalf("header takes priority: got %d, want 42", got)
	}

	r = httptest.NewRequest("GET", "/streams/x/events", nil)
	r.Header.Set("Last-Event-ID", "not-a-number")
	if got := lastEventID(r); got != 0 {
		t.Fatalf("invalid header: got %d, want 0", got)
	}
}

func TestWriteEventSplitsMultilineData(t *testing.T) {
	var buf strings.Builder
	writeEvent(&buf, hub.Event{ID: 5, Data: "line one\r\nline two\nline three"})

	want := "id: 5\ndata: line one\ndata: line two\ndata: line three\n\n"
	if got := buf.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAuthorized(t *testing.T) {
	s := &Server{cfg: Config{}}
	r := httptest.NewRequest("POST", "/streams/x", nil)
	if !s.authorized(r) {
		t.Fatal("no token configured: every request should be authorized")
	}

	s = &Server{cfg: Config{Token: "secret"}}
	r = httptest.NewRequest("POST", "/streams/x", nil)
	if s.authorized(r) {
		t.Fatal("token configured but no header sent: should be unauthorized")
	}

	r.Header.Set("Authorization", "Bearer wrong")
	if s.authorized(r) {
		t.Fatal("wrong token: should be unauthorized")
	}

	r.Header.Set("Authorization", "Bearer secret")
	if !s.authorized(r) {
		t.Fatal("correct bearer token: should be authorized")
	}
}

func newTestServer(cfg Config) (*Server, *hub.Hub) {
	h := hub.New(0)
	return NewServer(h, cfg), h
}

func doRequest(t *testing.T, s *Server, method, path, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	return rec
}

func TestHandlePublishRawBody(t *testing.T) {
	s, h := newTestServer(Config{})

	rec := doRequest(t, s, "POST", "/streams/chat-1", "hello", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %q", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	stream, ok := h.Stream("chat-1")
	if !ok {
		t.Fatal("publish did not create the stream")
	}
	if stats := stream.Stats(); stats.Events != 1 {
		t.Fatalf("events = %d, want 1", stats.Events)
	}
}

func TestHandlePublishJSONBody(t *testing.T) {
	s, h := newTestServer(Config{})

	body, err := json.Marshal(publishRequest{Data: "hi", Done: true})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	rec := doRequest(t, s, "POST", "/streams/chat-2", string(body), "application/json")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %q", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	stream, ok := h.Stream("chat-2")
	if !ok {
		t.Fatal("publish did not create the stream")
	}
	if stats := stream.Stats(); !stats.Done || stats.Events != 1 {
		t.Fatalf("stats = %+v, want one event and done", stats)
	}
}

func TestHandlePublishRejectsInvalidStreamID(t *testing.T) {
	s, _ := newTestServer(Config{})

	rec := doRequest(t, s, "POST", "/streams/has%20space", "hi", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlePublishRejectsEmptyChunk(t *testing.T) {
	s, _ := newTestServer(Config{})

	rec := doRequest(t, s, "POST", "/streams/chat-3", "", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandlePublishRequiresBearerToken(t *testing.T) {
	s, _ := newTestServer(Config{Token: "secret"})

	rec := doRequest(t, s, "POST", "/streams/chat-4", "hi", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	r := httptest.NewRequest("POST", "/streams/chat-4", strings.NewReader("hi"))
	r.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("correct token: status = %d, want %d, body %q", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestHandleFinishAndDelete(t *testing.T) {
	s, h := newTestServer(Config{})
	h.GetOrCreate("chat-5")

	rec := doRequest(t, s, "POST", "/streams/chat-5/done", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("finish: status = %d, want %d, body %q", rec.Code, http.StatusOK, rec.Body.String())
	}
	stream, _ := h.Stream("chat-5")
	if !stream.Done() {
		t.Fatal("finish did not mark the stream done")
	}

	rec = doRequest(t, s, "DELETE", "/streams/chat-5", "", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if _, ok := h.Stream("chat-5"); ok {
		t.Fatal("stream still present after delete")
	}

	rec = doRequest(t, s, "POST", "/streams/missing/done", "", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("finish missing: status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	rec = doRequest(t, s, "DELETE", "/streams/missing", "", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("delete missing: status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleStatsAndListAndHealth(t *testing.T) {
	s, h := newTestServer(Config{})
	stream := h.GetOrCreate("chat-6")
	if _, err := stream.Publish("a"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rec := doRequest(t, s, "GET", "/streams/chat-6", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("stats: status = %d, want %d", rec.Code, http.StatusOK)
	}
	var stats hub.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Events != 1 {
		t.Fatalf("stats.Events = %d, want 1", stats.Events)
	}

	rec = doRequest(t, s, "GET", "/streams", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: status = %d, want %d", rec.Code, http.StatusOK)
	}
	var list struct {
		Streams []hub.Stats `json:"streams"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Streams) != 1 || list.Streams[0].ID != "chat-6" {
		t.Fatalf("list = %+v, want one entry for chat-6", list.Streams)
	}

	rec = doRequest(t, s, "GET", "/healthz", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("health: status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("health body = %q, missing status ok", rec.Body.String())
	}
}

func TestHandleEventsUnknownStream(t *testing.T) {
	s, _ := newTestServer(Config{})

	rec := doRequest(t, s, "GET", "/streams/missing/events", "", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// A finished stream lets handleEvents run to completion without blocking on
// live events, which is what makes these framing tests safe to run without a
// context deadline or goroutine.
func TestHandleEventsReplaysThenSendsDone(t *testing.T) {
	s, h := newTestServer(Config{})
	stream := h.GetOrCreate("chat-7")
	if _, err := stream.Publish("a"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := stream.Publish("b"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	stream.Finish()

	rec := doRequest(t, s, "GET", "/streams/chat-7/events", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	want := "retry: 2000\n\n" +
		"id: 1\ndata: a\n\n" +
		"id: 2\ndata: b\n\n" +
		"event: done\ndata: {}\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
}

func TestHandleEventsResumesFromLastEventID(t *testing.T) {
	s, h := newTestServer(Config{})
	stream := h.GetOrCreate("chat-8")
	for _, data := range []string{"a", "b", "c"} {
		if _, err := stream.Publish(data); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	stream.Finish()

	r := httptest.NewRequest("GET", "/streams/chat-8/events", nil)
	r.Header.Set("Last-Event-ID", "1")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)

	want := "retry: 2000\n\n" +
		"id: 2\ndata: b\n\n" +
		"id: 3\ndata: c\n\n" +
		"event: done\ndata: {}\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHandleEventsReportsGapWhenBufferEvicted(t *testing.T) {
	h := hub.New(1)
	s := NewServer(h, Config{})
	stream := h.GetOrCreate("chat-9")
	for _, data := range []string{"a", "b", "c"} {
		if _, err := stream.Publish(data); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	stream.Finish()

	r := httptest.NewRequest("GET", "/streams/chat-9/events", nil)
	r.Header.Set("Last-Event-ID", "1")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)

	want := "retry: 2000\n\n" +
		"event: gap\ndata: {\"after\":1}\n\n" +
		"id: 3\ndata: c\n\n" +
		"event: done\ndata: {}\n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}
