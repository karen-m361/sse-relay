package hub

import (
	"errors"
	"testing"
	"time"
)

func drain(t *testing.T, sub *Subscription, n int) []Event {
	t.Helper()
	got := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("channel closed early after %d of %d events", i, n)
			}
			got = append(got, ev)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %d of %d", i, n)
		}
	}
	return got
}

func TestPublishAssignsSequentialIDs(t *testing.T) {
	s := newStream("s", 0)

	ev1, err := s.Publish("a")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ev2, err := s.Publish("b")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if ev1.ID != 1 || ev2.ID != 2 {
		t.Fatalf("got ids %d, %d, want 1, 2", ev1.ID, ev2.ID)
	}
}

func TestPublishAfterFinishFails(t *testing.T) {
	s := newStream("s", 0)
	s.Finish()

	if _, err := s.Publish("a"); !errors.Is(err, ErrStreamDone) {
		t.Fatalf("Publish after Finish: got %v, want ErrStreamDone", err)
	}
}

func TestSubscribeDeliversLiveEvents(t *testing.T) {
	s := newStream("s", 0)
	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("fresh stream reported a gap")
	}

	want, err := s.Publish("hello")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := drain(t, sub, 1)[0]
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSubscribeReplaysFromLastID(t *testing.T) {
	s := newStream("s", 0)
	for _, data := range []string{"a", "b", "c"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	sub, gap := s.Subscribe(1, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap when replay buffer covers lastID")
	}

	got := drain(t, sub, 2)
	if got[0].Data != "b" || got[1].Data != "c" {
		t.Fatalf("got %+v, want events b then c", got)
	}
}

func TestSubscribeReportsGapWhenBufferEvicted(t *testing.T) {
	s := newStream("s", 1)
	for _, data := range []string{"a", "b", "c"} {
		if _, err := s.Publish(data); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Event 2 ("b") was evicted by the capacity-1 buffer before this client,
	// which last saw event 1, could ask for it: that is a real gap.
	sub, gap := s.Subscribe(1, 0)
	defer sub.Close()
	if !gap {
		t.Fatal("expected a gap when the requested lastID was evicted")
	}

	got := drain(t, sub, 1)
	if got[0].Data != "c" {
		t.Fatalf("got %+v, want event c", got)
	}
}

func TestLaggingSubscriberIsDroppedNotBlocked(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 1)
	defer sub.Close()

	// The subscriber's buffer holds one event and nothing reads it, so the
	// second publish must find it full and evict it rather than block.
	if _, err := s.Publish("a"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := s.Publish("b"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case _, ok := <-sub.Events():
		if ok {
			// The one buffered event drains first, then the channel closes.
			_, ok = <-sub.Events()
			if ok {
				t.Fatal("expected channel to close after the buffered event")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the lagging subscriber to be dropped")
	}

	if !errors.Is(sub.Err(), ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", sub.Err())
	}

	stats := s.Stats()
	if stats.Subscribers != 0 {
		t.Fatalf("subscriber count = %d, want 0 after eviction", stats.Subscribers)
	}
}

func TestSubscribeAfterFinishReplaysThenCloses(t *testing.T) {
	s := newStream("s", 0)
	if _, err := s.Publish("a"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s.Finish()

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if gap {
		t.Fatal("unexpected gap")
	}

	got := drain(t, sub, 1)
	if got[0].Data != "a" {
		t.Fatalf("got %+v, want event a", got)
	}

	if _, ok := <-sub.Events(); ok {
		t.Fatal("expected channel to be closed after replay of a finished stream")
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil for a clean finish", err)
	}
}

func TestFinishClosesLiveSubscribers(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 0)
	defer sub.Close()

	s.Finish()

	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("expected a closed channel after Finish")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Finish to close the subscriber")
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
	if !s.Done() {
		t.Fatal("Done() = false after Finish")
	}
}

func TestCloseDetachesSubscriberWithoutError(t *testing.T) {
	s := newStream("s", 0)
	sub, _ := s.Subscribe(0, 0)
	sub.Close()

	if err := sub.Err(); err != nil {
		t.Fatalf("Err() after Close = %v, want nil", err)
	}
	if stats := s.Stats(); stats.Subscribers != 0 {
		t.Fatalf("subscriber count = %d, want 0 after Close", stats.Subscribers)
	}

	// Closing twice must not panic or re-fire the once.
	sub.Close()
}

func TestHubGetOrCreateReturnsSameStream(t *testing.T) {
	h := New(0)
	a := h.GetOrCreate("x")
	b := h.GetOrCreate("x")
	if a != b {
		t.Fatal("GetOrCreate returned different streams for the same id")
	}
	if _, ok := h.Stream("y"); ok {
		t.Fatal("Stream reported an id that was never created")
	}
}

func TestHubRemoveFinishesAndForgetsStream(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("x")
	sub, _ := s.Subscribe(0, 0)
	defer sub.Close()

	if !h.Remove("x") {
		t.Fatal("Remove reported false for an existing stream")
	}
	if h.Remove("x") {
		t.Fatal("Remove reported true for an already-removed stream")
	}
	if _, ok := h.Stream("x"); ok {
		t.Fatal("stream is still reachable after Remove")
	}
	if _, ok := <-sub.Events(); ok {
		t.Fatal("expected Remove to close the stream's subscribers")
	}
}

func TestHubIDsAreSorted(t *testing.T) {
	h := New(0)
	h.GetOrCreate("b")
	h.GetOrCreate("a")
	h.GetOrCreate("c")

	got := h.IDs()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestHubCloseAllFinishesEveryStream(t *testing.T) {
	h := New(0)
	s1 := h.GetOrCreate("a")
	s2 := h.GetOrCreate("b")

	h.CloseAll()

	if !s1.Done() || !s2.Done() {
		t.Fatal("CloseAll left a stream unfinished")
	}
}
