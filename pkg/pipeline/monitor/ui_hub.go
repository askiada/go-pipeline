package monitor

import "sync"

type uiHub struct {
	mu      sync.RWMutex
	buffer  int
	clients map[chan monitorEvent]struct{}
	closed  bool
}

func newUIHub(buffer int) *uiHub {
	if buffer < 1 {
		buffer = defaultBufferSize
	}

	return &uiHub{
		buffer:  buffer,
		clients: make(map[chan monitorEvent]struct{}),
	}
}

func (h *uiHub) subscribe() chan monitorEvent {
	ch := make(chan monitorEvent, h.buffer)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		close(ch)

		return ch
	}

	h.clients[ch] = struct{}{}

	return ch
}

func (h *uiHub) unsubscribe(ch chan monitorEvent) {
	if ch == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[ch]; !ok {
		return
	}

	delete(h.clients, ch)
	close(ch)
}

func (h *uiHub) publish(event monitorEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	for ch := range h.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *uiHub) publishPriority(event monitorEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.closed {
		return
	}

	for ch := range h.clients {
		if h.trySend(ch, event) {
			continue
		}

		// Drop one queued event to make room for the snapshot.
		select {
		case <-ch:
		default:
		}

		h.trySend(ch, event)
	}
}

func (h *uiHub) trySend(ch chan monitorEvent, event monitorEvent) bool {
	select {
	case ch <- event:
		return true
	default:
		return false
	}
}

func (h *uiHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	for ch := range h.clients {
		close(ch)
	}

	h.clients = nil
	h.closed = true
}
