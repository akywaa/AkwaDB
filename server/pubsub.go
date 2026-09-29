package server

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"sync"
)

type pubsubHub struct {
	mu   sync.RWMutex
	subs map[string]map[*client]struct{}
}

func newPubSubHub() *pubsubHub {
	return &pubsubHub{
		subs: make(map[string]map[*client]struct{}),
	}
}

func (h *pubsubHub) subscribe(channel string, cl *client) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.subs[channel]
	if !ok {
		set = make(map[*client]struct{})
		h.subs[channel] = set
	}
	set[cl] = struct{}{}

	cl.mu.Lock()
	cl.subs[channel] = struct{}{}
	total := len(cl.subs)
	cl.mu.Unlock()

	return total
}

func (h *pubsubHub) unsubscribe(channel string, cl *client) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	if set, ok := h.subs[channel]; ok {
		delete(set, cl)
		if len(set) == 0 {
			delete(h.subs, channel)
		}
	}

	cl.mu.Lock()
	delete(cl.subs, channel)
	total := len(cl.subs)
	cl.mu.Unlock()

	return total
}

func (h *pubsubHub) unsubscribeAll(cl *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cl.mu.Lock()
	for ch := range cl.subs {
		if set, ok := h.subs[ch]; ok {
			delete(set, cl)
			if len(set) == 0 {
				delete(h.subs, ch)
			}
		}
	}
	cl.subs = make(map[string]struct{})
	cl.mu.Unlock()
}

func (h *pubsubHub) publish(channel, message string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	set, ok := h.subs[channel]
	if !ok || len(set) == 0 {
		return 0
	}

	var buf bytes.Buffer
	buf.WriteString("*3\r\n$7\r\nmessage\r\n")
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(channel), channel))
	buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message))
	payload := buf.Bytes()
	delivered := 0
	for cl := range set {
		cl.mu.Lock()
		if !cl.closed && cl.sendCh != nil {
			select {
			case cl.sendCh <- payload:
				delivered++
			default:
			}
		}
		cl.mu.Unlock()
	}
	return delivered
}

func (cl *client) startPubSubPump() {
	cl.pumpOnce.Do(func() {
		ch := make(chan []byte, 128)
		done := make(chan struct{})
		cl.mu.Lock()
		cl.sendCh = ch
		cl.writerDone = done
		cl.mu.Unlock()
		go func() {
			defer close(done)
			for msg := range ch {
				if _, err := cl.writer.Write(msg); err != nil {
					_ = cl.conn.Close()
					return
				}
				if len(ch) == 0 {
					_ = cl.writer.Flush()
				}
			}
		}()
	})
}

func (s *Server) handleSubscribeLoop(cl *client, r *bufio.Reader, initialChannels []string) {
	cl.startPubSubPump()
	if len(initialChannels) == 0 {
		s.writeError(cl, "ERR wrong number of arguments for 'subscribe' command")
		_ = cl.writer.Flush()
		return
	}

	for _, ch := range initialChannels {
		subCount := s.pubsub.subscribe(ch, cl)
		s.writeSubStatus(cl, "subscribe", ch, subCount)
	}
	_ = cl.writer.Flush()

	for {
		args, err := parseRESP(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "SUBSCRIBE":
			for _, ch := range args[1:] {
				n := s.pubsub.subscribe(ch, cl)
				s.writeSubStatus(cl, "subscribe", ch, n)
			}
		case "UNSUBSCRIBE":
			if len(args) == 1 {
				cl.mu.Lock()
				var all []string
				for ch := range cl.subs {
					all = append(all, ch)
				}
				cl.mu.Unlock()
				for _, ch := range all {
					n := s.pubsub.unsubscribe(ch, cl)
					s.writeSubStatus(cl, "unsubscribe", ch, n)
				}
			} else {
				for _, ch := range args[1:] {
					n := s.pubsub.unsubscribe(ch, cl)
					s.writeSubStatus(cl, "unsubscribe", ch, n)
				}
			}
		case "PING":
			cl.writer.WriteString("*2\r\n$4\r\npong\r\n$0\r\n\r\n")
		case "QUIT":
			s.writeSimpleString(cl, "OK")
			_ = cl.writer.Flush()
			return
		default:
			s.writeError(cl, fmt.Sprintf("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in pub/sub mode, got '%s'", cmd))
		}
		_ = cl.writer.Flush()
	}
}

func (s *Server) cmdPublish(srv *Server, cl *client, args []string) error {
	if len(args) < 2 {
		srv.writeError(cl, "ERR wrong number of arguments for 'publish' command")
		return nil
	}
	receivers := srv.pubsub.publish(args[0], args[1])
	srv.writeInt(cl, int64(receivers))
	return nil
}