package socket

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

const ListenAddr = "127.0.0.1:49152"

type Command struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Event struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

type Handler func(conn net.Conn, cmd Command)

type Listener struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	listener net.Listener
	clients  map[net.Conn]bool
}

func NewListener() *Listener {
	return &Listener{
		handlers: make(map[string]Handler),
		clients:  make(map[net.Conn]bool),
	}
}

func (l *Listener) Handle(cmdType string, handler Handler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handlers[cmdType] = handler
}

func (l *Listener) Start() error {
	var err error
	l.listener, err = net.Listen("tcp", ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	go l.acceptLoop()
	return nil
}

func (l *Listener) acceptLoop() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			break
		}
		l.mu.Lock()
		l.clients[conn] = true
		l.mu.Unlock()
		go l.handleConn(conn)
	}
}

func (l *Listener) handleConn(conn net.Conn) {
	defer func() {
		l.mu.Lock()
		delete(l.clients, conn)
		l.mu.Unlock()
		conn.Close()
	}()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var cmd Command
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &cmd); err != nil {
			sendError(conn, "invalid JSON: "+err.Error())
			continue
		}

		l.mu.RLock()
		handler, ok := l.handlers[cmd.Type]
		l.mu.RUnlock()

		if !ok {
			sendError(conn, "unknown command: "+cmd.Type)
			continue
		}

		handler(conn, cmd)
	}
}

func (l *Listener) Broadcast(event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	data = append(data, '\n')

	l.mu.RLock()
	defer l.mu.RUnlock()

	for conn := range l.clients {
		conn.Write(data)
	}
}

func (l *Listener) Stop() {
	if l.listener != nil {
		l.listener.Close()
	}
}

func sendError(conn net.Conn, msg string) {
	resp, _ := json.Marshal(Event{Type: "error", Payload: map[string]string{"message": msg}})
	resp = append(resp, '\n')
	conn.Write(resp)
}

func SendEvent(conn net.Conn, event Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}
