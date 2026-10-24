package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"awesomeProject1/shared"
)

type Client struct {
	Username   string
	Queue      chan shared.Message
	LastActive time.Time
}

type ChatServer struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func NewChatServer() *ChatServer {
	cs := &ChatServer{
		clients: make(map[string]*Client),
	}
	// Background cleanup for stale polling clients (30-second timeout)
	go cs.cleanupStaleClients(30 * time.Second)
	return cs
}

func (s *ChatServer) Join(args *shared.JoinArgs, reply *shared.JoinReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if args.Username == "" {
		reply.Success = false
		reply.Message = "Username cannot be empty."
		return nil
	}

	if _, exists := s.clients[args.Username]; exists {
		reply.Success = false
		reply.Message = fmt.Sprintf("Username '%s' is already taken.", args.Username)
		return nil
	}

	client := &Client{
		Username:   args.Username,
		Queue:      make(chan shared.Message, 100),
		LastActive: time.Now(),
	}
	s.clients[args.Username] = client

	reply.Success = true
	reply.Message = fmt.Sprintf("Welcome to the chat, %s!", args.Username)

	// Broadcast Join Notification (excluding the newly joined user)
	s.broadcastUnsafe(shared.Message{
		Sender:    "SERVER",
		Content:   fmt.Sprintf("User '%s' has joined the chat.", args.Username),
		Type:      shared.TypeNotification,
		Timestamp: time.Now(),
	}, args.Username)

	log.Printf("User '%s' joined.", args.Username)
	return nil
}

func (s *ChatServer) Leave(args *shared.LeaveArgs, reply *shared.LeaveReply) error {
	s.removeClient(args.Username)
	reply.Success = true
	return nil
}

func (s *ChatServer) SendMessage(args *shared.SendMessageArgs, reply *shared.SendMessageReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, exists := s.clients[args.Sender]
	if !exists {
		reply.Success = false
		reply.Message = "User is not registered on the server."
		return nil
	}
	client.LastActive = time.Now()

	msg := shared.Message{
		Sender:    args.Sender,
		Content:   args.Content,
		Type:      shared.TypeChat,
		Timestamp: time.Now(),
	}

	// Broadcast to all other users excluding sender
	s.broadcastUnsafe(msg, args.Sender)
	reply.Success = true
	return nil
}

func (s *ChatServer) FetchMessages(args *shared.FetchMessagesArgs, reply *shared.FetchMessagesReply) error {
	s.mu.Lock()
	client, exists := s.clients[args.Username]
	if exists {
		client.LastActive = time.Now()
	}
	s.mu.Unlock()

	if !exists {
		return nil
	}

	// Drain queue without blocking
	var messages []shared.Message
	for {
		select {
		case msg := <-client.Queue:
			messages = append(messages, msg)
		default:
			reply.Messages = messages
			return nil
		}
	}
}

func (s *ChatServer) ListUsers(args *shared.ListUsersArgs, reply *shared.ListUsersReply) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]string, 0, len(s.clients))
	for username := range s.clients {
		users = append(users, username)
	}
	reply.Users = users
	return nil
}

// Internal thread-unsafe broadcast helper (must hold s.mu Lock)
func (s *ChatServer) broadcastUnsafe(msg shared.Message, excludeUser string) {
	for username, client := range s.clients {
		if username == excludeUser {
			continue
		}
		select {
		case client.Queue <- msg:
		default:
			// Queue full; discard oldest message to avoid blocking server operations
			select {
			case <-client.Queue:
			default:
			}
			client.Queue <- msg
		}
	}
}

func (s *ChatServer) removeClient(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.clients[username]; exists {
		delete(s.clients, username)
		log.Printf("User '%s' disconnected.", username)

		s.broadcastUnsafe(shared.Message{
			Sender:    "SERVER",
			Content:   fmt.Sprintf("User '%s' has left the chat.", username),
			Type:      shared.TypeNotification,
			Timestamp: time.Now(),
		}, username)
	}
}

func (s *ChatServer) cleanupStaleClients(timeout time.Duration) {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for username, client := range s.clients {
			if now.Sub(client.LastActive) > timeout {
				log.Printf("Client '%s' timed out due to inactivity.", username)
				delete(s.clients, username)
				s.broadcastUnsafe(shared.Message{
					Sender:    "SERVER",
					Content:   fmt.Sprintf("User '%s' timed out.", username),
					Type:      shared.TypeNotification,
					Timestamp: time.Now(),
				}, username)
			}
		}
		s.mu.Unlock()
	}
}

func main() {
	server := NewChatServer()
	err := rpc.Register(server)
	if err != nil {
		log.Fatalf("RPC Register Error: %v", err)
	}

	listener, err := net.Listen("tcp", ":12345")
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	defer listener.Close()

	log.Println("RPC Chat Server listening on port 12345...")

	// Graceful Shutdown on Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nServer shutting down...")
		listener.Close()
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-sigChan:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}
		go rpc.ServeConn(conn)
	}
}
