package shared

import "time"

type MessageType string

const (
	TypeChat         MessageType = "CHAT"
	TypeNotification MessageType = "NOTIFICATION"
)

type Message struct {
	Sender    string      `json:"sender"`
	Content   string      `json:"content"`
	Type      MessageType `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
}

// RPC Method Args/Replies

type JoinArgs struct {
	Username string
}

type JoinReply struct {
	Success bool
	Message string
}

type LeaveArgs struct {
	Username string
}

type LeaveReply struct {
	Success bool
}

type SendMessageArgs struct {
	Sender  string
	Content string
}

type SendMessageReply struct {
	Success bool
	Message string
}

type FetchMessagesArgs struct {
	Username string
}

type FetchMessagesReply struct {
	Messages []Message
}

type ListUsersArgs struct{}

type ListUsersReply struct {
	Users []string
}
