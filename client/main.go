package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"awesomeProject1/shared"
)

type ClientApp struct {
	rpcClient *rpc.Client
	username  string
	stopChan  chan struct{}
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter server address (e.g., localhost:12345 or press Enter for default): ")
	addr, _ := reader.ReadString('\n')
	addr = strings.TrimSpace(addr)
	if addr == "" {
		addr = "localhost:12345"
	}

	rpcClient, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to RPC server at %s: %v", addr, err)
	}
	defer rpcClient.Close()

	app := &ClientApp{
		rpcClient: rpcClient,
		stopChan:  make(chan struct{}),
	}

	for {
		fmt.Print("Enter unique username: ")
		username, _ := reader.ReadString('\n')
		username = strings.TrimSpace(username)

		if username == "" {
			fmt.Println("Username cannot be empty.")
			continue
		}

		var reply shared.JoinReply
		err := rpcClient.Call("ChatServer.Join", &shared.JoinArgs{Username: username}, &reply)
		if err != nil {
			log.Fatalf("RPC Join Error: %v", err)
		}

		if reply.Success {
			app.username = username
			fmt.Println("\n" + reply.Message)
			break
		} else {
			fmt.Printf("Error: %s\n", reply.Message)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		app.disconnect()
		os.Exit(0)
	}()

	go app.pollMessages()

	printHelp()

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			app.disconnect()
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		switch {
		case input == "/help":
			printHelp()

		case input == "/users":
			var reply shared.ListUsersReply
			err := app.rpcClient.Call("ChatServer.ListUsers", &shared.ListUsersArgs{}, &reply)
			if err != nil {
				fmt.Printf("[Error fetching users: %v]\n", err)
			} else {
				fmt.Printf("\n--- Connected Users (%d) ---\n", len(reply.Users))
				for _, user := range reply.Users {
					fmt.Printf("- %s\n", user)
				}
				fmt.Println("---------------------------")
			}

		case input == "/exit":
			app.disconnect()
			fmt.Println("Disconnected from chat.")
			return

		default:
			var reply shared.SendMessageReply
			err := app.rpcClient.Call("ChatServer.SendMessage", &shared.SendMessageArgs{
				Sender:  app.username,
				Content: input,
			}, &reply)

			if err != nil {
				fmt.Printf("[Error sending message: %v]\n", err)
			} else if !reply.Success {
				fmt.Printf("[Server Error: %s]\n", reply.Message)
			}
		}
	}
}

func (app *ClientApp) pollMessages() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-app.stopChan:
			return
		case <-ticker.C:
			var reply shared.FetchMessagesReply
			err := app.rpcClient.Call("ChatServer.FetchMessages", &shared.FetchMessagesArgs{Username: app.username}, &reply)
			if err != nil {
				return
			}

			for _, msg := range reply.Messages {
				timestamp := msg.Timestamp.Format("15:04:05")
				if msg.Type == shared.TypeNotification {
					fmt.Printf("\r\033[K[%s] *** %s ***\n> ", timestamp, msg.Content)
				} else {
					fmt.Printf("\r\033[K[%s] <%s>: %s\n> ", timestamp, msg.Sender, msg.Content)
				}
			}
		}
	}
}

func (app *ClientApp) disconnect() {
	close(app.stopChan)
	var reply shared.LeaveReply
	_ = app.rpcClient.Call("ChatServer.Leave", &shared.LeaveArgs{Username: app.username}, &reply)
}

func printHelp() {
	fmt.Println("\n================ Command Help ================")
	fmt.Println(" Type any text and press Enter to broadcast.")
	fmt.Println(" /users - List all currently online users")
	fmt.Println(" /help  - Display available commands")
	fmt.Println(" /exit  - Gracefully leave the chat")
	fmt.Println("==============================================\n")
}
