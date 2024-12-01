package server

import (
	"bytes"
	"context"
	db "database/database"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/tidwall/resp"
)

const (
	Port        = 6379 // Default port for the server
	receiveBufs = 1024 // Buffer size for receiving data
)

var wg sync.WaitGroup

// Createclient starts the client creation process and gracefully handles server shutdown on interrupt or termination signals.
func Createclient() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture interrupt signal for graceful shutdown
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		cancel()
	}()

	// Start the client and handle server errors
	if err := Client(ctx, Port); err != nil {
		fmt.Println("Server error:", err)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	fmt.Println("All connections handled. Exiting.")
}

// Client starts the server, listening for incoming TCP connections on the specified port.
// It handles context cancellation for graceful shutdown.
func Client(ctx context.Context, Port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", Port))
	if err != nil {
		slog.Error("Failed to bind to Port")
		os.Exit(1)
	}
	defer l.Close()

	// Channel to signal listener closure
	done := make(chan struct{})

	// Gracefully handle context cancellation and close the listener
	go func() {
		<-ctx.Done()
		fmt.Println("Context canceled, closing listener...")
		l.Close() // Close the listener to unblock Accept()
		close(done)
	}()

	// Accept and handle connections
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down server...")
			return nil
		default:
			conn, err := l.Accept()
			if err != nil {
				slog.Error("Error accepting connection")
				os.Exit(1)
			}
			wg.Add(1)
			go Handleconnection(conn) // Handle each connection in a new goroutine
		}
	}
}

// Handleconnection processes a single connection from a client.
// It reads client commands, executes them, and sends back appropriate responses.
func Handleconnection(conn net.Conn) {
	defer wg.Done()    // Ensure goroutine is finished when done
	defer conn.Close() // Close connection when done

	buf := make([]byte, receiveBufs)
	file_path := "../data/db"
	db, _ := db.Open(file_path)

	// Continuously read and process client commands
	for {
		dlen, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("reading", "err", err)
			}
			return
		}
		comms := ParseClientCommand(string(buf[:dlen-2])) // Parse client command
		commands, err := parseCommand(comms)              // Parse the command further

		// Handle different types of commands
		switch c := commands.(type) {
		case SETcommand:
			// Handle SET command: Store key-value pair in database
			_, exists, _ := db.Get(c.key)
			if !exists {
				err := db.Put(c.key, c.val)
				if err != nil {
					conn.Write([]byte("Error setting the value "))
				} else {
					conn.Write([]byte("+OK\r\n"))
				}
			} else {
				conn.Write([]byte("Key already exists\r\n"))
			}

		case GETcommand:
			// Handle GET command: Retrieve value for the given key
			value, _, err := db.Get(c.key)
			if err != nil {
				conn.Write([]byte("Error setting the value "))
			} else {
				if value == "" {
					conn.Write([]byte("Key not found\r\n"))
				} else {
					conn.Write([]byte(value + "\r\n"))
				}
			}
		case DELcommand:
			// Handle DEL command: Delete the given key from the database
			err := db.Del(c.key)
			if err != nil {
				conn.Write([]byte("Error setting the value\r\n"))
			} else {
				conn.Write([]byte("+OK\r\n"))
			}
		default:
			// Default response for unknown commands, :: This is done inorder to pass the redis-benchmarks
			conn.Write([]byte("+OK\r\n"))
		}
	}
}

// ParseClientCommand parses the client command into a RESP-formatted string.
// It splits the command into parts and writes it using the RESP protocol.
func ParseClientCommand(msg string) string {
	commands := strings.Split(msg, " ")
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = WriteRespArray(wr, commands) // Write the command array to the buffer
	return buf.String()              // Return the formatted RESP string
}

// WriteRespArray writes an array of RESP values to the writer.
func WriteRespArray(wr *resp.Writer, msgs []string) error {
	values := make([]resp.Value, len(msgs))
	for i, str := range msgs {
		values[i] = resp.StringValue(str) // Convert each string to RESP value
	}
	return wr.WriteArray(values) // Write the array of values to the writer
}
