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
	port        = 6789
	receiveBufs = 1024
)

var wg sync.WaitGroup

func createclient() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture interrupt signal for graceful shutdown
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		cancel()
	}()

	if err := client(ctx, port); err != nil {
		fmt.Println("Server error:", err)
	}

	wg.Wait() // Ensure all goroutines finish
	fmt.Println("All connections handled. Exiting.")
}

func client(ctx context.Context, port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		slog.Error("Failed to bind to port")
		os.Exit(1)
	}
	defer l.Close()

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
			go handleconnection(conn)
		}
	}
}

func handleconnection(conn net.Conn) {

	defer wg.Done()
	defer conn.Close()

	buf := make([]byte, receiveBufs)

	file_path := "../data/db"
	db, _ := db.Open(file_path)
	for {
		dlen, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("reading", "err", err)
			}
			return
		}
		comms := parseClientCommand(string(buf[:dlen-2]))
		commands, err := parseCommand(comms)
		// fmt.Print(commands)

		switch c := commands.(type) {
		case SETcommand:
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
			err := db.Del(c.key)
			if err != nil {
				conn.Write([]byte("Error setting the value\r\n"))
			} else {
				conn.Write([]byte("+OK\r\n"))
			}
		default:
			panic("not a valid command")
		}

	}
}

func parseClientCommand(msg string) string {
	commands := strings.Split(msg, " ")
	var buf bytes.Buffer
	wr := resp.NewWriter(&buf)
	_ = writeRespArray(wr, commands)
	// print(buf.String())
	return buf.String()
}

func writeRespArray(wr *resp.Writer, msgs []string) error {
	values := make([]resp.Value, len(msgs))

	for i, str := range msgs {
		values[i] = resp.StringValue(str)
	}
	return wr.WriteArray(values)
}

// 	for {
// 		dlen, err := conn.Read(buf)
// 		if err != nil {
// 			if err.Error() == "EOF" {
// 				fmt.Println("Connection closed")
// 				break
// 			}

// 			fmt.Println("Error reading:", err.Error())
// 			break
// 		}

// 		if dlen == 0 {
// 			fmt.Println("no data to read")
// 			break
// 		}
// 	}

// 	return nil
// }
