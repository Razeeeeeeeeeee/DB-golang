package server

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/tidwall/resp"
)

const (
	CommandSET = "SET" // Command for setting a key-value pair
	CommandGET = "GET" // Command for getting the value of a key
	CommandDEL = "DEL" // Command for deleting a key
)

// command is an empty interface implemented by different command types.
type command interface{}

// SETcommand represents a SET command with a key and value.
type SETcommand struct {
	key, val string
}

// GETcommand represents a GET command with a key.
type GETcommand struct {
	key string
}

// DELcommand represents a DEL command with a key.
type DELcommand struct {
	key string
}

// parseCommand parses a RESP-formatted message into a command.
// It identifies the command type (SET, GET, DEL) and extracts parameters.
func parseCommand(msg string) (command, error) {
	rd := resp.NewReader(bytes.NewBufferString(msg))

	// Loop to read values from the RESP array
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			break // End of input
		}
		if err != nil {
			log.Fatal(err) // Fatal error in reading
		}

		if v.Type() == resp.Array {
			// Process the array from the RESP protocol
			for _, value := range v.Array() {
				switch value.String() {

				case CommandSET:
					// Handle SET command
					if len(v.Array()) != 3 {
						return nil, fmt.Errorf("wrong number of parameters for SET command")
					}
					cmd := SETcommand{
						key: v.Array()[1].String(),
						val: v.Array()[2].String(),
					}
					return cmd, nil

				case CommandGET:
					// Handle GET command
					if len(v.Array()) != 2 {
						return nil, fmt.Errorf("wrong number of parameters for GET command")
					}
					cmd := GETcommand{
						key: v.Array()[1].String(),
					}
					return cmd, nil

				case CommandDEL:
					// Handle DEL command
					if len(v.Array()) != 2 {
						return nil, fmt.Errorf("wrong number of parameters for DEL command")
					}
					cmd := DELcommand{
						key: v.Array()[1].String(),
					}
					return cmd, nil

				default:
					// Unknown command, no action
				}
			}
		}
	}

	// Return an error if no valid command is found
	return nil, fmt.Errorf("invalid or unknown command")
}
