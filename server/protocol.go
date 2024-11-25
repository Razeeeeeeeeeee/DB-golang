package server

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/tidwall/resp"
)

const (
	CommandSET = "SET"
	CommandGET = "GET"
	CommandDEL = "DEL"
)

type command interface {
}

type SETcommand struct {
	key, val string
}

type GETcommand struct {
	key string
}

type DELcommand struct {
	key string
}

func parseCommand(msg string) (command, error) {
	rd := resp.NewReader(bytes.NewBufferString(msg))
	for {
		v, _, err := rd.ReadValue()
		// fmt.Print(v)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if v.Type() == resp.Array {
			for _, value := range v.Array() {
				switch value.String() {

				case CommandSET:
					if len(v.Array()) != 3 {
						return nil, fmt.Errorf("wrong number of parameters")
					}
					cmd := SETcommand{
						key: v.Array()[1].String(),
						val: v.Array()[2].String(),
					}
					return cmd, nil

				case CommandGET:
					if len(v.Array()) != 2 {
						return nil, fmt.Errorf("wrong number of parameters")
					}
					cmd := GETcommand{
						key: v.Array()[1].String(),
					}
					return cmd, nil

				case CommandDEL:
					if len(v.Array()) != 2 {
						return nil, fmt.Errorf("wrong number of parameters")
					}
					cmd := DELcommand{
						key: v.Array()[1].String(),
					}
					return cmd, nil

				default:
				}
			}
		}

	}
	return nil, fmt.Errorf("invalid or unknown command")
}
