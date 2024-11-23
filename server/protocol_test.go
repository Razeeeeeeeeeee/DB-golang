package server

import (
	"reflect"
	"testing"
)

func TestSETProtocol(t *testing.T) {
	raw := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	cmd, err := parseCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	cmd, ok := cmd.(SETcommand)
	if !ok {
		t.Errorf("not the right command")
	}
	if !reflect.DeepEqual(cmd, SETcommand{key: "foo", val: "bar"}) {
		t.Error("the parsing failed")
	}
}

func TestGETProtocol(t *testing.T) {
	raw := "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
	cmd, err := parseCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	cmd, ok := cmd.(GETcommand)
	if !ok {
		t.Errorf("not the right command")
	}
	if !reflect.DeepEqual(cmd, GETcommand{key: "foo"}) {
		t.Error("the parsing failed")
	}
}

func TestDELProtocol(t *testing.T) {
	raw := "*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n"
	cmd, err := parseCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	cmd, ok := cmd.(DELcommand)
	if !ok {
		t.Errorf("not the right command")
	}
	if !reflect.DeepEqual(cmd, DELcommand{key: "foo"}) {
		t.Error("the parsing failed")
	}
}
