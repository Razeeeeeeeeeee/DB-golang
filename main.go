package main

import (
	"database/server"
	"fmt"
)

func main() {
	fmt.Printf("Server running on port: %d \n", server.Port)
	fmt.Println("Supports SET, DEL and GET commands")
	server.Createclient()
}
