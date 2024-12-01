package main

import (
	"database/server"
	"fmt"
)

func main() {
	// Print the server port and supported commands
	fmt.Printf("Server created and running on port: %d \n", server.Port)
	fmt.Println("Supports SET, DEL and GET commands")

	// Start the client and begin handling connections
	server.Createclient()
}
