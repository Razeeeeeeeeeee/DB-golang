<!-- Code generated by gomarkdoc. DO NOT EDIT -->

# server

```go
import "database/server"
```

## Index

- [Constants](<#constants>)
- [func Client\(ctx context.Context, Port int\) error](<#Client>)
- [func Createclient\(\)](<#Createclient>)
- [func Handleconnection\(conn net.Conn\)](<#Handleconnection>)
- [func ParseClientCommand\(msg string\) string](<#ParseClientCommand>)
- [func WriteRespArray\(wr \*resp.Writer, msgs \[\]string\) error](<#WriteRespArray>)
- [type DELcommand](<#DELcommand>)
- [type GETcommand](<#GETcommand>)
- [type SETcommand](<#SETcommand>)


## Constants

<a name="CommandSET"></a>

```go
const (
    CommandSET = "SET" // Command for setting a key-value pair
    CommandGET = "GET" // Command for getting the value of a key
    CommandDEL = "DEL" // Command for deleting a key
)
```

<a name="Port"></a>

```go
const (
    Port = 6379 // Default port for the server

)
```

<a name="Client"></a>
## func Client

```go
func Client(ctx context.Context, Port int) error
```

Client starts the server, listening for incoming TCP connections on the specified port. It handles context cancellation for graceful shutdown.

<a name="Createclient"></a>
## func Createclient

```go
func Createclient()
```

Createclient starts the client creation process and gracefully handles server shutdown on interrupt or termination signals.

<a name="Handleconnection"></a>
## func Handleconnection

```go
func Handleconnection(conn net.Conn)
```

Handleconnection processes a single connection from a client. It reads client commands, executes them, and sends back appropriate responses.

<a name="ParseClientCommand"></a>
## func ParseClientCommand

```go
func ParseClientCommand(msg string) string
```

ParseClientCommand parses the client command into a RESP\-formatted string. It splits the command into parts and writes it using the RESP protocol.

<a name="WriteRespArray"></a>
## func WriteRespArray

```go
func WriteRespArray(wr *resp.Writer, msgs []string) error
```

WriteRespArray writes an array of RESP values to the writer.

<a name="DELcommand"></a>
## type DELcommand

DELcommand represents a DEL command with a key.

```go
type DELcommand struct {
    // contains filtered or unexported fields
}
```

<a name="GETcommand"></a>
## type GETcommand

GETcommand represents a GET command with a key.

```go
type GETcommand struct {
    // contains filtered or unexported fields
}
```

<a name="SETcommand"></a>
## type SETcommand

SETcommand represents a SET command with a key and value.

```go
type SETcommand struct {
    // contains filtered or unexported fields
}
```

Generated by [gomarkdoc](<https://github.com/princjef/gomarkdoc>)
