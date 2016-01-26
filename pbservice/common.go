package pbservice

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Identity string

const(
    Primary         = "Primary"
    Backup          = "Backup"
    Idle            = "Idle"
    Client          = "Client"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
    
    Id    int64
    Sender Identity
    Op  string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    Sender Identity
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

type TransferArgs struct {
    Database map[string]string
    Sender Identity
}

type TransferReply struct {
    Err Err
}


// Debugging
var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug == 12 {
		n, err = fmt.Printf(format, a...)
	}
	return
}