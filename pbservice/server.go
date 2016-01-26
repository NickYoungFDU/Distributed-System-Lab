package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
    
    currentView viewservice.View
    identity Identity
    database map[string]string
    seenRPCs map[int64]bool
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()
    sender := args.Sender
    if sender == Primary && pb.identity == Backup {
        pb.database = args.Database
        reply.Err = OK
    } else {
        reply.Err = ErrWrongServer
    }
    return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()
	// Your code here.
    sender := args.Sender
    if sender == Client {
        if pb.identity == Primary {
            key := args.Key
            v, ok := pb.database[key]
            if ok {
                reply.Value = v
                reply.Err = OK
            } else {
                reply.Value = ""
                reply.Err = ErrNoKey
            }        
            // forward this operation to the backup
            argsx := GetArgs{key, Primary}
            var replyx GetReply            
            call(pb.currentView.Backup, "PBServer.Get", &argsx, &replyx)
        } else {
            reply.Value = ""
            reply.Err = ErrWrongServer
        }        
    } else if sender == Primary {
        if pb.identity != Backup {
            reply.Value = ""
            reply.Err = ErrWrongServer
        }
    }    
	return nil
}

func (pb *PBServer) PrintDatabase() {
    for k, v := range pb.database {
        fmt.Printf("key:%s, value:%s\n", k, v)
    }
}

func (pb *PBServer) DoPutAppend(key, value, op string, id int64) {
            if op == "Put" {
                pb.database[key] = value               
            } else {
                _, ok := pb.database[key]
                if ok {
                    pb.database[key] = pb.database[key] + value
                } else {
                    pb.database[key] = value
                }
            }
            pb.seenRPCs[id] = true
            //pb.PrintDatabase()
            // forward this operation to the backup
            argsx := PutAppendArgs{key, value, id, Primary, op}
            var replyx PutAppendReply  
            DPrintf("Yeah3\n")   
            DPrintf("pb.currentView.Backup:%s\n", pb.currentView.Backup)            
            call(pb.currentView.Backup, "PBServer.PutAppend", &argsx, &replyx)    
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()
	// Your code here.
    id := args.Id
    completed, seen := pb.seenRPCs[id]
    if seen && completed{
        DPrintf("checkpoint, database[%s] = %s\n", args.Key, pb.database[args.Key])
        return nil
    } else {
        pb.seenRPCs[id] = false
    }
    sender := args.Sender
    if sender == Client {
        key, value, op := args.Key, args.Value, args.Op
        DPrintf("Yeah\n")
        if pb.identity == Primary {
            DPrintf("Yeah1\n")
            pb.DoPutAppend(key, value, op, id)
        } else if v, _ := pb.vs.Get(); v.Primary == pb.me {
            DPrintf("Yeah2\n")
            pb.identity = Primary  
            pb.currentView = v      
            pb.DoPutAppend(key, value, op, id)                
        } else {
            reply.Err = ErrWrongServer
        }
    } else if sender == Primary {
        if pb.identity != Backup {            
            reply.Err = ErrWrongServer
        } else {
            key, value, op := args.Key, args.Value, args.Op
            if op == "Put" {
                pb.database[key] = value               
            } else {
                _, ok := pb.database[key]
                if ok {
                    pb.database[key] = pb.database[key] + value
                } else {
                    pb.database[key] = value
                }
            }                        
        }
    }     
	return nil
}


func (pb *PBServer) getIdentity(view viewservice.View) Identity {
    if view.Primary == pb.me {
        return Primary
    } else if view.Backup == pb.me {
        return Backup
    } else {
        return Idle
    }
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
    pb.mu.Lock()
    defer pb.mu.Unlock()
	// Your code here.
    vx, _ := pb.vs.Get()
    view, _ := pb.vs.Ping(vx.Viewnum)
    pb.identity = pb.getIdentity(view)
    //fmt.Printf("I'm %s, and I'm a %s\n", pb.me, pb.identity)
    if pb.identity == Primary && pb.currentView.Backup != view.Backup {
        args := TransferArgs{pb.database, Primary}
        var reply TransferReply
        call(view.Backup, "PBServer.Transfer", &args, &reply)
    }
    pb.currentView = view
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
    pb.identity = Idle
    pb.database = make(map[string]string)
    pb.seenRPCs = make(map[int64]bool)
    pb.currentView = viewservice.View{0, "", ""}    

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
