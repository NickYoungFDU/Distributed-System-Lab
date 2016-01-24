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
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
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
            call(pb.currentView.Backup, "Get", &argsx, &replyx)
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


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
    sender := args.Sender
    if sender == Client {
        if pb.identity == Primary {
            key, value, op := args.Key, args.Value, args.Op
            if op == "Put" {
                pb.database[key] = value               
            } else {
                v, ok := pb.database[key]
                if ok {
                    pb.database[key] = pb.database[key] + value
                } else {
                    pb.database[key] = value
                }
            }
            // forward this operation to the backup
            argsx := PutAppendArgs{key, value, Primary, op}
            var replyx PutAppendReply            
            call(pb.currentView.Backup, "PutAppend", &argsx, &replyx)
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

func compareView(v1, v2 viewservice.View) {
    return v1.Viewnum == v2.Viewnum && v1.Primary == v2.Primary && v1.Backup == v2.Backup
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

	// Your code here.
    vx, _ := pb.vs.Get()
    view, _ := pb.vs.Ping(vx.Viewnum)
    pb.identity = pb.getIdentity(view)
    if pb.identity == Primary && pb.currentView != nil && pb.currentView.Backup != view.Backup {
        args := TransferArgs{pb.database, Primary}
        var reply TransferReply
        call(view.Backup, "Transfer", &args, &reply)
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
