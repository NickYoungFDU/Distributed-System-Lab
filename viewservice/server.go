package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
    
    currentView View
    
    recentPing map[string]time.Time        
    
    hasPrimaryAcked bool
}

func (vs *ViewServer) changeView(n uint, p, b string) {
    vs.currentView.Viewnum = n
    vs.currentView.Primary = p
    vs.currentView.Backup = b
    vs.hasPrimaryAcked = false
}


func (vs *ViewServer) PromoteBackup() {
    if !vs.hasPrimaryAcked {
        return
    }
    newBackup := ""
    for k := range vs.recentPing {
        if k != vs.currentView.Primary && k != vs.currentView.Backup {
            newBackup = k
            break
        }
    }
    vs.changeView(vs.currentView.Viewnum + 1, vs.currentView.Backup, newBackup)    
}

func (vs *ViewServer) RemoveBackup() {
    newBackup := ""
    for k := range vs.recentPing {
        if k != vs.currentView.Primary && k != vs.currentView.Backup {
            newBackup = k
            break
        }
    }
    vs.changeView(vs.currentView.Viewnum + 1, vs.currentView.Primary, newBackup)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
    
    
    
    clientId, clientViewNum := args.Me, args.Viewnum
    
    
    vs.mu.Lock()
    
    vs.recentPing[clientId] = time.Now()     

	// Your code here.
    
    switch clientId {
        case vs.currentView.Primary:
            if clientViewNum == vs.currentView.Viewnum {
                vs.hasPrimaryAcked = true
            } else if clientViewNum == 0 {
                vs.PromoteBackup()
            }
        default:
            if vs.currentView.Primary == "" {
                vs.changeView(1, clientId, "")
            } else if vs.currentView.Backup == "" {
                vs.changeView(vs.currentView.Viewnum + 1, vs.currentView.Primary, clientId)
            }
            
    }    
    
    reply.View = View(vs.currentView)   
           
    vs.mu.Unlock()           

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
    vs.mu.Lock()
    reply.View = View(vs.currentView)
    vs.mu.Unlock()

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
    
    vs.mu.Lock()
    
    primary, backup := vs.currentView.Primary, vs.currentView.Backup
        
    if time.Since(vs.recentPing[primary]) > DeadPings * PingInterval {
        vs.PromoteBackup()
        //fmt.Printf("Current Primary(Promoted):%s\n", vs.currentView.Primary)
        fmt.Printf("New View:%v\n", vs.currentView)
    }  
    if time.Since(vs.recentPing[backup]) > DeadPings * PingInterval {
        vs.RemoveBackup()
        fmt.Printf("New View:%v\n", vs.currentView)
    }
    
    vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
    vs.currentView = View{0, "", ""}
    vs.recentPing = make(map[string]time.Time)    

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
