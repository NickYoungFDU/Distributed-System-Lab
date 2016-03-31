package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)



type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.       
    maxSeq      int
    done        int
    p_done      []int
    propState map[int]ProposalState 
}

type ProposalState struct {
    nPrepare   int
    nAccept  int
    valAccept  interface{} 
    state Fate
    val interface{}
}

type PrepareArgs struct {
    Me      int
    Seq     int
    Num     int    
    Done    int
}

type PrepareReply struct {
    OK  bool
    Num int
    NAccept int
    ValAccept interface{}
    Done    int
}

type AcceptArgs struct {
    Me  int
    Seq int
    Num int
    Val interface{}
    Done    int
}

type AcceptReply struct {
    OK bool
    Num int
    Done    int
}

type DecideArgs struct {
    Me  int
    Seq int
    Val interface{}
    Done int
}

type DecideReply struct {
    OK bool
    Done    int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) PrintStates() {
    fmt.Printf("I'm peer %d, my proposalState size:%d, keys: ", px.me, len(px.propState))
    for k := range px.propState {
        fmt.Printf("%d, ", k)
    }
    fmt.Printf("\n")
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()
    
    sender, seq, num := args.Me, args.Seq, args.Num        
    
    if seq > px.maxSeq {
        px.maxSeq = seq
    }
    
    //bug fixing
    if args.Done > px.p_done[sender] {
        px.p_done[sender] = args.Done
    }   
    //bug fixing
    if seq < px.Min() {
        reply.OK = false
        return nil
    }
    
    _, ok := px.propState[seq]
    if !ok {
        px.propState[seq] = ProposalState{-1, -1, nil, Pending, nil}
    }  
    
    nState := px.propState[seq]  
    fmt.Printf("I'm peer %d, got prepare from peer %d, n:%d\n", px.me, sender, num)
    if num > nState.nPrepare {
        nState.nPrepare = num
        px.propState[seq] = nState
        reply.OK = true
        reply.Num = num
        reply.NAccept = nState.nAccept
        reply.ValAccept = nState.valAccept
        reply.Done = px.done    
    }    
   
    return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
   
    px.mu.Lock()
    defer px.mu.Unlock()
       
    sender, seq, num, val := args.Me, args.Seq, args.Num, args.Val      
  
    px.p_done[sender] = args.Done
    
    nState := px.propState[seq]
    
    if num >= nState.nPrepare {
        nState.nPrepare = num
        nState.nAccept = num
        nState.valAccept = val
        px.propState[seq] = nState
        reply.OK = true
        reply.Num = num
        reply.Done = px.done
    }
    
    return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {    
    
    px.mu.Lock()
    defer px.mu.Unlock()
                
    sender, seq, nVal := args.Me, args.Seq, args.Val        
    
    px.p_done[sender] = args.Done  
    
    nState := px.propState[seq]
    nState.state = Decided
    nState.val = nVal
    
    px.propState[seq] = nState    
    reply.OK = true
    reply.Done = px.done
    
    px.ClearMem()
    return nil
}

func (px *Paxos) ClearMem() {
    min := px.Min()
    for i := range px.propState {
        if i < min {
            delete(px.propState, i)
        }
    }
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.          
    if seq < px.Min() {
        return
    }       
    time.Sleep(time.Second)
    go px.Propose(seq, v)   
}

func (px *Paxos) Propose(seq int, v interface{}) {
    
    rand.Seed(int64(px.me))    
    n := rand.Intn(10)
    
    majorityNum := len(px.peers) / 2 + 1    
    
    decided := false
            
    for !decided {            
    quitPrepare := make(chan int)
    
    okNum, rejectNum, nAccept := 0, 0, -1    
    var valAccept interface{}            
    
    for i, peer := range px.peers {                               
        go func(peer string, quitChannel chan int) {
            if _, err := os.Stat(peer); os.IsNotExist(err) {     
                fmt.Printf("I'm peer %d, inst:%d, n:%d, peer %v does not exist\n", px.me, seq, n, peer)           
                rejectNum++
            } else {
                p_args, p_reply := PrepareArgs{px.me, seq, n, px.done}, PrepareReply{}            
                ok := call(peer, "Paxos.Prepare", &p_args, &p_reply)
            
                for !ok {
                    time.Sleep(100 * time.Millisecond)
                    ok = call(peer, "Paxos.Prepare", &p_args, &p_reply)
                } 
                                                                                          
                px.p_done[i] = p_reply.Done                
                px.mu.Lock()                                               
                if p_reply.OK {
                    fmt.Printf("I'm peer %d, inst:%d, n:%d, got ok from peer %v\n", px.me, seq, n, peer)
                    okNum++
                    if p_reply.NAccept > nAccept {
                        nAccept = p_reply.NAccept
                        valAccept = p_reply.ValAccept
                    }
                } else {
                    fmt.Printf("I'm peer %d, inst:%d, n:%d, got reject from peer %v\n", px.me, seq, n, peer)
                    rejectNum++
                }
                px.mu.Unlock()                                            
            }
            if okNum == majorityNum || rejectNum == majorityNum {              
                quitChannel <- 0                                                   
            }                                                  
        }(peer, quitPrepare)
    }                 
    <- quitPrepare
        
    if rejectNum >= majorityNum || okNum < majorityNum{        
        n = n + 20
        if fate, _ := px.Status(seq);fate == Decided {
            decided = true
        }
        continue    
    }
                    
    if valAccept == nil {
        valAccept = v
    }            
    
    quitAccept := make(chan int)
    okNum, rejectNum = 0, 0
    for i, peer := range px.peers {
        go func(peer string, quitChannel chan int) {
            if _, err := os.Stat(peer); os.IsNotExist(err) {            
                rejectNum++
            } else {
                a_args, a_reply:= AcceptArgs{px.me, seq, n, valAccept, px.done}, AcceptReply{}
                ok := call(peer, "Paxos.Accept", &a_args, &a_reply)
            
                for !ok {
                    time.Sleep(100 * time.Millisecond)
                    ok = call(peer, "Paxos.Accept", &a_args, &a_reply)
                }
                        
                px.p_done[i] = a_reply.Done
                px.mu.Lock()                
                if a_reply.OK {
                    okNum++
                } else {
                    rejectNum++
                }                
                px.mu.Unlock()                           
            }
            if okNum == majorityNum || rejectNum == majorityNum {
                quitChannel <- 0                
            }   
        }(peer, quitAccept)
    }
    
    <- quitAccept
           
    if rejectNum >= majorityNum || okNum < majorityNum {        
        n = n + 20
        if fate, _ := px.Status(seq);fate == Decided {
            decided = true
        }
        continue
    }
    
    d_args, d_reply := DecideArgs{px.me, seq, valAccept, px.done}, DecideReply{}
    
    for _, peer := range px.peers {
        if _, err := os.Stat(peer); os.IsNotExist(err) {
            continue
        }          
        ok := call(peer, "Paxos.Decide", &d_args, &d_reply)
        
        for !ok {
            ok = call(peer, "Paxos.Decide", &d_args, &d_reply)
        }
        
    }
    fmt.Printf("I'm peer %d, decide on inst %d, val:%v\n", px.me, seq, valAccept)   
    decided = true    
    }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
    if seq > px.done {
        px.done = seq
    }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.    
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
    
    min := 0x7fffffff
    
    for i := range px.p_done {
        if px.p_done[i] < min {
            min = px.p_done[i]
        }
    }
      
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
    _, ok := px.propState[seq]
    if !ok {        
        return Forgotten, nil
    }
    result := px.propState[seq]
	return result.state, result.val
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
    

	// Your initialization code here.
    px.propState = make(map[int]ProposalState)
    px.maxSeq = -1
    px.done = -1
    px.p_done = make([]int, len(px.peers))
    for i := range px.p_done {
        px.p_done[i] = -1
    }

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
