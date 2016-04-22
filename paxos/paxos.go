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
import "strconv"
import "io/ioutil"


func (px *Paxos) DPrintf(format string, a ...interface{}) (n int, err error) {
	if px.Ds {
		fmt.Printf(format, a...)
	}
	return
}

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
    max_seq      int
    done        int
    p_done      []int
    instances   map[int]*InstanceState 
    nMajority   int
    Ds          bool
}

type InstanceState struct {
    promised_n  string
    accepted_n  string
    accepted_v  interface{} 
    status      Fate    
}

type PrepareArgs struct {
    Seq         int
    ProposalNum string   
}

type PrepareReply struct {
    Promised    bool
    AcceptedN   string
    AcceptedV   interface{}
}

type AcceptArgs struct {    
    Seq         int
    ProposalNum string
    AcceptedV   interface{}   
}

type AcceptReply struct {
    Accepted    bool
}

type DecideArgs struct {
    Me      int
    Seq     int
    ProposalNum string
    Val     interface{}
    Done    int
}

type DecideReply struct {    
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
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
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


func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()
    reply.Promised = false
    promised := false    
    
    if _, exist := px.instances[args.Seq]; exist {
        if px.instances[args.Seq].promised_n < args.ProposalNum {
            promised = true
        }
    } else {
        px.instances[args.Seq] = &InstanceState{
            promised_n: "",
            accepted_n: "",
            accepted_v: nil,
            status:     Pending,
        }
        promised = true
    }
    if promised {
        px.instances[args.Seq].promised_n = args.ProposalNum
        reply.Promised = true
        reply.AcceptedN = px.instances[args.Seq].accepted_n
        reply.AcceptedV = px.instances[args.Seq].accepted_v
    }
    return nil    
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
   
    px.mu.Lock()
    defer px.mu.Unlock()
       
    if _, exist := px.instances[args.Seq]; !exist {
        px.instances[args.Seq] = &InstanceState{
            promised_n: "",
            accepted_n: "",
            accepted_v: nil,
            status    : Pending,
        }
    }      
    
    if args.ProposalNum >= px.instances[args.Seq].promised_n {
        px.instances[args.Seq].promised_n = args.ProposalNum
        px.instances[args.Seq].accepted_n = args.ProposalNum
        px.instances[args.Seq].accepted_v = args.AcceptedV
        px.instances[args.Seq].status = Pending
        reply.Accepted = true      
    } else {
        reply.Accepted = false
    }
    return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {    
    
    px.mu.Lock()
    defer px.mu.Unlock()
    
    if args.Seq > px.max_seq {
        px.max_seq = args.Seq
    }
    
    px.p_done[args.Me] = args.Done
                
    if _, exist := px.instances[args.Seq]; exist {
        px.instances[args.Seq].accepted_n = args.ProposalNum
        px.instances[args.Seq].accepted_v = args.Val
        px.instances[args.Seq].status = Decided
    } else {
        px.instances[args.Seq] = &InstanceState{
            promised_n: "",
            accepted_n: args.ProposalNum,
            accepted_v: args.Val,
            status    : Decided,
        }
    }        
         
    reply.Done = px.done
    
    px.ClearMem()
    return nil
}

func (px *Paxos) ClearMem() {
    min := px.Min()
    for i := range px.instances {
        if i < min {
            delete(px.instances, i)
        }
    }
}

func generateIncreasingNum(me int) string {
    return strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.Itoa(me) 
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
    go func() {
        if seq < px.Min() {
            return
        }
        decided := false
        for !decided {
            proposal_num := generateIncreasingNum(px.me)
            nPrepared, accepted_v := px.sendPrepare(seq, proposal_num, v)
            if nPrepared >= px.nMajority {
                nAccepted := px.sendAccept(seq, proposal_num, accepted_v)
                if nAccepted >= px.nMajority {
                    px.sendDecide(seq, proposal_num, accepted_v)
                    decided = true
                } else {
                    time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
                }
            } else {
                time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
            }
        }
    }()
}

func (px *Paxos) sendPrepare(seq int, proposal_num string, v interface{}) (int, interface{}) {    
    nPrepared := 0
    args := PrepareArgs{seq, proposal_num}
    accepted_n := ""
    var accepted_v interface{}
    for i, peer := range px.peers {              
        var reply PrepareReply          
        ok := false 
        if i != px.me {
            ok = call(peer, "Paxos.Prepare", &args, &reply)
        } else {
            ok = px.Prepare(&args, &reply) == nil
        }
        if ok && reply.Promised {
            if reply.AcceptedN > accepted_n {
                accepted_n = reply.AcceptedN
                accepted_v = reply.AcceptedV
            }
            nPrepared++                
        }        
    }    
    if accepted_v == nil {
        accepted_v = v
    }    
    return nPrepared, accepted_v
}

func (px *Paxos) sendAccept(seq int, proposal_num string, v interface{}) int {
    nAccepted := 0
    args := AcceptArgs{seq, proposal_num, v}
    for i, peer := range px.peers {
        reply := AcceptReply{Accepted: false}
        ok := false 
        if i != px.me {
            ok = call(peer, "Paxos.Accept", &args, &reply)
        } else {
            ok = px.Accept(&args, &reply) == nil
        }
        if ok && reply.Accepted {
            nAccepted++
        }
    }
    return nAccepted
}

func (px *Paxos) sendDecide(seq int, proposal_num string, v interface{}) {
    all_decided := false
    args := DecideArgs{px.me, seq, proposal_num, v, px.done}
    for !all_decided {
		all_decided = true
		for i, server := range px.peers {
			var reply DecideReply
			ret := false
			if i != px.me {
				ret = call(server, "Paxos.Decide", &args, &reply)
			} else {
				ret = px.Decide(&args, &reply) == nil
			}
			if !ret {
				all_decided = false
			} else {
                px.p_done[i] = reply.Done
			}
		}
		if !all_decided {
			time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
		}
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
	return px.max_seq
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
// should just inspect the local peer State;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
    _, ok := px.instances[seq]
    if !ok {        
        return Forgotten, nil
    }
    result := px.instances[seq]
	return result.status, result.accepted_v
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
    log.SetOutput(ioutil.Discard)

	// Your initialization code here.
    px.instances = make(map[int]*InstanceState)
    px.max_seq = -1
    px.done = -1
    px.p_done = make([]int, len(px.peers))
    for i := range px.p_done {
        px.p_done[i] = -1
    }
    px.Ds = false
    px.nMajority = len(px.peers) / 2 + 1

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
