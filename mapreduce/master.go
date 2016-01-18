package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
    mr.Workers = make(map[string]*WorkerInfo)
    flag := true
    
    quitRegister := make(chan int)
    mr.availableWorkers = make(chan string, 10)    
    go func() {
        for {
            fmt.Printf("Finding workers...\n")
            if !flag {
                break
            }
            select {
                case worker := <-mr.registerChannel :
                    mr.Workers[worker] = &WorkerInfo{worker}
                    fmt.Printf("Register new worker: %s\n", worker)
                    mr.availableWorkers <- worker
                case <-quitRegister:
                    flag = false
                    break
            }
        }
    }()
    
    
    quitMap := make(chan int)
    
    //fmt.Printf("# availableWorkers = %d\n", len(mr.Workers))
    //for k := range mr.Workers {
      //  mr.availableWorkers <- k
    //}    
    mapJobNum, reduceJobNum := 0, 0
    for mapJobNum < mr.nMap{                         
        worker := <- mr.availableWorkers
        var args *DoJobArgs
        args = &DoJobArgs{mr.file, Map, mapJobNum, mr.nReduce}
        mapJobNum++
        var reply DoJobReply                
        go func(currentMapJob int) {
            for !reply.OK {
                ok := call(mr.Workers[worker].address, "Worker.DoJob", args, &reply)
                if ok == false {
                    fmt.Printf("DoWork: RPC %s DoJob error (Map)\n", mr.Workers[worker].address)
                    worker = <-mr.availableWorkers
                } else {
                    if reply.OK {
                        mr.availableWorkers <- worker
                        if mapJobNum == mr.nMap {
                            quitMap <- 0
                        }
                    }
                }
            }
        }(mapJobNum - 1)                   
    }
    <- quitMap
    
    quitReduce := make(chan int)
    
    for reduceJobNum < mr.nReduce {                         
        worker := <- mr.availableWorkers
        var args *DoJobArgs
        args = &DoJobArgs{mr.file, Reduce, reduceJobNum, mr.nMap}
        reduceJobNum++
        var reply DoJobReply                
        go func(currentReduceJob int) {
            for !reply.OK { 
                ok := call(mr.Workers[worker].address, "Worker.DoJob", args, &reply)
                if ok == false {
                    fmt.Printf("DoWork: RPC %s DoJob error\n", mr.Workers[worker].address)
                    worker = <-mr.availableWorkers
                } else if reply.OK{
                    mr.availableWorkers <- worker
                    if reduceJobNum == mr.nReduce {
                        quitReduce <- 0
                    }  
                }
            }
        }(reduceJobNum - 1)                   
    }
    <- quitReduce
    
    quitRegister <- 0
        
	return mr.KillWorkers()
}

