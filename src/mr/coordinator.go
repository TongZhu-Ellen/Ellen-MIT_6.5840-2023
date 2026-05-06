package mr

import (
    "log"
    "net"
    "os"
    "sync"
    "time"
    "net/http"
    "net/rpc"
)

type TaskStatus int
const (
    Todo TaskStatus = iota
    Doing
    Done
)

type MapTask struct {
	filename string 
	status TaskStatus
	assignedAt time.Time
}

type ReduceTask struct {
	status TaskStatus 
	assignedAt time.Time
}

type Coordinator struct {
	mu  sync.Mutex
	phase       int // 0=map, 1=reduce, 2=done

	mapTask []*MapTask // size X+1 
	reduceTask []*ReduceTask // size Y+1
	capX int 
	capY int 

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(fnames []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	fileNum := len(fnames) // X

	c.mapTask = make([]*MapTask, fileNum + 1)
	c.reduceTask = make([]*ReduceTask, nReduce + 1)

	x := 1
	for _, fname := range fnames {
		c.mapTask[x] = &MapTask{filename: fname}
		x++
	}

	c.capX = fileNum
	c.capY = nReduce


	c.server()
	return &c
}

func (c *Coordinator) fillReduce() {
	for y := 1; y <= c.capY; y++ {
		c.reduceTask[y] = &ReduceTask{}
	}
}


func (c *Coordinator) RequestTask(np *Empty, tap *TaskAssignment) error {

	// some general info first
	tap.X = c.capX
	tap.Y = c.capY

	// 加锁！	
	c.mu.Lock()
	defer c.mu.Unlock()

	x := 0
	y := 0


	if c.phase == 0 {
		x = c.fetchMap()
	}
	if c.phase == 1 {
		y = c.fetchReduce()
	}
	if c.phase == 2 {
		tap.TaskType = TaskExit
		return nil
	}

	if x > 0 {
		tap.TaskType = TaskMap
		tap.Filename = c.mapTask[x].filename
		tap.LowerX = x 

		c.mapTask[x].status = Doing
		c.mapTask[x].assignedAt = time.Now()
		return nil
	}
	if y > 0 {
		tap.TaskType = TaskReduce
		tap.LowerY = y 

		c.reduceTask[y].status = Doing
		c.reduceTask[y].assignedAt = time.Now()
		return nil
	}

	// TaskNone
	return nil

	


	

}

// helper函数们本身不是线程安全的。而且本身也并不检查phase！说白了只是一个循环检查的机制罢了！
func (c *Coordinator) fetchMap() int {
	taskLeft := c.capX
	

	for x := 1; x <= c.capX; x++ {
		tp := c.mapTask[x]
		switch tp.status {
		case Todo:
			return x
		case Doing:
			if time.Since(tp.assignedAt) > 15*time.Second {
				tp.status = Todo
				return x
			}
		case Done:
			taskLeft--
		}
	}
	
	if taskLeft == 0 {
		c.phase = 1
		c.fillReduce()

	}

	return 0
	
}

func (c *Coordinator) fetchReduce() int {
	taskLeft := c.capY
	
	for y := 1; y <= c.capY; y++ {
		tp := c.reduceTask[y]
		switch tp.status {
		case Todo:
			return y
		case Doing:
			if time.Since(tp.assignedAt) > 10*time.Second {
				tp.status = Todo
				return y
			}
		case Done:
			taskLeft--
		}
	}
	if taskLeft == 0 {
		c.phase = 2
	}

	return 0
}

func (c *Coordinator) ResponseTask(tcp *TaskCompletion, np *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if tcp.LowerX != 0 && c.mapTask[tcp.LowerX].status != Done {
		c.mapTask[tcp.LowerX].status = Done
		return nil
	} 
	
	if tcp.LowerY != 0 && c.reduceTask[tcp.LowerY].status != Done{
		c.reduceTask[tcp.LowerY].status = Done
		return nil
	}

	return nil  // 不该出现的情况
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == 2
}


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


