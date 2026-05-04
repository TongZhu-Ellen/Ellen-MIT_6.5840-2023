package mr

import (
    "encoding/json"
    "fmt"
    "hash/fnv"
    "io"
    "io/ioutil"
    "log"
    "net/rpc"
    "os"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func yOf(key string, Y int) int {
	return ihash(key) % Y + 1
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	exit := false
	wait := false

	for !exit {
		if wait {
			time.Sleep(time.Second)
		}
		exit, wait = eachCall(mapf, reducef)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 返回eachCall所造成的线程后果：结束？等待？
func eachCall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) (bool, bool) {
	tap := &TaskAssignment{}
	called := call("Coordinator.RequestTask", &Empty{}, tap)
	var err error

	if !called {
		return false, true
	}

	switch tap.TaskType {
	case TaskExit:
		return true, false
	case TaskNone:
		return false, true
	case TaskMap:
		err = mapper(tap.Filename, tap.LowerX, tap.Y, mapf)
	case TaskReduce:
		err = reducer(tap.LowerY, tap.X, reducef)
	}

	if err != nil {
		log.Println(err)
	} else {
		tcp := &TaskComplete{TaskComplete: true}
		call("Coordinator.ResponseTask", tcp, &Empty{})
	}

	return false, false
}







func mapper(
	filename string,
	x int,
	Y int,
	mapf func(string, string) []KeyValue,
) error {

	// open txt!
	contentBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	// get kvs!
	kvs := mapf(filename, string(contentBytes))

	// divide kvs! (分桶)
	mp := make(map[int][]KeyValue)
	for _, kv := range kvs {
		y := yOf(kv.Key, Y)
		mp[y] = append(mp[y], kv)
	}

	// write kvs into intermedia files!
	for y := 1; y <= Y; y++ {
		if err := intermediateFileWriter(fmt.Sprintf("mr-%d-%d", x, y), mp[y]); err != nil {
			return err
		}
	}

	// no problem!
	return nil

}

func intermediateFileWriter(filename string, kvs []KeyValue) error {

	// create file!
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// write file!
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			return err
		}
	}

	// return nil!
	return nil

}

func reducer(
	y int,
	X int,
	reducef func(string, []string) string,
) error {

	// prepair our map[key][]val!
	mp := make(map[string][]string)
	for x := 1; x <= X; x++ {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", x, y))
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				break
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}

	// create finalFile!
	finalFile, err := os.Create(fmt.Sprintf("mr-out-%d", y-1)) // y-1 here!
	if err != nil {
		return err
	}
	defer finalFile.Close()

	// fill finalFile!
	for key, vals := range mp {
		output := reducef(key, vals)
		if err := fmt.Fprintf(finalFile, "%v %v\n", key, output); err != nil {
			return err
		}
	}

	// finish!
	return nil

}



//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
