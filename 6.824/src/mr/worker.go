package mr

import (
	"fmt"
	"hash/fnv"
	"net/rpc"
	"strings"
	"time"

	log "6.824/mylog"
	"github.com/google/uuid"
)

const MaxWorkerLoop = 100

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type mapfnT func(string, string) []KeyValue
type reducefnT func(string, []string) string
type MyWorker struct {
	Name    string
	Mapf    mapfnT
	Reducef reducefnT
}

func NewMyWorker(mapf mapfnT, reducef reducefnT) *MyWorker {
	return &MyWorker{
		Name:    uuid.New().String(),
		Mapf:    mapf,
		Reducef: reducef,
	}
}
func (w *MyWorker) requestJob() *Job {
	args := RequestJobArg{WorkerName: w.Name}
	reply := RequestJobReply{
		Job: Job{
			ID:   "",
			Typ:  NoJobType,
			Data: DataSpec{},
		},
	}
	call("Coordinator.OfferJob", &args, &reply)
	return &reply.Job
}

func (w *MyWorker) ackJob(job Job, resultFiles []string, err error) {
	args := AckJobArg{
		Job:         job,
		ResultFiles: resultFiles,
		Err:         err,
	}
	reply := AckJobReply{}
	call("Coordinator.CollectIntermediate", &args, &reply)
}

func (w *MyWorker) run(MaxNoJob int) {
	noJobCnt := 0
workloop:
	for {
		job := w.requestJob()
		switch job.Typ {
		case NoJobType:
			noJobCnt += 1
			if noJobCnt >= MaxWorkerLoop {
				break workloop
			}
			time.Sleep(time.Second * 5)
		case StopWorkerType:
			break workloop
		case MapType:
			noJobCnt = 0
			content, err := job.Data.ReadData()
			if err != nil {
				log.Warnf("fail to read data in map job %v, data %v", err, job.Data)
				w.ackJob(*job, []string{}, err)
				break
			}
			intermediate := w.Mapf(job.Data.Filename, content)
			filenames, err := job.writeIntermediate(intermediate, w.Name)
			if err != nil {
				log.Warnf("fail to write map result %v, data %v", err, job)
				w.ackJob(*job, []string{}, err)
				break
			}
			w.ackJob(*job, filenames, nil)
		case ReduceType:
			noJobCnt = 0
			noJobCnt = 0
			content, err := job.Data.ReadData()
			if err != nil {
				log.Warnf("fail to read data in reduce job %v, data %v", err, job.Data)
				w.ackJob(*job, []string{}, err)
				break
			}
			lines := strings.Split(content, "\n")
			results := make([]KeyValue, 0, len(lines))
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				key, values := job.deserializeIntermediate(content)
				result := w.Reducef(key, values)
				results = append(results, KeyValue{key, result})
			}
			resultFile, err := job.writeReduceResults(results, w.Name)
			if err != nil {
				log.Warnf("fail to write map result %v, data %v", err, job)
				w.ackJob(*job, []string{}, err)
				break
			}
			w.ackJob(*job, []string{resultFile}, nil)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker := NewMyWorker(mapf, reducef)

	worker.run(MaxWorkerLoop)
	// workerName := uuid.New()
	// noJobCnt := 0

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("dialing: %v", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
