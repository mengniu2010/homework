package mr

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	log "6.824/mylog"

	"github.com/google/uuid"
)

var reduceJobOnce = sync.Once{}

const (
	JobQueueSize = 10
	JobTimeout   = 10 * time.Second
	JobMaxAge    = 10
)

type Coordinator struct {
	MapJobChan         chan *Job
	ReduceJobChan      chan *Job
	statusMu           *sync.RWMutex
	intermediateDataMu *sync.RWMutex
	MapWg              *sync.WaitGroup
	ReduceWg           *sync.WaitGroup
	MapSpliter         Spliter
	Stage              int64 // 0 for map stage, 1 for reduce stage, 2 for all job done
	nReduce            int
	JobStatus          map[string]*Job
	IntermediateData   [][]string
	InputFiles         []string
}

func NewCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := &Coordinator{
		MapJobChan:         make(chan *Job, JobQueueSize),
		ReduceJobChan:      make(chan *Job, nReduce),
		statusMu:           &sync.RWMutex{},
		intermediateDataMu: &sync.RWMutex{},
		MapWg:              &sync.WaitGroup{},
		ReduceWg:           &sync.WaitGroup{},
		MapSpliter:         *NewChunkSpliter(100),
		Stage:              0,
		nReduce:            nReduce,
		JobStatus:          map[string]*Job{},
		IntermediateData:   [][]string{},
		InputFiles:         files,
	}
	for i := 0; i < nReduce; i++ {
		coordinator.IntermediateData[i] = make([]string, 0, 10)
	}
	go func(files []string) {
		for _, file := range files {
			specs, err := coordinator.MapSpliter.Split(file)
			if err != nil {
				log.Errorf("cannot split %s with %v, error %s",
					file,
					coordinator.MapSpliter.Name,
					err)
				continue
			}
			coordinator.MapWg.Add(len(specs))
			for _, spec := range specs {
				job := Job{
					ID:      uuid.New().String(),
					Typ:     MapType,
					Data:    *spec,
					NReduce: nReduce,
					Status:  StatutPending,
				}
				coordinator.MapJobChan <- &job
			}
		}
	}(files)
	return coordinator
}

func (c *Coordinator) updateToDistributeJobStatusAndSetTimer(job *Job, jobChan chan<- *Job) {
	if job == nil {
		return
	}
	c.statusMu.Lock()
	note, ok := c.JobStatus[job.ID]
	if !ok {
		c.JobStatus[job.ID] = &Job{
			Status:  StatusWorking,
			ID:      job.ID,
			mu:      &sync.Mutex{},
			NReduce: job.NReduce,
			Age:     job.Age,
			Typ:     job.Typ,
			Data:    job.Data,
		}
		note = c.JobStatus[job.ID]
	}
	c.statusMu.Unlock()

	if job.Age > JobMaxAge {
		log.Panicf("job %v exceed max age %d", *job, JobMaxAge)
	}
	job.Status = StatusWorking

	go func(note *Job, distributedJob *Job, d time.Duration, jobChan chan<- *Job) {
		time.Sleep(d)
		note.mu.Lock()
		// TOOD add isSameStatus func
		if note.Age == distributedJob.Age && errors.Is(StatusWorking, note.Status) {
			note.Status = StatusWorking
			note.mu.Unlock()
			distributedJob.Age += 1
			distributedJob.Status = StatusWorking
			jobChan <- distributedJob
		} else {
			note.mu.Unlock()
		}
	}(note, job, JobTimeout, jobChan)
}

func (c *Coordinator) pollJob(jobChan <-chan *Job, timeout time.Duration, workername string) (*Job, error) {
	var job *Job
	var err error
	select {
	case job = <-jobChan:
	case <-time.After(timeout):
		err = fmt.Errorf("no more jobs after %d, %w", timeout, io.EOF)
		return nil, err
	}
	c.updateToDistributeJobStatusAndSetTimer(job, c.MapJobChan)
	return job, nil
}

func (c *Coordinator) transferStage(from, to int64, workername string) (changed bool) {
	changed = atomic.CompareAndSwapInt64(&c.Stage, from, to)
	if changed {
		log.Infof("coordinator transfer stage from %d to %d by %s's requst", from, to, workername)

	} else {
		log.Infof("coordinator transfer stage from %d to %d by %s's requst, now it's %d",
			from,
			to,
			workername,
			atomic.LoadInt64(&c.Stage))
	}
	return changed
}

func (c *Coordinator) copyToReply(job *Job, reply *RequestJobReply) {
	if job == nil || reply == nil {
		log.Panicf("copyToReply nil, job %v, reply %v", job, reply)
	}
	reply.Job.ID = job.ID
	reply.Job.Age = job.Age
	reply.Job.Data = job.Data
	reply.Job.NReduce = job.NReduce
	reply.Job.Status = job.Status
	reply.Job.Typ = job.Typ
}

func (c *Coordinator) NewNoJob() *Job {
	return &Job{
		NReduce: c.nReduce,
		Age:     0,
		Typ:     NoJobType,
		Status:  StatutPending,
		ID:      "",
		Data:    DataSpec{},
	}
}

func (c *Coordinator) OfferJob(args *RequestJobArg, reply *RequestJobReply) error {

	var job *Job
	stage := atomic.LoadInt64(&c.Stage)
	fmt.Println(job, stage)
	if stage == 0 {
		job, err := c.pollJob(c.MapJobChan, 2*JobTimeout, args.WorkerName)
		if job == nil && err != nil && errors.As(io.EOF, err) {
			c.transferStage(0, 1, args.WorkerName)
		}
		if job == nil {
			reply.Job.Typ = NoJobType
			reply.Job.ID = ""
			return nil
		}
		c.copyToReply(job, reply)
	} else if stage == 2 {
		job, err := c.pollJob(c.ReduceJobChan, 2*JobTimeout, args.WorkerName)
		if job == nil && err != nil && errors.As(io.EOF, err) {
			c.transferStage(1, 2, args.WorkerName)
		}
		if job == nil {
			reply.Job.Typ = NoJobType
			reply.Job.ID = ""
			return nil
		}
		c.copyToReply(job, reply)
	}
	if job == nil {
		job = c.NewNoJob()
	}
	reply.Job = *job
	return nil
}

// func (c *)
func (c *Coordinator) CollectResult(arg *AckJobArg, reply *AckJobReply) {
	c.statusMu.RLock()
	jobNote, ok := c.JobStatus[arg.Job.ID]
	c.statusMu.Unlock()
	if !ok {
		log.Warnf("received unkonwn job ack %v", arg)
		return
	}
	jobNote.mu.Lock()
	if jobNote.Status != StatusWorking && jobNote.Age != arg.Job.Age {
		log.Warnf("inconsistent job ack, note %v, ack %v", *jobNote, arg)
		jobNote.mu.Unlock()
		return
	} else {
		jobNote.mu.Unlock()
	}
	jobNote.Status = StatusSuccess
	c.MapWg.Done()
	c.ReduceWg.Add(1)
	go c.cacheIntermediateFiles(arg.ResultFiles)
	go c.givenReduceJobs()

}

func (c *Coordinator) cacheIntermediateFiles(resultFiles map[int]string) {
	defer c.ReduceWg.Done()
	c.intermediateDataMu.Lock()
	defer c.intermediateDataMu.Unlock()
	for k := range resultFiles {
		c.IntermediateData[k] = append(c.IntermediateData[k], resultFiles[k])
	}
}

func (c Coordinator) concatenateFiles(files []string, catfile string) (string, int64) {
	dirPath := "./coordinator"
	err := os.MkdirAll(dirPath, 0640)
	if err != nil {
		log.Panicf("cannot mkdir for coordinator")
	}
	concatPath := path.Join(dirPath, catfile)
	concat, err := os.OpenFile(concatPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	if err != nil {
		log.Panicf("canont create file %d", concat)
	}
	defer concat.Close()
	var concatLen int64 = 0
	for _, f := range files {
		src, err := os.OpenFile(f, os.O_RDONLY, 0)
		if err != nil {
			log.Panicf("cannot read %s", f)
		}
		n, err := io.Copy(concat, src)
		if err != nil {
			log.Panicf("cannot copy from %s to %s", f, concatPath)
		}
		concatLen += n
		src.Close()
	}
	return concatPath, concatLen
}

func (c *Coordinator) givenReduceJobs() {
	if atomic.LoadInt64(&c.Stage) != 1 {
		return
	}
	reduceJobOnce.Do(func() {
		c.MapWg.Wait()
		c.intermediateDataMu.Lock()
		defer c.intermediateDataMu.Unlock()
		for k := range c.IntermediateData {
			files := c.IntermediateData[k]
			concatPath, concatLen := c.concatenateFiles(
				files,
				fmt.Sprintf("concatIntermediate-%d.txt", k),
			)
			reduceJob := &Job{
				NReduce: c.nReduce,
				Age:     0,
				Typ:     ReduceType,
				Status:  StatutPending,
				ID:      uuid.NewString(),
				Data: DataSpec{
					Offset:   0,
					Len:      concatLen,
					Filename: concatPath,
					Whence:   0,
				},
			}
			c.ReduceJobChan <- reduceJob
		}
		c.IntermediateData = make([][]string, 0)
	})

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error: %v", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := atomic.LoadInt64(&c.Stage) == 2
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
