package mr

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	log "6.824/mylog"
)

type JobType int

const (
	NoJobType JobType = iota
	MapType
	ReduceType
	StopWorkerType
)

type JobStatus error

var (
	StatusSuccess JobStatus = nil
	StatutPending JobStatus = fmt.Errorf("job pending")
	StatusWorking JobStatus = fmt.Errorf("job working")
	StatusTimeout JobStatus = fmt.Errorf("job timeout")
	StatusFail    JobStatus = fmt.Errorf("job fail")
)

type DataSpec struct {
	Offset   int64
	Len      int64
	Filename string
	Whence   int
}

type Job struct {
	NReduce int
	Age     int
	Typ     JobType
	Status  JobStatus
	ID      string
	Data    DataSpec
	mu      *sync.Mutex // only works in coordinator's status map
}

func (data *DataSpec) ReadData() (string, error) {
	file, err := os.OpenFile(data.Filename, os.O_RDONLY, 0)
	if err != nil {
		log.Errorf("fail to open data %v", err)
		return "", fmt.Errorf("%w with %v", err, data)
	}
	defer file.Close()
	_, err = file.Seek(data.Offset, data.Whence)
	if err != nil {
		log.Errorf("fail to seek %v, error %v", data, err)
		return "", fmt.Errorf("%w with %v", err, data)
	}
	buff := make([]byte, data.Len)
	n, err := file.Read(buff)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("%w with %v", err, data)
	}
	if n != int(data.Len) {
		log.Warnf("expect len %d, got %d, dataspec %v", data.Len, n, data)
	}
	return string(buff), nil
}

func (job *Job) getIntermediateFileName(key string, workername string) string {
	return path.Join(fmt.Sprintf("./worker-%s/intermediate-job-%s", workername, job.ID), fmt.Sprintf("%d.txt", ihash(key)))
}

func (job *Job) getReduceResultFileName(workername string) string {
	return path.Join(fmt.Sprintf("./worker-%s/reduce-job-%s", workername, job.ID))
}

func (job *Job) serializeIntermediate(key string, values []string) string {
	words := make([]string, 0, len(values)+1)
	words = append(words, key)
	words = append(words, values...)
	return strings.Join(words, " ")
}

func (job *Job) deserializeIntermediate(content string) (key string, values []string) {
	words := strings.Split(content, " ")
	key = words[0]
	values = words[1:]
	return key, values
}

func (job *Job) parentDirMustExist(filepath string) {
	dirpath := path.Dir(filepath)
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		err = os.MkdirAll(dirpath, 0640)
		if err != nil {
			log.Panicf("mkdir for results, %w", err)
		}
	}
}

func (job *Job) writeIntermediateToFile(key string, values []string, workername string) (string, error) {
	filepath := job.getIntermediateFileName(key, workername)
	job.parentDirMustExist(filepath)
	ofile, err := os.OpenFile(filepath, os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return "", err
	}
	defer ofile.Close()
	n, err := fmt.Fprintln(ofile, job.serializeIntermediate(key, values))
	if n < 0 || err != nil {
		return "", err
	}
	return filepath, err
}

func (job *Job) writeReduceResults(results []KeyValue, workername string) (map[int]string, error) {
	filepath := job.getReduceResultFileName(workername)
	job.parentDirMustExist(filepath)
	ofile, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	if err != nil {
		return map[int]string{}, err
	}
	defer ofile.Close()
	for _, kv := range results {
		fmt.Fprintf(ofile, "%s %s\n", kv.Key, kv.Value)
	}
	result := make(map[int]string, 1)
	result[0] = filepath
	return result, nil

}
func (job *Job) writeIntermediate(intermediate []KeyValue, workername string) (map[int]string, error) {
	if len(intermediate) == 0 {
		return map[int]string{}, nil
	}
	filenames := make(map[int]string, job.NReduce)
	sort.Sort(ByKey(intermediate))
	// keys are sorted, only one file handler need at a time.
	var i int
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) {
			if intermediate[i] != intermediate[j] {
				break
			}
			j += 1
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		filename, err := job.writeIntermediateToFile(intermediate[i].Key, values, workername)
		if err != nil {
			return map[int]string{}, nil
		}
		filenames[ihash(intermediate[i].Key)] = filename
		i = j
	}
	return filenames, nil
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
