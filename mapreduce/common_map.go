package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	logInfo(jobName, mapPhase, mapTaskNumber, fmt.Sprintf("Processing file: %s", inFile))
	data, err := ioutil.ReadFile(inFile)
	if err != nil {
		logFatal(jobName, mapPhase, mapTaskNumber, fmt.Sprintf("Failed to read input: %s", inFile))
	}

	// Opening all temp files
	var encoders = make([]*json.Encoder, nReduce)
	var fd *os.File = nil
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTaskNumber, i)
		fd, err = os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			logFatal(jobName, mapPhase, mapTaskNumber, fmt.Sprintf("Failed to open: %s", fileName))
			return
		}
		encoders[i] = json.NewEncoder(fd)
		defer fd.Close()
	}

	// Apply map f()
	kvs := mapF(inFile, string(data))
	logInfo(jobName, mapPhase, mapTaskNumber, fmt.Sprintf("Applied map f(), returned %d key-values", len(kvs)))

	// Marshal and write all K-V pairs to temp files
	for _, kv := range kvs {
		r := ihash(kv.Key) % nReduce
		err = encoders[r].Encode(kv)
		if err != nil {
			logFatal(jobName, mapPhase, mapTaskNumber, fmt.Sprintf("Failed to marshal/write k: %s, v: %s", kv.Key, kv.Value))
			return
		}
	}
	logInfo(jobName, mapPhase, mapTaskNumber, fmt.Sprintf("Processed file: %s", inFile))
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
