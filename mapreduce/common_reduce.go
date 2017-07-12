package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// Opening all temp files
	logInfo(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Starting to process %d files", nMap))
	var decoders = make([]*json.Decoder, nMap)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		fd, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
		if err != nil {
			logFatal(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Failed to open: %s", fileName))
			return
		}
		decoders[i] = json.NewDecoder(fd)
		defer fd.Close()
	}

	kvs := make(map[string][]string)

	// Unmarshal all temp files and collate key-values
	for i := 0; i < nMap; i++ {
		var kv *KeyValue
		for {
			err := decoders[i].Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	logInfo(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Read %d keys from %d files", len(kvs), nMap))

	// Sort by key
	logInfo(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Sorting data by keys"))
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create output file
	fd, err := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		logFatal(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Failed to open: %s", outFile))
		return
	}
	defer fd.Close()

	// Apply reduce f() and write results
	logInfo(jobName, reducePhase, reduceTaskNumber, fmt.Sprintf("Applying reduce f() and writing to output file: %s", outFile))
	encoder := json.NewEncoder(fd)
	for _, key := range keys {
		encoder.Encode(KeyValue{key, reduceF(key, kvs[key])})
	}
}
