package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	inFiles := make([]*os.File, nMap)
	for i:=0; i<nMap; i++ {
		reduceName := reduceName(jobName, i, reduceTask)
		file,err := os.Create(reduceName)
		if err != nil {
			fmt.Printf("error happens when create input file of reductTask:{%d}, error is {%v} \n", reduceTask, err)
			return
		}
		inFiles[i] = file
	}
	kvMap := make(map[string][]string)
	for _,file := range inFiles {
		dec := json.NewDecoder(file)
		for dec.More() {
			kv := new(KeyValue)
			err := dec.Decode(kv)
			if err != nil {
				fmt.Printf("error happens when decode from intermediate input file:{%v}, error is {%v} \n", file, err)
				break
			}
			values := kvMap[kv.Key]
			if values == nil {
				values = make([]string, 10)
			}
			values = append(values, kv.Value)
			kvMap[kv.Key] = values
		}
	}

	oFile,err := os.Create(outFile)
	if err != nil {
		fmt.Printf("error happens when create output file of reductTask:{%d}, error is {%v} \n", reduceTask, err)
		return
	}
	enc := json.NewEncoder(oFile)
	for key,values := range kvMap {
		result := reduceF(key, values)
		err := enc.Encode(KeyValue{key, result})
		if err != nil {
			fmt.Printf("error happens when create encode result of reductTask:{%d} to outFile:{%s}, error is {%v} \n", reduceTask, outFile, err)
		}
	}

	defer func() {
		for _,file := range inFiles {
			_ = file.Close()
		}
		_ = oFile.Close()
	}()

	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
