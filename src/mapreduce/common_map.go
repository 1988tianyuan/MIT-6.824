package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	inBytes,err := ioutil.ReadFile(inFile)
	var contents string
	var resultKV []KeyValue
	if err != nil {
		fmt.Println("error happens when reading file of mapTask: ", err)
	} else {
		contents = string(inBytes)
		// execute user-defined mapTask function
		resultKV = mapF(inFile, contents)
		encMap := make(map[string]*json.Encoder, nReduce)
		rNames := make([]string, nReduce)
		outFiles := make([]*os.File, nReduce)
		for i:=0; i<nReduce; i++ {
			rName := reduceName(jobName, mapTask, i)
			rNames[i] = rName
			file, err := os.Create(rName)
			if err != nil {
				fmt.Println("error happens when create result file of mapTask: ", err)
				return
			}
			enc := json.NewEncoder(file)
			encMap[rName] = enc
			outFiles[i] = file
		}
		for _,item := range resultKV {
			hash := ihash(item.Key)
			enc := encMap[rNames[hash % nReduce]]
			err := enc.Encode(&item)
			if err != nil {
				fmt.Printf("error happens when writing result file of mapTask:{%d}, error is {%v} \n", mapTask, err)
				return
			}
		}
		defer func() {
			for _,file := range outFiles {
				err := file.Close()
				if err != nil {
					fmt.Println("io error happens when close result file of mapTask", err)
				}
			}
		}()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
