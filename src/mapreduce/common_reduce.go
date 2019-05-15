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
		file,err := os.Open(reduceName)
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
}
