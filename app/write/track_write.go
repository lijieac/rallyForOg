/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client"
	"github.com/lijieac/rallyForOg/lib/logs"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println(" -t thread [-h host] [-i index]")
	fmt.Println("    thread: write thread number, can not greater than 32")
	fmt.Println("    host: default is \"127.0.0.1:8086\"")
	fmt.Println("    index: \"index\" or \"noindex\", default is \"index\"")
}

type JsonInfo struct {
	name    string
	count   int
	realCnt int
	log     []logs.Log
}

func main() {
	var index string
	var host string
	var thread string
	flag.StringVar(&host, "h", "127.0.0.1:8086", "Specify target host address.")
	flag.StringVar(&index, "i", "index", "Specify whether an index needs to be established.")
	flag.StringVar(&thread, "t", "0", "Specify wtire thread number.")
	flag.Parse()

	threadCnt, _ := strconv.Atoi(thread)
	if threadCnt <= 0 || threadCnt > 32 {
		usage()
		return
	}

	httpTarget := "http://" + host
	// all documents: 247249096
	jsonFile := []JsonInfo{
		{"documents-181998.json", 2800000, 2708746, nil},     // 270 8746
		{"documents-191998.json", 10000000, 9697882, nil},    // 969 7882
		{"documents-201998.json", 13100000, 13053463, nil},   // 1305 3463
		{"documents-211998.json", 17700000, 17647279, nil},   // 1764 7279
		{"documents-221998.json", 10800000, 10716760, nil},   // 1071 6760
		{"documents-231998.json", 12000000, 11961342, nil},   // 1196 1342
		{"documents-241998.json", 181500000, 181463624, nil}, // 1 8146 3624
	}

	// new openGemini client, every thread has client connection
	fmt.Println("---------------------------------------------------------------------------------")
	fmt.Println("---------------------------------------------------------------------------------")
	cons := make([]*client.Client, threadCnt)
	for i := 0; i < threadCnt; i++ {
		cons[i] = logs.NewOpenGeminiClient(httpTarget)
	}
	// create the schema of measurement.
	err := logs.CreateMeasurementForLogs(cons[0], index == "noindex")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("--Open openGmeini client and create database success.")

	// get the data from file.
	cnt := 0
	oStart := time.Now().UnixMicro()
	fmt.Println("---------------------------------------------------------------------------------")
	fmt.Println("---------------------------------------------------------------------------------")

	var wg sync.WaitGroup
	for i := 0; i < len(jsonFile); i++ {
		wg.Add(1)
		go func(idx int) {
			filePath := "../../resource/http_logs/" + jsonFile[idx].name
			log, err := logs.ReadDataFromFile(filePath, jsonFile[idx].count)
			if err != nil {
				fmt.Println("--read data from [", jsonFile[idx].name, "]failed")
				wg.Done()
				return
			}
			jsonFile[idx].log = log
			fmt.Println("--read data from [", jsonFile[idx].name, "]successfully, RealCount-ReadCount:", jsonFile[idx].realCnt, len(jsonFile[idx].log))
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i := 0; i < len(jsonFile); i++ {
		cnt = cnt + len(jsonFile[i].log)
	}
	oEnd := time.Now().UnixMicro()
	fmt.Println("---------------------------------------------------------------------------------")
	fmt.Println("--Finish read data, Cost time:", float64(oEnd-oStart)/1000, " Docment count:", cnt)

	// write data to openGemini
	fmt.Println("---------------------------------------------------------------------------------")
	fmt.Println("---------------------------------------------------------------------------------")
	var sum int64 = 0
	oStart = time.Now().UnixMicro()
	for i := 0; i < len(jsonFile); i++ {
		start := time.Now().UnixMicro()
		cnt := logs.WriteLogsToOpenGemini(cons, jsonFile[i].log, threadCnt)
		end := time.Now().UnixMicro()
		fmt.Println("--Finish write file: [", jsonFile[i].name, "], Cost time: ", float64(end-start)/1000, "Write count:", cnt)
		sum = sum + (end - start)
	}
	oEnd = time.Now().UnixMicro()
	fmt.Println("---------------------------------------------------------------------------------")
	fmt.Println("--All cost time:", float64(oEnd-oStart)/1000, "Write cost time: ", sum/1000)
	fmt.Println("---------------------------------------------------------------------------------")
	fmt.Println("---------------------------------------------------------------------------------")
}
