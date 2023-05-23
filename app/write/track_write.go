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
	"strconv"
	"time"

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
		{"documents-181998.json", 2800000, 2708746},     // 270 8746
		{"documents-191998.json", 10000000, 9697882},    // 969 7882
		{"documents-201998.json", 13100000, 13053463},   // 1305 3463
		{"documents-211998.json", 17700000, 17647279},   // 1764 7279
		{"documents-221998.json", 10800000, 10716760},   // 1071 6760
		{"documents-231998.json", 12000000, 11961342},   // 1196 1342
		{"documents-241998.json", 181500000, 181463624}, // 1 8146 3624
	}

	// new openGemini client and create the schema of measurement.
	var sum int64 = 0
	cons := logs.NewGeminiClientAndMeasurement(httpTarget, index == "noindex")

	oStart := time.Now().UnixMicro()
	for i := 0; i < len(jsonFile); i++ {
		// get the data from file.
		fmt.Println("Start to read file [", jsonFile[i].name, "] and write to openGemini...")
		filePath := "../../resource/http_logs/" + jsonFile[i].name
		log := logs.ReadDataFromFile(filePath, jsonFile[i].count)
		fmt.Println("read data from [", jsonFile[i].name, "]successfully, count:", len(log))

		start := time.Now().UnixMicro()
		logs.WriteLogsToOpenGemini(cons, log, threadCnt)
		end := time.Now().UnixMicro()
		fmt.Println("File:", jsonFile[i].name, " Write count:", jsonFile[i].realCnt, " Cost time: ", float64(end-start)/1000)
		sum = sum + (end - start)
	}
	oEnd := time.Now().UnixMicro()
	fmt.Println("All cost time:", float64(oEnd-oStart)/1000, "Write cost time: ", sum)

}
