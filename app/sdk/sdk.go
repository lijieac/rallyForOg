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
	"os"
	"strconv"

	"github.com/lijieac/rallyForOg/lib/logs"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println("	sdk -c 500000 [-i index/noindex], default is \"index\"")
}

func main() {
	var dataCnt string
	var index string
	flag.StringVar(&index, "i", "index", "Specify whether an index needs to be established")
	flag.StringVar(&dataCnt, "c", "0", "Specify the amount of data")
	flag.Parse()

	count, _ := strconv.Atoi(dataCnt)
	if count <= 0 || (index != "index" && index != "noindex") {
		usage()
		return
	}

	fileName := "documents-" + dataCnt + ".json"
	filePath := "../../resource/http_logs/" + fileName
	_, err := os.Stat(filePath)
	if err != nil {
		fmt.Println("the file [", fileName, "] is not existed. err:", err)
		return
	}

	// new openGemini client and create the schema of measurement.
	cons := logs.NewGeminiClientAndMeasurement("http://127.0.0.1:8086", index == "noindex")

	// get the data from file.
	fmt.Println("Begin to write logs to openGemini...")
	log, err := logs.ReadDataFromFile(filePath, count)
	fmt.Println("read data successfully, count:", len(log))
	if err != nil {
		return
	}

	// write to openGemini.
	logs.WriteLogsToOpenGemini(cons, log, 1)
}
