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
	var dataCnt int
	var index string
	flag.StringVar(&index, "i", "index", "Specify whether an index needs to be established")
	flag.IntVar(&dataCnt, "c", 0, "Specify the amount of data")
	flag.Parse()

	if dataCnt <= 0 || (index != "index" && index != "noindex") {
		usage()
		return
	}

	fileName := "documents-" + os.Args[1] + ".json"
	filePath := "../../resource/http_logs/" + fileName
	_, err := os.Stat(filePath)
	if err != nil {
		fmt.Println("the file [", fileName, "] is not existed. err:", err)
		return
	}
	count, _ := strconv.Atoi(os.Args[1])
	logs.WriteLogsToOpenGemini(filePath, "http://127.0.0.1:8086", count, index == "noindex")
}
