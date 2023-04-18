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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Log struct {
	Timestamp int64  `json:"@timestamp"`
	Clientip  string `json:"clientip"`
	Request   string `json:"request"`
	Status    int    `json:"status"`
	Size      int    `json:"size"`
}

const (
	MaxBuf int = 10000
)

func writeJsonToFile(outFileName string, timestamp int64, logs []Log) int64 {
	fp, err := os.OpenFile(outFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("open output path failed, err:", err)
		return timestamp
	}
	defer fp.Close()

	write := bufio.NewWriter(fp)
	for i := 0; i < len(logs); i++ {
		log := fmt.Sprintf("{\"@timestamp\": %d, \"clientip\":\"%s\", \"request\": \"%s\", \"status\": %d, \"size\": %d}",
			timestamp, logs[i].Clientip, logs[i].Request, logs[i].Status, logs[i].Size)
		write.WriteString(log)
		write.WriteString("\n")
		timestamp = timestamp + 10
	}

	write.Flush()
	return timestamp
}

func readJsonAndMerge(inFileName string, outFileName string, timestamp int64) int64 {
	fp, err := os.Open(inFileName)
	if err != nil {
		fmt.Println("open input path failed, err:", err)
		return 0
	}
	defer fp.Close()

	i := 0
	j := 0
	buff := bufio.NewReader(fp)
	logs := make([]Log, 0, MaxBuf)
	for {
		// read data form json file
		data, _, err := buff.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("read file EOF!")
				break
			}
			fmt.Println(err)
		}
		var log Log
		err = json.Unmarshal(data, &log)
		if err != nil {
			fmt.Println(i)
			panic("Unmarshal failed.")
		}
		logs = append(logs, log)

		// write data to output file.
		i++
		if i >= MaxBuf {
			j++
			fmt.Println("	begin to deal file, count:", j)
			timestamp = writeJsonToFile(outFileName, timestamp, logs)
			logs = make([]Log, 0, MaxBuf)
			i = 0
		}
	}
	return writeJsonToFile(outFileName, timestamp, logs)
}

/*
download the http_logs files and extract them:

	https://rally-tracks.elastic.co/http_logs/documents-181998.json.bz2
	https://rally-tracks.elastic.co/http_logs/documents-191998.json.bz2
	https://rally-tracks.elastic.co/http_logs/documents-201998.json.bz2
	https://rally-tracks.elastic.co/http_logs/documents-211998.json.bz2
	https://rally-tracks.elastic.co/http_logs/documents-221998.json.bz2
	https://rally-tracks.elastic.co/http_logs/documents-231998.json.bz2
	https://rally-tracks.elastic.co/http_logs/documents-241998.json.bz2
*/

func merge() {
	jsonFile := []string{
		"documents-181998.json",
		"documents-191998.json",
		"documents-201998.json",
		"documents-211998.json",
		"documents-221998.json",
		"documents-231998.json",
		"documents-241998.json",
	}
	outFile := "documents-001002.json"

	var timestamp int64 = 800000000
	for i := 0; i < len(jsonFile); i++ {
		fmt.Println("begin to merge file:", jsonFile[i])
		timestamp = readJsonAndMerge(jsonFile[i], outFile, timestamp)
	}
}

func main() {
	fmt.Println("Begin to merge file...")
	merge()
}
