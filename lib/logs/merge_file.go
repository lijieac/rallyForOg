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

package logs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

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
		timestamp = timestamp + 100000000 // 10 Docs per 1 second
	}

	write.Flush()
	return timestamp
}

func readJsonAndMerge(inFileName string, outFileName string, timestamp int64, count uint32, maxMergeCount uint32) (int64, uint32) {
	fp, err := os.Open(inFileName)
	if err != nil {
		fmt.Println("open input path failed, err:", err)
		return timestamp, count
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

		count++
		if count >= maxMergeCount {
			break
		}

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
	return writeJsonToFile(outFileName, timestamp, logs), count
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

func Merges(maxMergeCount uint32, outFile string) {
	jsonFile := []string{
		"documents-181998.json",
		"documents-191998.json",
		"documents-201998.json",
		"documents-211998.json",
		"documents-221998.json",
		"documents-231998.json",
		"documents-241998.json",
	}

	var timestamp int64 = 800000000
	var count uint32 = 0
	for i := 0; i < len(jsonFile); i++ {
		fmt.Println("begin to merge file:", jsonFile[i])
		timestamp, count = readJsonAndMerge(jsonFile[i], outFile, timestamp, count, maxMergeCount)
		if count >= maxMergeCount {
			break
		}
	}
}

func readJsonAndTransfer(inFileName string, outPath string, timestamp int64) (int64, int) {
	fp, err := os.Open(inFileName)
	if err != nil {
		fmt.Println("open input path failed, err:", err)
		return timestamp, 0
	}
	defer fp.Close()

	outFileName := outPath + "/" + inFileName
	count := 0
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

		count++
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
	return writeJsonToFile(outFileName, timestamp, logs), count
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

func Transfer(outFath string) {
	jsonFile := []string{
		"documents-181998.json",
		"documents-191998.json",
		"documents-201998.json",
		"documents-211998.json",
		"documents-221998.json",
		"documents-231998.json",
		"documents-241998.json",
	}

	var timestamp int64 = 800000000
	var cnt int = 0
	var allCnt int = 0
	for i := 0; i < len(jsonFile); i++ {
		fmt.Println("begin to merge file:", jsonFile[i])
		timestamp, cnt = readJsonAndTransfer(jsonFile[i], outFath, timestamp)
		allCnt = allCnt + cnt
		fmt.Println("sum of docs:", allCnt, "cur file docs:", cnt)
	}
}
