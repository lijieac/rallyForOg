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

func writeJsonToFile(outPath string, timestamp int64, logs []Log) int64 {
	fp, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
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

func readJsonAndCombine(inPath string, outPath string, timestamp int64) int64 {
	fp, err := os.Open(inPath)
	if err != nil {
		fmt.Println("open input path failed, err:", err)
		return 0
	}
	defer fp.Close()

	fmt.Println("begin to deal file:", inPath)
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
			fmt.Println("begin to deal file, count:", j)
			timestamp = writeJsonToFile(outPath, timestamp, logs)
			logs = make([]Log, 0, MaxBuf)
			i = 0
		}
	}
	return writeJsonToFile(outPath, timestamp, logs)
}

func main() {
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
	for i := 0; i < len(jsonFile); i++ {
		timestamp = readJsonAndCombine(jsonFile[i], "documents-001002.json", timestamp)
	}
}
