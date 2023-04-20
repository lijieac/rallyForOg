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
	"log"
	"net/url"
	"os"
	"time"

	client "github.com/influxdata/influxdb1-client"
)

const (
	printCount  int = 1000000
	batchPoints int = 100
	maxRetry    int = 5
)

type Log struct {
	Timestamp int64  `json:"@timestamp"`
	Clientip  string `json:"clientip"`
	Request   string `json:"request"`
	Status    int    `json:"status"`
	Size      int    `json:"size"`
}

func readDataFromFile(fileName string, maxCount int) []Log {
	fp, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	var i int = 0
	buff := bufio.NewReader(fp)
	logs := make([]Log, 0, maxCount)
	for {
		// read data form json file
		data, _, err := buff.ReadLine()
		if err != nil {
			if err == io.EOF {
				fmt.Println("read file EOF!")
				break
			}
			log.Fatal(err)
		}

		var log Log
		err = json.Unmarshal(data, &log)
		if err != nil {
			fmt.Println("Unmarshal failed.", i, log)
			continue
		}
		logs = append(logs, log)

		i++
		if i%printCount == 0 {
			fmt.Println("	read count:", i, "max:", maxCount)
		}
		if i == maxCount {
			break
		}
	}
	return logs
}

// openGemini use influxdb sdk
func NewOpenGeminiClient(rawURL string) *client.Client {
	host, err := url.Parse(rawURL)
	if err != nil {
		log.Fatal(err)
	}
	con, err := client.NewClient(client.Config{URL: *host, Username: "admin", Password: "At1314comi!"})
	if err != nil {
		log.Fatal(err)
	}

	return con
}

func createMeasurementForLogs(con *client.Client) error {
	q := client.Query{
		Command: "drop database logdb; create database logdb",
	}

	r, err := con.Query(q)
	if err != nil {
		fmt.Println("create database error:", err)
		return err
	}
	if r.Err != nil {
		fmt.Println("create database error:", r.Err)
		return r.Err
	}

	q = client.Query{
		Command:  "create measurement logTable(clientip string tag, request string field, index idx1 request type text)",
		Database: "logdb",
	}

	r, err = con.Query(q)
	if err != nil {
		fmt.Println("create measurment(logdb.logTable) error:", err)
		return err
	}

	if r.Err != nil {
		fmt.Println("create measurment(logdb.logTable) error:", r.Err)
		return r.Err
	} else {
		fmt.Println("create measurment(logdb.logTable) successfully.")
	}

	return nil
}

func WriteLogsToOpenGemini(file, rawURL string, count int) {
	fmt.Println("Begin to write logs to openGemini...")
	logs := readDataFromFile("../../resource/http_logs/"+file, count)
	fmt.Println("read data successfully, count:", len(logs))
	con := NewOpenGeminiClient(rawURL)
	err := createMeasurementForLogs(con)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now().UnixMicro()
	pre := time.Now().UnixMicro()
	i := 0
	for i < len(logs) {
		points := make([]client.Point, 0, batchPoints)
		curMax := i + batchPoints
		if curMax >= len(logs) {
			curMax = len(logs)
		}
		for i < curMax {
			point := client.Point{
				Measurement: "logTable",
				Tags: map[string]string{
					"clientip": logs[i].Clientip,
				},
				Fields: map[string]interface{}{
					"request": logs[i].Request,
					"status":  logs[i].Status,
					"size":    logs[i].Size,
				},
				Time:      time.Unix(0, logs[i].Timestamp),
				Precision: "ns",
			}
			points = append(points, point)

			if (i != 0) && (i%printCount) == 0 {
				cur := time.Now().UnixMicro()
				ti := int((cur - pre) / 1000000)
				if ti != 0 {
					fmt.Println("current time：", time.Now(), "write(poingts/s): ", printCount/ti)
				} else {
					fmt.Println("current time：", time.Now(), "write(poingts): ", i)
				}
				pre = cur
			}
			i++
		}

		bps := client.BatchPoints{
			Points:   points,
			Database: "logdb",
		}
		for retry := 0; retry < maxRetry; retry++ {
			_, err := con.Write(bps)
			if err == nil {
				break
			}
			fmt.Println("current time：", time.Now(), "panic error, (k,i, retry):", k, i, retry, " error: ", err)
		}
	}

	end := time.Now().UnixMicro()
	fmt.Println("sdk push", i, "logs cost time: ", float64(end-start)/1000)
}
