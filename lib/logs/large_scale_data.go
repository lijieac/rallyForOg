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
	maxCount    int = 180000000
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
			fmt.Println(err)
		}

		var log Log
		err = json.Unmarshal(data, &log)
		if err != nil {
			fmt.Println(i)
			panic("Unmarshal failed.")
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
		Command:  "create measurement logTable(clientip sring tags, request string field, index idx1 request type text)",
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

func WriteLogsToOpenGemini() {
	logs := readDataFromFile("../resource/documents-180000000.json", maxCount)
	fmt.Println("read data successfully, count:", len(logs))
	con := NewOpenGeminiClient("http://127.0.0.1:8086")
	err := createMeasurementForLogs(con)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now().UnixMicro()
	pre := time.Now().UnixMicro()
	for i := 0; i < len(logs); {
		points := make([]client.Point, 0, batchPoints)
		k := i
		for ; k < i+100 && k < len(logs); k++ {
			point := client.Point{
				Measurement: "logTable",
				Tags: map[string]string{
					"clientip": logs[k].Clientip,
				},
				Fields: map[string]interface{}{
					"logs":   logs[k].Request,
					"status": logs[k].Status,
					"size":   logs[k].Size,
				},
				Time:      time.Unix(0, logs[k].Timestamp),
				Precision: "ns",
			}
			points = append(points, point)

			if k%printCount == 0 {
				cur := time.Now().UnixMicro()
				ti := int(cur - pre/1000000)
				if ti != 0 {
					fmt.Println("current time：", time.Now(), "write(/s): ", printCount/ti)
				} else {
					fmt.Println("current time：", time.Now(), "write(w): ", k)
				}
				pre = cur
			}
		}
		i = i + k
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
	fmt.Println("sdk push logs cost time:")
	fmt.Println(float64(end-start) / 1000)
}
