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
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client"
)

const (
	printCount  int = 10000000
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

func ReadDataFromFile(fileName string, maxCount int) ([]Log, error) {
	fp, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Open file err:", fileName, err)
		return nil, err
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
	return logs, nil
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

func CreateMeasurementForLogs(con *client.Client, noIndex bool) error {
	q := client.Query{
		Command: "drop database logdb; create database logdb with SHARD DURATION 40d",
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

	if noIndex {
		q = client.Query{
			Command:  "create measurement logTable",
			Database: "logdb",
		}
	} else {
		q = client.Query{
			Command:  "create measurement logTable(clientip string tag, request string field, index idx1 request type text)",
			Database: "logdb",
		}
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

func NewGeminiClientAndMeasurement(rawURL string, noIndex bool) *client.Client {
	con := NewOpenGeminiClient(rawURL)
	err := CreateMeasurementForLogs(con, noIndex)
	if err != nil {
		log.Fatal(err)
	}
	return con
}

type WriteLogs struct {
	log      []Log
	curIndex int
	writeCnt int
	lock     sync.RWMutex
}

func NewWriteLogs(log []Log) *WriteLogs {
	return &WriteLogs{
		log:      log,
		curIndex: 0,
		writeCnt: 0,
	}
}

func (wlogs *WriteLogs) GetCurIndexAndAdd() int {
	wlogs.lock.Lock()
	index := wlogs.curIndex
	wlogs.curIndex = wlogs.curIndex + batchPoints
	wlogs.lock.Unlock()
	return index
}

func (wlogs *WriteLogs) AddWriteCnt(cnt int) {
	wlogs.lock.Lock()
	wlogs.writeCnt = wlogs.writeCnt + cnt
	wlogs.lock.Unlock()
}

func writeToOpenGemini(con *client.Client, writeLogs *WriteLogs, threadId int) {
	start := time.Now().UnixMicro()

	cnt := 0
	logs := writeLogs.log
	i := writeLogs.GetCurIndexAndAdd()
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
				fmt.Println("ID:", threadId, "- current time:", cur, "count:", i)
			}
			i++
			cnt++
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
			fmt.Println("current time:", time.Now(), "panic error, (i, retry):", i, retry, " error: ", err)
		}
		i = writeLogs.GetCurIndexAndAdd()
	}

	end := time.Now().UnixMicro()
	fmt.Println("ID:", threadId, " Write count:", cnt, " Cost time: ", float64(end-start)/1000)
	writeLogs.AddWriteCnt(cnt)
}

func WriteLogsToOpenGemini(cons []*client.Client, logs []Log, threadCnt int) int {
	if cons == nil || logs == nil || threadCnt <= 0 {
		fmt.Println("wrong parameter.")
		return 0
	}

	writeLogs := NewWriteLogs(logs)
	var wg sync.WaitGroup

	if len(cons) < threadCnt {
		fmt.Println("wrong parameter.")
		return 0
	}

	for i := 0; i < threadCnt; i++ {
		wg.Add(1)
		go func(id int) {
			writeToOpenGemini(cons[id], writeLogs, id)
			wg.Done()
		}(i)
	}
	wg.Wait()
	return writeLogs.writeCnt
}
