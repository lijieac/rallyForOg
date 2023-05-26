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
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println(" -h host")
	fmt.Println("    host: like \"127.0.0.1:8086\"")
}

func main() {
	var host string
	flag.StringVar(&host, "h", "nil", "Specify target host address.")
	flag.Parse()

	if host == "nil" {
		usage()
		return
	}

	// Open a new client
	addr := "http://" + host
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     addr,
		Username: "admin",
		Password: "At1314comi!",
	})
	if err != nil {
		fmt.Println("Error", err.Error())
	}
	defer c.Close()

	mtoken := make([]string, 18)
	mtoken[0] = "select * from logTable where match_phrase(request, 'GET /images/photo02.gif')"
	mtoken[1] = "select * from logTable where match_phrase(request, 'GET /english/playing/images/play_hm_mascot.gif')"
	mtoken[2] = "select * from logTable where match_phrase(request, 'GET /images/cal_steti.gif HTTP/1.0')"
	mtoken[3] = "select * from logTable where match_phrase(request, 'GET /images/hm_bg.jpg HTTP/1.0')"
	mtoken[4] = "select * from logTable where match_phrase(request, 'GET /images/s102325.gif HTTP/1.0')"
	mtoken[5] = "select * from logTable where match_phrase(request, 'GET /english/frntpage.htm HTTP/1.0')"

	mtoken[6] = "select * from logTable where match(request, '11104.gif')"
	mtoken[7] = "select * from logTable where match(request, '11104.gif')"
	mtoken[8] = "select * from logTable where match(request, 'mascot_on.gif box_saver1.gif')"
	mtoken[9] = "select * from logTable where match(request, 'team_hm_header_shad.gif')"
	mtoken[10] = "select * from logTable where match(request, 'past_cups past_bu_30_off.gif')"
	mtoken[11] = "select * from logTable where match(request, 'venues venue_bu_acomm_on.gif')"

	mtoken[12] = "select * from logTable where request like 'venues frntpage.%'"
	mtoken[13] = "select * from logTable where request like 'venues photo02.%'"
	mtoken[14] = "select * from logTable where request like 'venues trophytxt.gif'"
	mtoken[15] = "select * from logTable where request like 'venues backg.gif'"
	mtoken[16] = "select * from logTable where request like 'venues teamgroup.htm'"
	mtoken[17] = "select * from logTable where request like 'venues competition'"

	for i := 0; i < len(mtoken); i++ {
		s := time.Now().UnixMicro()
		q := client.NewQuery(mtoken[i], "logdb", "")
		// Query
		response, err := c.Query(q)
		if err == nil && response.Error() == nil {
			e := time.Now().UnixMicro()
			if len(response.Results) == 0 {
				fmt.Println("Q", i, "E2E time:", float64(e-s)/1000, "result: 0")
				continue
			}
			if len(response.Results[0].Series) == 0 {
				continue
			}
			fmt.Println("Q", i, "E2E time:", float64(e-s)/1000, "result:", len(response.Results[0].Series[0].Values))
		}
	}
}
