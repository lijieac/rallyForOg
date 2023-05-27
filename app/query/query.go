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

var querySql []string = []string{
	"select * from logdb.autogen.mst181998 where match_phrase(request, 'GET /french/nav_top_inet.html HTTP')",
	"select * from logdb.autogen.mst191998 where match_phrase(request, 'GET /english/splash_inet.html HTTP/1.1')",
	"select * from logdb.autogen.mst201998 where match_phrase(request, 'GET /english/competition/stage2.htm HTTP/1.0')",
	"select * from logdb.autogen.mst211998 where match_phrase(request, 'english/history/past_cups/images/posters/germany74.gif')",
	"select * from logdb.autogen.mst221998 where match_phrase(request, 'history_of/images/france/history_france_platinibw.gif')",
	"select * from logdb.autogen.mst231998 where match_phrase(request, 'GET /english/frntpage.htm HTTP/1.0')",
	"select * from logdb.autogen.mst241998 where match_phrase(request, 'GET /english/frntpage.htm HTTP/1.0')",

	"select * from logdb.autogen.mst181998 where match(request, 'home_eng_button.gif')",
	"select * from logdb.autogen.mst191998 where match(request, 'team_group_header_e.gif')",
	"select * from logdb.autogen.mst201998 where match(request, '13cafe.jpg')",
	"select * from logdb.autogen.mst211998 where match(request, 'out_france.html')",
	"select * from logdb.autogen.mst221998 where match(request, 'history_france_platinibw.gif')",
	"select * from logdb.autogen.mst231998 where match(request, 'venues venue_bu_acomm_on.gif')",
	"select * from logdb.autogen.mst241998 where match(request, 'venues venue_bu_acomm_on.gif')",

	"select * from logdb.autogen.mst181998 where request like 'download.%'",
	"select * from logdb.autogen.mst191998 where request like 'a_g.gif'",
	"select * from logdb.autogen.mst201998 where request like 'mi%.gif'",
	"select * from logdb.autogen.mst211998 where request like 'out_france.html'",
	"select * from logdb.autogen.mst221998 where request like 'universality._if'",
	"select * from logdb.autogen.mst231998 where request like 'venues competition'",
	"select * from logdb.autogen.mst241998 where request like 'venues competition'",
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

	// query the
	for i := 0; i < len(querySql); i++ {
		s := time.Now().UnixMicro()
		q := client.NewQuery(querySql[i], "logdb", "")
		// Query
		response, err := c.Query(q)
		if err == nil && response.Error() == nil {
			e := time.Now().UnixMicro()
			if len(response.Results) == 0 {
				fmt.Println("Q", i, "E2E time:", float64(e-s)/1000, "result: 0")
				continue
			}
			if len(response.Results[0].Series) == 0 {
				fmt.Println("Q", i, "E2E time:", float64(e-s)/1000, "result: 0")
				continue
			}
			fmt.Println("Q", i, "E2E time:", float64(e-s)/1000, "result:", len(response.Results[0].Series[0].Values))
		}
	}
}
