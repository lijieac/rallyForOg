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
	"fmt"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

func main() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: "admin",
		Password: "At1314comi!",
	})
	if err != nil {
		fmt.Println("Error", err.Error())
	}
	defer c.Close()

	mtoken := make([]string, 30)
	mtoken[0] = "select * from logTable where match_phrase(request, 'GET /images/photo02.gif')"
	mtoken[1] = "select * from logTable where match_phrase(request, 'GET /english/playing/images/play_hm_mascot.gif')"
	mtoken[2] = "select * from logTable where match_phrase(request, 'GET /images/11104.gif HTTP/1.1')"
	mtoken[3] = "select * from logTable where match_phrase(request, 'GET /images/cal_steti.gif HTTP/1.0')"
	mtoken[4] = "select * from logTable where match_phrase(request, 'GET /images/base.gif HTTP/1.0')"
	mtoken[5] = "select * from logTable where match_phrase(request, 'GET /english/playing/images/anim/mascot_on.gif')"
	mtoken[6] = "select * from logTable where match_phrase(request, 'GET /english/playing/download/images/box_saver1.gif')"
	mtoken[7] = "select * from logTable where match_phrase(request, 'GET /french/images/hm_official.gif')"
	mtoken[8] = "select * from logTable where match_phrase(request, 'past_cups/images/past_bu_30_off.gif')"
	mtoken[9] = "select * from logTable where match_phrase(request, 'GET /french/venues/images/venue_bu_acomm_on.gif HTTP/1.0')"

	mtoken[10] = "select * from logTable where match_phrase(request, 'GET /french/news/3004bres.htm')"
	mtoken[11] = "select * from logTable where match_phrase(request, 'GET / HTTP/1.0')"
	mtoken[12] = "select * from logTable where match_phrase(request, 'GET /english/history/past_cups/images/past_bracket_bot.gif')"
	mtoken[13] = "select * from logTable where match_phrase(request, 'GET /french/tickets/images/ticket_bu_infrance2.gif')"
	mtoken[14] = "select * from logTable where match_phrase(request, 'GET /french/tickets/images/ticket_bu_abroad2.gif')"
	mtoken[15] = "select * from logTable where match_phrase(request, 'GET /images/hm_bg.jpg HTTP/1.0')"
	mtoken[16] = "select * from logTable where match_phrase(request, 'GET /images/s102325.gif HTTP/1.0')"
	mtoken[17] = "select * from logTable where match_phrase(request, 'GET /english/frntpage.htm HTTP/1.0')"
	mtoken[18] = "select * from logTable where match_phrase(request, 'GET /english/history/history_of/images/cup')"
	mtoken[19] = "select * from logTable where match_phrase(request, 'GET /english/images/team_hm_header_shad.gif HTTP/1.0')"

	mtoken[20] = "select * from logTable where match_phrase(request, 'GET /french/venues/body.html HTTP/1.0')"
	mtoken[21] = "select * from logTable where match_phrase(request, 'GET /english/images/space.gif HTTP/1.1')"
	mtoken[22] = "select * from logTable where match_phrase(request, 'GET /images/hm_linkf.gif HTTP/1.1')"
	mtoken[23] = "select * from logTable where match_phrase(request, 'GET /images/11101.gif HTTP/1.0')"
	mtoken[24] = "select * from logTable where match_phrase(request, '/english/playing/images/banner2.gif HTTP/1.0')"
	mtoken[25] = "select * from logTable where match_phrase(request, 'GET /english/images/fpnewstop.gif HTTP/1.0')"
	mtoken[26] = "select * from logTable where match_phrase(request, 'GET /images/bord_stories01.gif HTTP/1.0')"
	mtoken[27] = "select * from logTable where match_phrase(request, 'GET /images/dburton.jpg HTTP/1.0')"
	mtoken[28] = "select * from logTable where match_phrase(request, 'GET /images/base.gif HTTP/1.0')"
	mtoken[29] = "select * from logTable where match_phrase(request, '/english/venues/cities/images/denis/venue_denn_bg.jpg')"

	for i := 0; i < 30; i++ {
		s := time.Now().UnixMicro()
		q := client.NewQuery(mtoken[i], "logdb", "")
		if response, err := c.Query(q); err == nil && response.Error() == nil {
			e := time.Now().UnixMicro()
			if len(response.Results) == 0 {
				continue
			}
			if len(response.Results[0].Series) == 0 {
				continue
			}
			fmt.Println(len(response.Results[0].Series[0].Values))
			//fmt.Println(response.Results)
			fmt.Println("end to end time")
			fmt.Println(float64(e-s) / 1000)
		}
	}
}
