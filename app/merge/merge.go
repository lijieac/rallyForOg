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
	"os"
	"strconv"

	"github.com/lijieac/rallyForOg/lib/logs"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage:")
		fmt.Println("	merge count")
		fmt.Println("	eg.. merge 500000")
		return
	}

	count, _ := strconv.Atoi(os.Args[1])
	fmt.Println("Begin to merge file, count: ", count)
	logs.Merges(uint32(count), "documents-"+os.Args[1]+".json")
}
