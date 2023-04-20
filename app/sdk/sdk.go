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

	"github.com/lijieac/rallyForOg/lib/logs"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println("	sdk type")
	fmt.Println("type = [50w, 5000w, 18000w]")
}

func main() {
	if len(os.Args) != 2 {
		usage()
		return
	}

	switch os.Args[1] {
	case "50w":
		logs.WriteLogsToOpenGemini("documents-500000.json", "http://127.0.0.1:8086", 500000)
	case "5000w":
		logs.WriteLogsToOpenGemini("documents-50000000.json", "http://127.0.0.1:8086", 50000000)
	case "18000w":
		logs.WriteLogsToOpenGemini("documents-180000000.json", "http://127.0.0.1:8086", 180000000)
	default:
		usage()
	}
}
