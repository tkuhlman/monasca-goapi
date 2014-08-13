/* Simple program used to benchmark go for the Monasca api
	It just receives measurements and pushes them into kafka, nothing more.
 */

package main

import (
	"fmt"
	"encoding/json"
	"log"
	"net/http"
	"runtime"
)

type metricMetaData struct {
	tenantId, region string
}

type metric struct {
	name string
	dimensions string
	creation_time, timestamp string  //In a real implementation probably shouldn't be a string
	meta metricMetaData
	value int
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // Use all the machine's cores
	http.HandleFunc("/v2.0/metrics", metrics)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("failed to start server", err)
	}
}

func metrics(writer http.ResponseWriter, request *http.Request) {
	body := make([]byte, 1024)
	_, err := request.Body.Read(body)
	if err != nil {
		fmt.Fprintf(writer, "Error reading payload", err)
		return
	}
	var metrics []metric
	err = json.Unmarshal(body, metrics)
	if err != nil{
		fmt.Fprintf(writer, "Invalid json", err)
		return
	}
	// todo I need to write to kafka
	fmt.Printf("%v", metrics)

	writer.WriteHeader(http.StatusNoContent)  //StatusNoContent == 204
// todo it is returning a 200 not 204 for some reason
//	fmt.Fprint(writer, "")
}
