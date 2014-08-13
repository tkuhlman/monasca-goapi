/* Simple program used to benchmark go for the Monasca api
	It just receives measurements and pushes them into kafka, nothing more.
 */

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
)

/*
type metric struct {
	name string
	dimensions map[string]string
	timestamp int  //In a real implementation probably shouldn't be an int
	value int
}
*/

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // Use all the machine's cores
	http.HandleFunc("/v2.0/metrics", metrics)
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal("failed to start server", err)
	}
}

func metrics(writer http.ResponseWriter, request *http.Request) {
	var buffer []byte
	bodyBuffer := bytes.NewBuffer(buffer)
	_, err := bodyBuffer.ReadFrom(request.Body)
	if err != nil && err != io.EOF {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "Error reading payload", err)
		return
	}
	body := make([]byte, bodyBuffer.Len())
	bodyBuffer.Read(body)
// todo For some reason json is not properly unmarshalling to my array of metric, using a generic interface instead
//	var metrics []metric

	var metrics []interface{}
	err = json.Unmarshal(body, &metrics)
	if err != nil{
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "Invalid json", err)
		return
	}
	// todo I need to write to kafka
	//fmt.Printf("JSON - %s\n\nMetrics - %+v\n---\n", body[:bodySize], metrics)

	writer.WriteHeader(http.StatusNoContent)  //StatusNoContent == 204
}
