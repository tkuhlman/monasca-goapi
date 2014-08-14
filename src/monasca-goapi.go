/* Simple program used to benchmark go for the Monasca api
	It just receives measurements and pushes them into kafka, nothing more.
 */

package main

import (
	"encoding/binary"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"log"
	"net/http"
	"runtime"
)

type metric struct {
	Name string
	Dimensions map[string]string
	Timestamp int  //In a real implementation probably shouldn't be an int
	Value int
}

// Implment the Encode interface so metric can be used for sending to kafka.
func (m metric) Encode() ([]byte, error) {
	buffer := new(bytes.Buffer)
	err := binary.Write(buffer, binary.LittleEndian, m)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // Use all the machine's cores

	kafkaHosts := []string{"192.168.10.4:9092"}
//	kafkaHosts := []string{"10.22.156.14:9092", "10.22.156.15:9092", "10.22.156.16:9092"}
	measurements := make(chan metric, 1024)  // Room for 1024 as buffer to spikes

	go kafkaProducer(kafkaHosts, measurements)

	// todo I need to figure out how to pass in measurements chan to the handleFunc
	http.HandleFunc("/v2.0/metrics", metrics)
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal("failed to start server", err)
	}
}

// Produce to kafka anything read from the measurements channel
func kafkaProducer(url []string, measurements <-chan metric) {

	//Setup the producer
	client, err := sarama.NewClient("goapi", url, sarama.NewClientConfig())
	if err != nil {
		panic(err)
	}
	defer client.Close()
	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	//Push from the measurements channel to kafka
	for measurement := range(measurements) {
		// todo I should check for errors coming back from Kafka
		err = producer.QueueMessage("message", nil, measurement)
		if err != nil {
			fmt.Printf("Unable to publish to Kafka\n\t%v", err)
		}
	}
}

func metrics(writer http.ResponseWriter, request *http.Request) {
	bodyBuffer := new(bytes.Buffer)
	_, err := bodyBuffer.ReadFrom(request.Body)
	if err != nil && err != io.EOF {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "Error reading payload", err)
		return
	}
	body := make([]byte, bodyBuffer.Len())
	bodyBuffer.Read(body)

	var metrics []metric
	err = json.Unmarshal(body, &metrics)
	if err != nil{
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "Invalid json", err)
		return
	}

	for _, measurement := range(metrics) {
		fmt.Printf("%+v", measurement)
//		measurements <- measurement
	}

	writer.WriteHeader(http.StatusNoContent)  //StatusNoContent == 204
}
