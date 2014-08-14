/* Simple program used to benchmark go for the Monasca api
It just receives measurements and pushes them into kafka, nothing more.
*/

package main

import (
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
	Name       string
	Dimensions map[string]string
	Timestamp  int //In a real implementation probably shouldn't be an int
	Value      int
}

// Implment the Encode interface so metric can be used for sending to kafka.
func (m metric) Encode() ([]byte, error) {
	// This is probably not the best way to do this but it is sufficient for a proof of concept
	jsonMetric, err := json.Marshal(m)
	if err == nil {
		return nil, err
	}
	encoder := sarama.StringEncoder(jsonMetric)
	return encoder.Encode()
}

type metricsHandler struct {
	measurements chan<- metric
}

func (mh metricsHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "Invalid json", err)
		return
	}

	//todo no validation is done on the json at this point
	for _, measurement := range metrics {
		mh.measurements <- measurement
	}

	writer.WriteHeader(http.StatusNoContent) //StatusNoContent == 204
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // Use all the machine's cores

	kafkaHosts := []string{"192.168.10.4:9092"}
	//	kafkaHosts := []string{"10.22.156.14:9092", "10.22.156.15:9092", "10.22.156.16:9092"}
	measurements := make(chan metric, 1024) // Room for 1024 as buffer to spikes

	go kafkaProducer(kafkaHosts, measurements)

	handler := metricsHandler{measurements} // Should only respond on /v2.0/metrics but as it is the only functionality I let it respond everwhere
	if err := http.ListenAndServe(":8000", handler); err != nil {
		log.Fatal("failed to start server", err)
	}
}

// Produce to kafka anything read from the measurements channel
func kafkaProducer(url []string, measurements <-chan metric) {

	//Setup the producer
	client, err := sarama.NewClient("goapi", url, sarama.NewClientConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	//Push from the measurements channel to kafka
	for measurement := range measurements {
		// todo I should check for errors coming back from Kafka
		err = producer.QueueMessage("message", nil, measurement)
		if err != nil {
			fmt.Printf("Unable to publish to Kafka\n\t%v\n", err)
		}
	}
}
