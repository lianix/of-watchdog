package executor

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

// HTTPFunctionRunner creates and maintains one process responsible for handling all calls
type KafkaRunner struct {
	Topics []string
	Broker string
}

func KafkaRun() {
	f := buildkafkaRunner()
	f.start()
}

func buildkafkaRunner() kafkaRunner {
	broker := "kafka"
	if val, exists := os.LookupEnv("broker_host"); exists {
		broker = val
	}

	topics := []string{}
	if val, exists := os.LookupEnv("topics"); exists {
		for _, topic := range strings.Split(val, ",") {
			if len(topic) > 0 {
				topics = append(topics, topic)
			}
		}
	}
	if len(topics) == 0 {
		log.Fatal(`Provide a list of topics i.e. topics="payment_published,slack_joined"`)
	}

	gatewayURL := "http://gateway:8080"
	if val, exists := os.LookupEnv("gateway_url"); exists {
		gatewayURL = val
	}

	upstreamTimeout := time.Second * 30
	rebuildInterval := time.Second * 3

	if val, exists := os.LookupEnv("upstream_timeout"); exists {
		parsedVal, err := time.ParseDuration(val)
		if err == nil {
			upstreamTimeout = parsedVal
		}
	}

	if val, exists := os.LookupEnv("rebuild_interval"); exists {
		parsedVal, err := time.ParseDuration(val)
		if err == nil {
			rebuildInterval = parsedVal
		}
	}

	printResponse := false
	if val, exists := os.LookupEnv("print_response"); exists {
		printResponse = (val == "1" || val == "true")
	}

	printResponseBody := false
	if val, exists := os.LookupEnv("print_response_body"); exists {
		printResponseBody = (val == "1" || val == "true")
	}

	delimiter := ","
	if val, exists := os.LookupEnv("topic_delimiter"); exists {
		if len(val) > 0 {
			delimiter = val
		}
	}

	return kafkaRunner{
		Topics: topics,
		Broker: broker,
	}
}

// Start forks the process used for processing incoming requests
func (f *kafkaRunner) Start() error {

	fmt.Println("kafka start listen %s", f.Topics)

	waitForBrokers(f)

	makeConsumer(f)

}

// Start forks the process used for processing incoming requests
func waitForBrokers(f *KafkaRunner) {
	var client sarama.Client
	var err error

	fmt.Println("kafka start listen %s", f.Topics)

	brokers := []string{f.Broker + "9092"}

	for {
		client, err = sarama.NewClient(brokers, nil)
		if client != nil && err == nil {
			break
		}
		if client != nil {
			client.Close(0)
		}
		fmt.Println("wait for brokers (%s) to come up", f.Broker)

		time.sleep(1 * time.Second)
	}
}

func makeConsumer(f *KafkaRunner) {
	//setup consumer
	log.Printf("Binding to topics: %v", f.Topics)

	consumer, err := sarama.NewConsumer(f.brokers, group, nil)
	if err != nil {
		log.Fatalln("Fail to create Kafka consumer: ", err)
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(f.topics, 0, OffsetNewest)
	if err != nil {
		log.Fatalln("Fail to create partitions")
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("[#%d] Received on [%v,%v]: '%s'\n",
				msg.Offset,
				msg.Topic,
				msg.Partition,
				string(msg.Value))
			consumed++
			//	controller.Invoke(msg.Topic, &msg.Value)

		case err = <-partitionConsumer.Errors():
			fmt.Println("consumer error: ", err)

		case <-signals:
			fmt.Printf("exit: %+v\n", ntf)
		}
	}
}
