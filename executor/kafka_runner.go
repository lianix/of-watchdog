package executor

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

// HTTPFunctionRunner creates and maintains one process responsible for handling all calls
type KafkaRunner struct {
	Topics  []string
	Brokers []string
}

func KafkaRun() {
	f := buildkafkaRunner()
	f.Start()
}

func buildkafkaRunner() KafkaRunner {
	broker := "kafka"
	if val, exists := os.LookupEnv("broker_host"); exists {
		broker = val
	}
	brokers := []string{broker + ":9092"}
	fmt.Println("brokers %v", brokers)
	topics := []string{}
	if val, exists := os.LookupEnv("topics"); exists {
		for _, topic := range strings.Split(val, ",") {
			if len(topic) > 0 {
				topics = append(topics, topic)
			}
		}
	}
	if len(topics) == 0 {
		fmt.Println(`Provide a list of topics i.e. topics="payment_published,slack_joined"`)
	}

	//	gatewayURL := "http://gateway:8080"
	//	if val, exists := os.LookupEnv("gateway_url"); exists {
	//		gatewayURL = val
	//	}
	//
	//	upstreamTimeout := time.Second * 30
	//	rebuildInterval := time.Second * 3
	//
	//	if val, exists := os.LookupEnv("upstream_timeout"); exists {
	//		parsedVal, err := time.ParseDuration(val)
	//		if err == nil {
	//			upstreamTimeout = parsedVal
	//		}
	//	}
	//
	//	if val, exists := os.LookupEnv("rebuild_interval"); exists {
	//		parsedVal, err := time.ParseDuration(val)
	//		if err == nil {
	//			rebuildInterval = parsedVal
	//		}
	//	}

	//	printResponse := false
	//	if val, exists := os.LookupEnv("print_response"); exists {
	//		printResponse = (val == "1" || val == "true")
	//	}
	//
	//	printResponseBody := false
	//	if val, exists := os.LookupEnv("print_response_body"); exists {
	//		printResponseBody = (val == "1" || val == "true")
	//	}
	//
	//	delimiter := ","
	//	if val, exists := os.LookupEnv("topic_delimiter"); exists {
	//		if len(val) > 0 {
	//			delimiter = val
	//		}
	//	}

	return KafkaRunner{
		Topics:  topics,
		Brokers: brokers,
	}
}

// Start forks the process used for processing incoming requests
func (f *KafkaRunner) Start() {

	fmt.Println("kafka start listen %v", f.Topics)

	waitForBrokers(f)

	makeConsumer(f)
}

// Start forks the process used for processing incoming requests
func waitForBrokers(f *KafkaRunner) {
	var client sarama.Client
	var err error

	fmt.Println("kafka start listen %s", f.Topics)

	//brokers := []string{f.Broker + "9092"}

	for {
		client, err = sarama.NewClient(f.Brokers, nil)
		if client != nil && err == nil {
			break
		}
		if client != nil {
			client.Close()
		}
		fmt.Println("wait for brokers (%s) to come up", f.Brokers[0])

		time.Sleep(1 * time.Second)
	}
}

func makeConsumer(f *KafkaRunner) {
	//setup consumer
	fmt.Println("Binding to topics: %v", f.Topics)

	consumer, err := sarama.NewConsumer(f.Brokers, nil)
	if err != nil {
		fmt.Println("Fail to create Kafka consumer: ", err)
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(f.Topics[0], 0, sarama.OffsetNewest)
	if err != nil {
		//log.Fatalln("Fail to create partitions")
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			//log.Fatalln(err)
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
			fmt.Printf("exit:\n")
		}
	}
}
