package executor

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

// HTTPFunctionRunner creates and maintains one process responsible for handling all calls
type KafkaRunnerCfg struct {
	UpstreamURL string
	Topics      []string
	Brokers     []string
}

type KafkaRunner struct {
	Cfg        *KafkaRunnerCfg
	NoProducer bool
	Consumer   sarama.Consumer
	Producer   sarama.SyncProducer
}

func KafkaRun() {
	var k *KafkaRunner = new(KafkaRunner)

	k.Cfg = buildkafkaRunnerCfg()

	err := makeKafkaClient(k)
	if err != nil {
		fmt.Println("Failed to create client")
	}

	listenKafka(k)
}

func buildkafkaRunnerCfg() *KafkaRunnerCfg {
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
				topics = append(topics, strings.TrimSpace(topic))
			}
		}
	}
	if len(topics) == 0 {
		fmt.Println("please provide topics")
	}
	fmt.Println("topics %v", topics)

	var upstreamURL string
	if val, exists := os.LookupEnv("upstream_url"); exists {
		upstreamURL = val
	}
	fmt.Println("upstreamURL:  %v", upstreamURL)
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

	return &KafkaRunnerCfg{
		UpstreamURL: upstreamURL,
		Topics:      topics,
		Brokers:     brokers,
	}
}

// Make Kafka Client/consumer/producer
func makeKafkaClient(k *KafkaRunner) error {
	var err error

	//brokers := []string{f.Broker + ":9092"}
	// Wait for Broker ready
	for {
		client, err := sarama.NewClient(k.Cfg.Brokers, nil)
		if client != nil && err == nil {
			break
		}
		if client != nil {
			client.Close()
		}
		fmt.Println("wait for brokers (%s) to come up", k.Cfg.Brokers[0])

		time.Sleep(1 * time.Second)
	}

	len := len(k.Cfg.Topics)
	if len == 0 {
		return errors.New("No Topic")
	} else if len == 1 {
		k.NoProducer = true
	} else {
		k.NoProducer = false
	}

	// setup consumer
	k.Consumer, err = sarama.NewConsumer(k.Cfg.Brokers, nil)
	if err != nil {
		fmt.Println("could not create consumer: ", err)
		return err
	}

	if k.NoProducer {
		return nil
	}
	// setup producer
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	k.Producer, err = sarama.NewSyncProducer(k.Cfg.Brokers, config)
	if err != nil {
		fmt.Println("could not create producer", err)
		return err
	}

	return nil
}

func listenKafka(k *KafkaRunner) {
	topic := k.Cfg.Topics[0]
	consumer := k.Consumer

	fmt.Println("Polling topic: %s", topic)
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}

	fmt.Println("partitionList %v", partitionList)

	initialOffset := sarama.OffsetNewest //OfffsetOldest
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageHandle(k, message)
			}
		}(pc)
	}
}

func messageHandle(k *KafkaRunner, message *sarama.ConsumerMessage) {
	fmt.Printf("[#%d] Received on [%v,%v]: '%s'\n",
		message.Offset,
		message.Topic,
		message.Partition,
		string(message.Value))

	resp, err := http.Post(k.Cfg.UpstreamURL, "text/plain", bytes.NewReader(message.Value))
	if err != nil {
		fmt.Println("http post err", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("io read error")
	}

	fmt.Println(string(body))

	messageSend(k, body)
}

func messageSend(k *KafkaRunner, value []byte) {
	if k.NoProducer {
		return
	}

	topic := k.Cfg.Topics[1]
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(value),
	}

	partition, offset, err := k.Producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Could not send message to Topic %s", topic)
		fmt.Println("Error: %s", err.Error())
	} else {
		fmt.Println("message was sent to partion %d offset is %d",
			partition, offset)
	}
}
