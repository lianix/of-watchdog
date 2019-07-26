package executor

import (
	"bytes"
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
	Topics      [][2]string
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
	if k.Cfg == nil {
		fmt.Println("Failed to create CFG")
		return
	}
	err := makeKafkaClient(k)
	if err != nil {
		fmt.Println("Failed to create client")
		return
	}

	listenKafka(k)
}

func buildkafkaRunnerCfg() *KafkaRunnerCfg {
	broker := "kafka"
	if val, exists := os.LookupEnv("broker_host"); exists {
		broker = val
	}
	brokers := []string{broker + ":9092"}
	fmt.Println("brokers", brokers)

	topics := [][2]string{}
	if val, exists := os.LookupEnv("topics"); exists {
		for _, topicsGroup := range strings.Split(val, ";") {
			topicsGroup = strings.TrimSpace(topicsGroup)
			fmt.Println(topicsGroup)
			var tmp [2]string
			tmp2 := strings.Split(topicsGroup, ",")
			switch len(tmp) {
			case 2:
				tmp[0] = strings.TrimSpace(tmp2[0])
				tmp[1] = strings.TrimSpace(tmp2[1])
				topics = append(topics, tmp)
			case 1:
				tmp[0] = strings.TrimSpace(tmp2[0])
				tmp[1] = ""
				topics = append(topics, tmp)
			default:
				fmt.Println("please provide rx,[tx] topics")
			}
		}
	}
	if len(topics) == 0 {
		fmt.Println("please provide topics")
		return nil
	}
	fmt.Println("topics", topics)

	var upstreamURL string
	if val, exists := os.LookupEnv("upstream_url"); exists {
		upstreamURL = val
	}
	fmt.Println("upstreamURL:", upstreamURL)
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
		fmt.Println("wait for brokers:", k.Cfg.Brokers[0])

		time.Sleep(1 * time.Second)
	}

	//	len := len(k.Cfg.Topics)
	//	if len == 0 {
	//		return errors.New("No Topic")
	//	} else if len == 1 {
	//		k.NoProducer = true
	//	} else {
	//		k.NoProducer = false
	//	}

	// setup consumer
	k.Consumer, err = sarama.NewConsumer(k.Cfg.Brokers, nil)
	if err != nil {
		fmt.Println("could not create consumer: ", err)
		return err
	}

	//	if k.NoProducer {
	//		return nil
	//	}

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

	for _, topicsGroup := range k.Cfg.Topics {
		rx := topicsGroup[0]
		tx := topicsGroup[1]
		consumer := k.Consumer

		fmt.Println("listen to topic:", rx)

		if rx == "" {
			go func() {
				messageHandle(k, tx, []byte{})
				time.Sleep(time.Duration(500) *
					time.Microsecond)
			}()

			continue
		}

		//get all partitions on the given topic
		partitionList, err := consumer.Partitions(rx)
		if err != nil {
			fmt.Println("Error retrieving partitionList ", err)
		}

		initialOffset := sarama.OffsetNewest //OfffsetOldest
		for _, partition := range partitionList {
			pc, _ := consumer.ConsumePartition(rx, partition, initialOffset)

			go func(pc sarama.PartitionConsumer) {
				for message := range pc.Messages() {
					fmt.Printf("[#%d] Received on",
						" [%v,%v]: '%s'\n",
						message.Offset,
						message.Topic,
						message.Partition,
						string(message.Value))
					messageHandle(k, tx, message.Value)
				}
			}(pc)
		}
	}
}

func messageHandle(k *KafkaRunner, tx string, value []byte) {

	resp, err := http.Post(k.Cfg.UpstreamURL, "text/plain", bytes.NewReader(value))
	if err != nil {
		fmt.Println("http post err", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("io read error")
	}

	fmt.Println("response:", string(body))

	if tx != "" {
		messageSend(k, tx, body)
	}
}

func messageSend(k *KafkaRunner, tx string, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic:     tx,
		Partition: -1,
		Value:     sarama.ByteEncoder(value),
	}

	partition, offset, err := k.Producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Could not send message to Topic", tx)
		fmt.Println("Error:", err.Error())
	} else {
		fmt.Printf("message was sent to topic %s partion %d offset is %d\n",
			tx, partition, offset)
	}
}
