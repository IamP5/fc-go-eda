package kafka

import (
	"encoding/json"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	ConfigMap *ckafka.ConfigMap
	Producer  *ckafka.Producer
}

func NewKafkaProducer(configMap *ckafka.ConfigMap) *Producer {
	producer, err := ckafka.NewProducer(configMap)
	if err != nil {
		panic(err)
	}
	return &Producer{
		ConfigMap: configMap,
		Producer:  producer,
	}
}

func (p *Producer) Publish(msg interface{}, key []byte, topic string) error {
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	message := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Value:          msgJson,
		Key:            key,
	}
	err = p.Producer.Produce(message, nil)
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) Close() {
	// Wait for all messages to be delivered
	remaining := p.Producer.Flush(15000) // 15 seconds timeout
	if remaining > 0 {
		// Log or handle messages that couldn't be delivered
		println("Warning: producer closed with", remaining, "messages still in queue")
	}
	p.Producer.Close()
}
