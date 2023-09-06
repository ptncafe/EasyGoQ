package EasyGoQ

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"

	"log"
)

type HandlerConsumer[T any] func(data T) error

type CoreConsumer struct {
	rabbitMqClient IRabbitMqClient
	queueName      string
}

// Qos koh chạy, phải gắn 1 connecion, nhiều channel
func NewCoreConsumer[T any](connectionString string, logger ILogger, queueName string, concurrency int, refetchCount int, handler HandlerConsumer[T]) (*CoreConsumer, error) {
	rabbitMqClient, err := NewRabbitMqClient(connectionString, logger, false)
	if err != nil {
		logger.Errorf("NewCoreConsumer %s", Dump(err))
		return nil, err
	}

	var coreConsumer = CoreConsumer{
		rabbitMqClient: rabbitMqClient,
		queueName:      queueName,
	}
	for i := 0; i < concurrency; i++ {
		_, err := newConsumer(coreConsumer, rabbitMqClient, logger, queueName, refetchCount, handler)
		if err != nil {
			return nil, err
		}
	}
	return &coreConsumer, nil
}

func newConsumer[T any](coreConsumer CoreConsumer, rabbitMqClient IRabbitMqClient, logger ILogger, queueName string, refetchCount int, handler HandlerConsumer[T]) (*CoreConsumer, error) {
	done := make(chan error)
	channel, err := rabbitMqClient.GetChannel(false)
	//defer channel.Close()
	if err != nil {
		logger.Errorf("NewConsumer %s %s", queueName, Dump(err))
		return nil, fmt.Errorf("Channel: %s", err)
	}
	err = channel.Qos(refetchCount, 0, false)
	if err != nil {
		logger.Errorf("NewConsumer Qos %s %s", queueName, Dump(err))
		return nil, fmt.Errorf("Qos: %s", err)
	}
	logger.Infof("Queue bound to Exchange, starting Consume (consumer tag %q)", queueName)

	deliveries, err := channel.Consume(
		queueName, // name
		queueName, // consumerTag,
		false,     // noAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		logger.Errorf("NewConsumer Consume %s %s", queueName, Dump(err))
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(coreConsumer, deliveries, done, queueName, logger, handler)
	return &coreConsumer, nil
}

func handle[T any](conmonConsumer CoreConsumer, deliveries <-chan amqp.Delivery, done chan error, queueName string, logger ILogger, handler HandlerConsumer[T]) {
	logger.Debugf("handle total %v %s", len(deliveries), Dump(deliveries))
	//var wait sync.WaitGroup
	for d := range deliveries {
		//go func() {
		err := processMessage[T](conmonConsumer, d, queueName, handler, logger)
		if err == nil {
			d.Ack(false)
		} else {
			logger.Errorf("NewCoreConsumer handle %s %s %s", queueName, queueName, Dump(err))
		}
		//}()
	}
	//wait.Wait()
	logger.Warnf("handle: deliveries channel closed")
	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func processMessage[T any](conmonConsumer CoreConsumer, d amqp.Delivery, queueName string, handler HandlerConsumer[T], logger ILogger) error {
	var err error
	PanicWrapper(logger, queueName, func() {
		var dataConsume T
		_ = json.Unmarshal(d.Body, &dataConsume)
		err := handler(dataConsume)
		if err != nil {
			logger.Errorf("CoreConsumer handler error %s", Dump(err))
			err = conmonConsumer.rabbitMqClient.PublishError(queueName, d.Body, d.Headers, errors.Wrap(err, "CoreConsumer"))
			if err != nil {
				logger.Errorf("CoreConsumer PublishError %s", Dump(err))
			}
		}
	})
	return err

}
