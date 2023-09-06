package EasyGoQ

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// InitRabbitMq mind set là exchange và queue sẽ dùng tên, 1 exchange và 1 queue
func RegisterQueue(connectionString string, logger ILogger, queueNames []string) error {
	rabbitMqClient, err := NewRabbitMqClient(connectionString, logger, true)
	if err != nil {
		logger.Errorf("RegisterQueue %s", err.Error())
		return err
	}
	rabbitMqChannel, err := rabbitMqClient.GetChannel(true)
	if err != nil {
		panic(err)
	}
	for _, name := range queueNames {
		_ = InitExchange(rabbitMqChannel, name)
		_ = InitQueue(rabbitMqChannel, name)
		//return err
		logger.Infof("rabbitmq_provider RegisterQueue done %s", name)
	}
	for _, name := range queueNames {
		_ = InitExchange(rabbitMqChannel, GetErrorQueueName(name))
		_ = InitQueue(rabbitMqChannel, GetErrorQueueName(name))

		_ = InitExchange(rabbitMqChannel, GetDeadQueueName(name))
		_ = InitQueue(rabbitMqChannel, GetDeadQueueName(name))
		//return err
		logger.Infof("rabbitmq_provider RegisterQueue done %s", name)
	}
	return nil
}

func InitExchange(channel *amqp.Channel, name string) error {
	err := channel.ExchangeDeclare(name, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("rabbitmq_provider InitExchange %s %+v ", name, errors.Wrap(err, "InitExchange"))
		return err
	}
	return nil
}

func InitQueue(channel *amqp.Channel, name string) error {
	_, err := channel.QueueDeclare(name, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("rabbitmq_provider InitQueue %s %+v ", name, errors.Wrap(err, "InitQueue"))
		return err
	}
	err = channel.QueueBind(
		name, // queue name
		"",   // routing key
		name, // exchange
		false,
		nil)
	if err != nil {
		log.Fatalf("InitQueue %s %+v ", name, errors.Wrap(err, "InitQueue"))
		return err
	}
	log.Printf("InitQueue %s done ", name)

	return nil
}
