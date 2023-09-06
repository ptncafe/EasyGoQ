package EasyGoQ

import (
	"encoding/json"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type IRabbitMqClient interface {
	Publish(queueName string, data interface{}) error
	PublishError(queueName string, data []byte, table amqp.Table, errorCause error) error
	GetChannel(isReuse bool) (*amqp.Channel, error)
	PublishRetry(queueName string, delivery amqp.Delivery) error
}

type rabbitMqClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	logger     ILogger
}

// singleton
var _rabbitMqClient *rabbitMqClient
var mu sync.Mutex

func NewRabbitMqClient(connectionString string, logger ILogger, isReuse bool) (*rabbitMqClient, error) {
	if isReuse == true && _rabbitMqClient != nil {
		return _rabbitMqClient, nil
	}
	mu.Lock()
	defer mu.Unlock()
	rabbitMqConnection, err := amqp.Dial(connectionString)
	if err != nil {
		logger.Errorf("InitConnectionRabbitMq %+v", errors.Wrap(err, "InitConnectionRabbitMq Dial"))
		return nil, err
	}
	go func() {
		logger.Warnf("closing: %s", <-rabbitMqConnection.NotifyClose(make(chan *amqp.Error)))
	}()
	rabbitMqChannel, err := rabbitMqConnection.Channel()
	if err != nil {
		logger.Errorf("InitConnectionRabbitMq %+v", errors.Wrap(err, "InitConnectionRabbitMq Channel"))
		return nil, err
	}
	if isReuse == true {
		_rabbitMqClient = &rabbitMqClient{
			rabbitMqConnection,
			rabbitMqChannel,
			logger,
		}
		return _rabbitMqClient, nil
	}

	return &rabbitMqClient{
		rabbitMqConnection,
		rabbitMqChannel,
		logger,
	}, nil
}

func (rabbitMqClient *rabbitMqClient) Publish(queueName string, data interface{}) error {
	dataJson, _ := json.Marshal(data)

	err := rabbitMqClient.Channel.Publish(queueName, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        dataJson,
		Timestamp:   time.Now(),
	})
	if err != nil {
		rabbitMqClient.logger.Errorf("rabbitMqClient Publish %s %v", queueName, errors.Wrap(err, "Publish"))
	}
	return err
}

func (rabbitMqClient *rabbitMqClient) PublishRetry(queueName string, delivery amqp.Delivery) error {

	err := rabbitMqClient.Channel.Publish(queueName, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        delivery.Body,
		Timestamp:   time.Now(),
		Headers:     delivery.Headers,
	})
	if err != nil {
		rabbitMqClient.logger.Errorf("rabbitMqClient Publish %s %v", queueName, errors.Wrap(err, "Publish"))
	}
	return err
}
func (rabbitMqClient *rabbitMqClient) PublishError(queueName string, data []byte, table amqp.Table, errorCause error) error {
	if table == nil {
		table = amqp.Table{
			"queueName":   queueName,
			"retry-no":    1,
			"errorCause":  errorCause.Error(),
			"createdDate": time.Now(),
			"updatedDate": time.Now(),
		}
	} else {
		table["queueName"] = queueName
		retryNo, _ := table["retry-no"].(int32)

		table["retry-no"] = retryNo + 1
		table["errorCause"] = errorCause.Error()
		table["updatedDate"] = time.Now()
	}
	rabbitMqClient.logger.Warnf("rabbitMqClient PublishError %s %v %v", queueName, table, data)
	err := rabbitMqClient.Channel.Publish(GetErrorQueueName(queueName), "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
		Timestamp:   time.Now(),
		Headers:     table,
	})
	if err != nil {
		rabbitMqClient.logger.Errorf("rabbitMqClient Publish %s %v", queueName, errors.Wrap(err, "Publish"))
	}
	return err
}

func (rabbitMqClient *rabbitMqClient) GetChannel(isReuse bool) (*amqp.Channel, error) {

	if isReuse {
		return _rabbitMqClient.Channel, nil
	}

	rabbitMqChannel, err := rabbitMqClient.Connection.Channel()
	if err != nil {
		rabbitMqClient.logger.Errorf("rabbitMqClient InitConnectionRabbitMq %+v", errors.Wrap(err, "InitConnectionRabbitMq Channel"))
		return nil, err
	}
	//defer rabbitMqChannel.Close()
	return rabbitMqChannel, nil
}

func (rabbitMqClient *rabbitMqClient) Close() error {
	return rabbitMqClient.Connection.Close()
}
