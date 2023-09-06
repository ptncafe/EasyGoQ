package EasyGoQ

import (
	"fmt"
	"github.com/pkg/errors"
)

func RetryErrorMessageAll(connectionString string, queueNames []string, sizeMessage int, logger ILogger) error {
	for _, name := range queueNames {
		name = GetErrorQueueName(name)
		logger.Debugf("rabbitmq_provider RetryErrorMessageAll start %s", Dump(name))
		err := RetryErrorMessage(connectionString, name, sizeMessage, logger)
		if err != nil {
			logger.Errorf("rabbitmq_provider RetryErrorMessageAll start %+v", errors.Wrap(err, "RetryErrorMessageAll"))
			return err
		}
		logger.Infof("rabbitmq_provider RetryErrorMessageAll done %s", Dump(name))
	}
	return nil
}

func RetryErrorMessage(connectionString string, queueErrorName string, sizeMessage int, logger ILogger) error {
	rabbitMqClient, err := NewRabbitMqClient(connectionString, logger, false)
	if err != nil {
		logger.Errorf("RetryErrorMessage %s %v", queueErrorName, errors.Wrap(err, "NewConsumer"))
		return fmt.Errorf("RetryErrorMessage channel : %s", err)
	}
	channel, err := rabbitMqClient.GetChannel(false)
	if err != nil {
		logger.Errorf("RetryErrorMessage %s %v", queueErrorName, errors.Wrap(err, "NewConsumer"))
		return fmt.Errorf("RetryErrorMessage channel : %s", err)
	}
	err = channel.Qos(1, 0, false)
	if err != nil {
		logger.Errorf("RetryErrorMessage Qos %s %v", queueErrorName, errors.Wrap(err, "NewConsumer"))
		return fmt.Errorf("RetryErrorMessage Qos : %s", err)
	}
	logger.Infof("RetryErrorMessage, starting Consume (consumer tag %q)", queueErrorName)

	for i := 1; i < sizeMessage; i++ {
		var d, ok, err = channel.Get(queueErrorName, false)
		if ok == false {
			logger.Warnf("RetryErrorMessage retry %v", ok)
			break
		}
		if err != nil {
			logger.Errorf("RetryErrorMessage retry %v", ok)
			break
		}

		logger.Infof("RetryErrorMessage retry %s => %s => %s", string(d.Body), fmt.Sprint(d.Body), Dump(d.Headers))

		table := d.Headers
		if table == nil {
			logger.Errorf("RetryErrorMessage retry  %s %s", fmt.Sprint(d.Body), Dump(d.Headers))
			continue
		}
		queueName := table["queueName"].(string)
		retryNo, ok := table["retry-no"].(int32)
		if ok == false {
			retryNo = 0
		}
		//dataJson,_:=json.Marshal(d.Body)
		if retryNo > 10 {
			err = rabbitMqClient.PublishRetry(GetDeadQueueName(queueName), d)
			if err != nil {
				logger.Errorf("RetryErrorMessage retry  %s %v %s", string(d.Body), err, Dump(d.Headers))
				continue
			}
			d.Ack(false)
			continue
		}
		err = rabbitMqClient.PublishRetry(queueName, d)
		if err != nil {
			logger.Errorf("RetryErrorMessage retry  %s %v %s", string(d.Body), err, Dump(d.Headers))
			continue
		}
		d.Ack(false)
	}

	//defer channel.Close()
	defer rabbitMqClient.Connection.Close()
	return nil
}

func GetErrorQueueName(queue string) string {
	return fmt.Sprintf("%s_error", queue)
}

func GetDeadQueueName(queue string) string {
	return fmt.Sprintf("%s_error_error", queue)
}
