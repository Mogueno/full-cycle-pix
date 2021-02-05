package kafka

import (
	"fmt"
	"os"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
	"github.com/mogueno/application/factory"
	appmodel "github.com/mogueno/application/model"
	"github.com/mogueno/application/usecase"
	"github.com/mogueno/domain/model"
)

type KafkaProcessor struct {
	Database     *gorm.DB
	Producer     *ckafka.Producer
	DeliveryChan chan ckafka.Event
}

func NewKafkaProcessor(database *gorm.DB, producer *ckafka.Producer, deliveryChan chan ckafka.Event) *KafkaProcessor {
	return &KafkaProcessor{
		Database:     database,
		Producer:     producer,
		DeliveryChan: deliveryChan,
	}
}

func (k *KafkaProcessor) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
		"group.id":          os.Getenv("kafkaConsumerGroupId"),
		"auto.offset.reset": "earliest",
	}
	c, err := ckafka.NewConsumer(configMap) //Comecei a ler o topico do kafka

	if err != nil {
		panic(err)
	}

	topics := []string{os.Getenv("kafkaTransactionTopic"), os.Getenv("kafkaTransactionConfirmationTopic")}
	c.SubscribeTopics(topics, nil)

	fmt.Println("kafka consumer has been started")

	for {
		msg, err := c.ReadMessage(-1) //Recebi a mensagem
		if err == nil {
			k.processMessage(msg) // Mando ela processar
		}
	}

}

func (k *KafkaProcessor) processMessage(msg *ckafka.Message) {
	transactionsTopic := "transactions" // Se a mensagem for do tipo transaction
	transactionConfirmationTopic := "transaction_confirmation"

	switch topic := *msg.TopicPartition.Topic; topic {
	case transactionsTopic:
		k.processTransaction(msg)
	case transactionConfirmationTopic:
		k.processTransactionConfirmation(msg)
	default:
		fmt.Println("not a valid topic", string(msg.Value))

	}
}

func (k *KafkaProcessor) processTransaction(msg *ckafka.Message) error { // Como processar a mensagem (recebe a mensagem do kafka)
	transaction := appmodel.NewTransaction() // Cria uma transacao
	err := transaction.ParseJson(msg.Value)  //parseio do json pra minha struct em GO
	if err != nil {
		return err
	}

	transactionUseCase := factory.TransactionUseCaseFactory(k.Database) //se os dados estiverem validos, eu registro os dados no Database

	createdtransaction, err := transactionUseCase.Register(
		transaction.AccountID,
		transaction.Amount,
		transaction.PixKeyTo,
		transaction.PixKeyKindTo,
		transaction.Description,
	)

	if err != nil {
		fmt.Println("error registering transaction", err)
		return err
	}

	topic := "bank" + createdtransaction.PixKeyTo.Account.Bank.Code
	transaction.ID = createdtransaction.ID
	transaction.Status = model.TransactionPending
	transactionJson, err := transaction.ToJson()

	if err != nil {
		return err
	}

	err = Publish(string(transactionJson), topic, k.Producer, k.DeliveryChan) // Publica para o banco de destino
	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaProcessor) processTransactionConfirmation(msg *ckafka.Message) error {
	transaction := appmodel.NewTransaction() // Cria uma transacao
	err := transaction.ParseJson(msg.Value)  //parseio do json pra minha struct em GO
	if err != nil {
		return err
	}

	transactionUseCase := factory.TransactionUseCaseFactory(k.Database) //se os dados estiverem validos, eu registro os dados no Database

	if transaction.Status == model.TransactionConfirmed {
		err = k.confirmTransaction(transaction, transactionUseCase)
		if err != nil {
			return err
		}
	} else if transaction.Status == model.TransactionCompleted {
		_, err := transactionUseCase.Complete(transaction.ID)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (k *KafkaProcessor) confirmTransaction(transaction *appmodel.Transaction, transactionUseCase usecase.TransactionUseCase) error {
	confirmedTransaction, err := transactionUseCase.Confirm(transaction.ID)
	if err != nil {
		return err
	}
	topic := "bank" + confirmedTransaction.AccountFrom.Bank.Code
	transactionJson, err := transaction.ToJson()

	if err != nil {
		return err
	}

	err = Publish(string(transactionJson), topic, k.Producer, k.DeliveryChan)
	if err != nil {
		return err
	}
	return nil
}
