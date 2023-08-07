package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	amgp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn  *amgp.Connection
	queue string
}

func NewConsumer(conn *amgp.Connection) (Consumer, error) {
	c := Consumer{
		conn: conn,
	}

	// consumer.setup() is for creating the queue and binding it to the exchange

	err := c.setup()

	if err != nil {
		return Consumer{}, err
	}

	return c, nil
}

func (c *Consumer) setup() error {
	// channel, err := c.conn.Channel() is for creating a channel (we'll use it to create the queue)
	channel, err := c.conn.Channel()

	if err != nil {
		return err
	}

	// declareExchange(channel) is for creating the exchange

	return declareExchange(channel)
}

type Payload struct {
	Id_user int    `json:"id_user"`
	Content string `json:"content"`
	Type    string `json:"type"`
}

func (c *Consumer) Listen(topics []string) error {
	fmt.Println("Listening for messages... consumer.go")
	// channel, err := c.conn.Channel() is for creating a channel (we'll use it to create the queue)
	channel, err := c.conn.Channel()

	if err != nil {
		return err
	}

	// defer channel.Close() will not work here, because the program will exit before it is called
	defer channel.Close()

	//declareRandomQueue(channel) is for creating the queue (the name of the queue will be a random string)

	queue, err := declareRandomQueue(channel)

	if err != nil {
		return err
	}

	// for _, topic := range topics is for looping through the topics we want to listen to (e.g. "post.created", "post.deleted", "post.updated")
	fmt.Println("topics: ", topics)
	for _, s := range topics {
		fmt.Println("s: ", s)
		fmt.Println("queue.Name: ", queue.Name)
		// bindQueue(channel, queue, topic) is for binding the queue to the exchange (so that the queue will receive messages published to the exchange)

		// channel.QueueBind(queue, s, "events", false, nil) is for binding the queue to the exchange (so that the queue will receive messages published to the exchange)
		channel.QueueBind(queue.Name, s, "events", false, nil)

		if err != nil {
			return err
		}

	}

	// msgs, err := channel.Consume(queue, "", true, false, false, false, nil) is for consuming messages from the queue (we'll use it for receiving messages)
	messages, err := channel.Consume(queue.Name, "", true, false, false, false, nil)
	fmt.Println("messages: ", messages)

	if err != nil {
		return err
	}

	// forever:= make(chan bool) is for blocking the main thread (so that the program doesn't exit before we receive any messages)

	forever := make(chan bool)

	// go func() is for running a function in a goroutine (a lightweight thread)
	go func() {
		for d := range messages {
			var payload Payload

			fmt.Println("d.Body: ", d.Body)
			_ = json.Unmarshal(d.Body, &payload)

			fmt.Println("payload: ", payload)

			go handlePayload(payload)

		}
	}()

	fmt.Println("Listening for messages...")
	// <-forever is for blocking the main thread (so that the program doesn't exit before we receive any messages)
	<-forever

	return nil
}

// func handlePayload(payload Payload) is for handling the payload (printing it to the console)

func handlePayload(payload Payload) {
	fmt.Printf("Received message: %s\n", payload.Id_user)

	fmt.Printf("Received message: %s\n", payload.Content)

	switch payload.Type {
	case "post.created":
		err := postCreated(payload)
		if err != nil {
			fmt.Println("134-consumer.go " + err.Error())
			fmt.Println(err)
		}
	case "post.deleted":
		fmt.Println("post deleted")
	case "post.updated":
		fmt.Println("post updated")
	default:
		err := fmt.Errorf("unknown event: %s", payload.Type)
		if err != nil {
			fmt.Println(err)
		}
	}
}

// func logEvent(payload Payload) is for logging the event to the database (using the event-service)

type PostCreatedPayload struct {
	Id_user int    `json:"id_user"`
	Content string `json:"content"`
}

func postCreated(payload Payload) error {

	payloadPost := PostCreatedPayload{
		Id_user: payload.Id_user,
		Content: payload.Content,
	}

	jsonDat, _ := json.MarshalIndent(payloadPost, "", "\t")

	request, err := http.NewRequest("POST", "http://post-service/v1/post", bytes.NewBuffer(jsonDat))

	if err != nil {

		return err
	}

	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	response, err := client.Do(request)

	if err != nil {

		return err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {

		return errors.New("unexpected status from post-service: " + response.Status)
	}

	return nil
}
