package amqp_helper

import (
	"context"
	"log"
	"os"
	"time"

	types_plugin "github.com/CodeClarityCE/utility-types/plugin_db"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func Listen(queue string, callback func(args any, config types_plugin.Plugin, message []byte), args any, config types_plugin.Plugin) {
	// Create connexion
	url := ""
	protocol := os.Getenv("AMQP_PROTOCOL")
	if protocol == "" {
		protocol = "amqp"
	}
	host := os.Getenv("AMQP_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("AMQP_PORT")
	if port == "" {
		port = "5672"
	}
	user := os.Getenv("AMQP_USER")
	if user == "" {
		user = "guest"
	}
	password := os.Getenv("AMQP_PASSWORD")
	if password == "" {
		password = "guest"
	}
	url = protocol + "://" + user + ":" + password + "@" + host + ":" + port + "/"

	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Open channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Listen on sbom queue
	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Read message
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan struct{})
	go func(callback func(args any, config types_plugin.Plugin, message []byte), args any, config types_plugin.Plugin) {
		for d := range msgs {
			// Start analysis
			callback(args, config, []byte(d.Body))
		}
	}(callback, args, config)

	log.Printf(" [*] %s Waiting for messages on %s. To exit press CTRL+C", config.Name, queue)
	<-forever
}

func Send(connection string, data []byte) {
	log.Println("Sending message to " + connection)
	url := ""
	protocol := os.Getenv("AMQP_PROTOCOL")
	if protocol == "" {
		protocol = "amqp"
	}
	host := os.Getenv("AMQP_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("AMQP_PORT")
	if port == "" {
		port = "5672"
	}
	user := os.Getenv("AMQP_USER")
	if user == "" {
		user = "guest"
	}
	password := os.Getenv("AMQP_PASSWORD")
	if password == "" {
		password = "guest"
	}
	url = protocol + "://" + user + ":" + password + "@" + host + ":" + port + "/"

	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		connection, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/javascript",
			Body:        data,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", data)
}
