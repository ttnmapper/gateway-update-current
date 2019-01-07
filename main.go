package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/kellydunn/golang-geo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
	"log"
	"math"
	"net/http"
	"os"
	"ttnmapper-gateway-update-current/types"
)

var messageChannel = make(chan types.GatewayStatus)
var movedChannel = make(chan types.GatewayStatus)
var newChannel = make(chan types.GatewayStatus)

type Configuration struct {
	AmqpHost     string
	AmqpPort     string
	AmqpUser     string
	AmqpPassword string

	MysqlHost     string
	MysqlPort     string
	MysqlUser     string
	MysqlPassword string
	MysqlDatabase string

	PromethuesPort string
}

var myConfiguration = Configuration{
	AmqpHost:     "localhost",
	AmqpPort:     "5672",
	AmqpUser:     "user",
	AmqpPassword: "password",

	MysqlHost:     "localhost",
	MysqlPort:     "3306",
	MysqlUser:     "user",
	MysqlPassword: "password",
	MysqlDatabase: "database",

	PromethuesPort: "2114",
}

func main() {

	file, err := os.Open("conf.json")
	if err != nil {
		log.Print(err.Error())
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&myConfiguration)
	if err != nil {
		log.Print(err.Error())
	}
	err = file.Close()
	if err != nil {
		log.Print(err.Error())
	}
	log.Printf("Using configuration: %+v", myConfiguration) // output: [UserA, UserB]

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe("127.0.0.1:"+myConfiguration.PromethuesPort, nil)
		if err != nil {
			log.Print(err.Error())
		}
	}()

	// Start thread to handle MySQL inserts
	go insertToMysql()

	go publishMovedGateways()
	go publishNewGateways()

	// Start amqp listener on this thread - blocking function
	subscribeToRabbit()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func subscribeToRabbit() {
	conn, err := amqp.Dial("amqp://" + myConfiguration.AmqpUser + ":" + myConfiguration.AmqpPassword + "@" + myConfiguration.AmqpHost + ":" + myConfiguration.AmqpPort + "/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"gateway_locations", // name
		"fanout",            // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"mysql_gateway_update_status", // name
		false,                         // durable
		false,                         // delete when usused
		false,                         // exclusive
		false,                         // no-wait
		nil,                           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,              // queue name
		"",                  // routing key
		"gateway_locations", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

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

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			//log.Printf(" [a] %s", d.Body)
			var packet types.GatewayStatus
			if err := json.Unmarshal(d.Body, &packet); err != nil {
				log.Print(" [a] " + err.Error())
				continue
			}
			log.Print(" [a] Packet received")
			messageChannel <- packet
		}
	}()

	log.Printf(" [a] Waiting for packets. To exit press CTRL+C")
	<-forever

}

func insertToMysql() {

	db, err := sqlx.Open("mysql", myConfiguration.MysqlUser+":"+myConfiguration.MysqlPassword+"@tcp("+myConfiguration.MysqlHost+":"+myConfiguration.MysqlPort+")/"+myConfiguration.MysqlDatabase+"?parseTime=true")
	if err != nil {
		panic(err.Error())
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	stmtSelectForced, err := db.PrepareNamed("SELECT * FROM gateway_forced_locations WHERE gtw_id = :gtw_id")
	if err != nil {
		panic(err.Error())
	}
	defer stmtSelectForced.Close()

	stmtSelect, err := db.PrepareNamed("SELECT * FROM gateway_status_current WHERE gtw_id = :gtw_id")
	if err != nil {
		panic(err.Error())
	}
	defer stmtSelect.Close()

	stmtInsert, err := db.PrepareNamed("INSERT INTO gateway_status_current " +
		"(gtw_id, description, first_seen, last_seen," +
		"latitude, longitude, altitude, location_accuracy, location_source) " +
		"VALUES " +
		"(:gtw_id, :description, :first_seen, :last_seen," +
		":latitude, :longitude, :altitude, :location_accuracy, :location_source)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtInsert.Close()

	stmtUpdateCoordinates, err := db.PrepareNamed("UPDATE gateway_status_current SET " +
		"description=:description, first_seen=:first_seen, last_seen=:last_seen," +
		"latitude=:latitude, longitude=:longitude, altitude=:altitude, location_accuracy=:location_accuracy, location_source=:location_source " +
		"WHERE gtw_id = :gtw_id")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpdateCoordinates.Close()

	stmtUpdateDescription, err := db.PrepareNamed("UPDATE gateway_status_current SET " +
		"description=:description, first_seen=:first_seen, last_seen=:last_seen " +
		"WHERE gtw_id = :gtw_id")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpdateDescription.Close()

	stmtUpdateSeen, err := db.PrepareNamed("UPDATE gateway_status_current SET " +
		"first_seen=:first_seen, last_seen=:last_seen " +
		"WHERE gtw_id = :gtw_id")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpdateSeen.Close()

	for {
		message := <-messageChannel
		log.Printf(" [m] Processing gateway status")

		// Check list of ignored gateways
		ignoredGateways := map[string]struct{}{"0102030405060708": {}}
		if _, ok := ignoredGateways[message.GtwId]; ok {
			log.Printf("  [I] Ignored gateway: %s", message.GtwId)
		}

		// If the gateway exists in the force table, change the coordinates in message now
		rows, err := stmtSelectForced.Query(message)
		if err != nil {
			log.Print(" Forced select: " + err.Error())
		} else {
			for rows.Next() {
				var id int64
				var gtwId string
				var latitude float32
				var longitude float32
				var altitude sql.NullInt64
				err = rows.Scan(&id, &gtwId, &latitude, &longitude, &altitude)
				if err != nil {
					log.Print(err.Error())
					continue
				}

				log.Print("  [F] Forced location found")
				message.Location.Latitude = latitude
				message.Location.Longitude = longitude
				if altitude.Valid {
					message.Location.Altitude = int32(altitude.Int64)
				}
			}
		}

		// Check if coordinates are valid
		err = validateLocation(message.Location)
		if err != nil {
			log.Print("  [E] " + err.Error())
			continue
		}

		// Clamp outlier altitudes to sea level
		if math.Abs(float64(message.Location.Altitude)) > 99999 {
			log.Print("  [A] Clamping out of range altitude to sea level")
			message.Location.Altitude = 0
		}

		// 1. Select:
		//    gateway exist, location changed
		var entry = types.MysqlGatewayStatus{}

		result := stmtSelect.QueryRow(message)
		err = result.StructScan(&entry)
		if err != nil {
			log.Print("  [m] " + err.Error())
		}

		if entry.LastSeen.After(message.LastSeen.GetTime()) {
			log.Print("  [m] Ignoring backwards jump in status time")
			continue
		}

		if entry.GtwId == "" {
			log.Print("  [m] Adding new gateway")
			// The struct is empty so there was no previous entry in the database
			notifyGatewayNew(message)

			// Insert a new entry
			copyMessageToEntry(message, &entry)
			insertResult, err := stmtInsert.Exec(entry)
			if err != nil {
				log.Print(err.Error())
			} else {
				lastId, err := insertResult.LastInsertId()
				if err != nil {
					log.Print(err.Error())
				}

				rowsAffected, err := insertResult.RowsAffected()
				if err != nil {
					log.Print(err.Error())
				}

				log.Printf("  [m] Inserted entry id=%d (affected %d rows)", lastId, rowsAffected)
			}

		} else if distanceBetweenLocationsMetres(entry.Latitude, entry.Longitude, entry.Altitude,
			message.Location.Latitude, message.Location.Longitude, message.Location.Altitude) > 100.0 {
			// We assume we get the statuses in chronologically
			log.Print("  [m] Updating coordinates")
			// Gateway moved
			notifyGatewayMoved(message)

			// Updated the coordinates
			copyMessageToEntry(message, &entry)
			entry.FirstSeen = entry.LastSeen // when moved the first seen time is the time the gateway move, ie. last seen
			updateResult, err := stmtUpdateCoordinates.Exec(entry)
			if err != nil {
				log.Print(err.Error())
			}
			rowsAffected, err := updateResult.RowsAffected()
			if err != nil {
				log.Print(err.Error())
			}
			if rowsAffected > 0 {
				log.Printf("  [m] Updated %d rows", rowsAffected)
			}

		} else if entry.Description != message.Description {
			log.Print("  [m] Updating description")
			// Update the description
			copyMessageToEntry(message, &entry)
			updateResult, err := stmtUpdateDescription.Exec(entry)
			if err != nil {
				log.Print(err.Error())
			}
			rowsAffected, err := updateResult.RowsAffected()
			if err != nil {
				log.Print(err.Error())
			}
			if rowsAffected > 0 {
				log.Printf("  [m] Updated %d rows", rowsAffected)
			}

		} else if entry.LastSeen.Before(message.LastSeen.GetTime()) || entry.FirstSeen.After(message.LastSeen.GetTime()) {
			log.Print("  [m] Updating last and first heard times")
			// Only update the last_heard
			copyMessageToEntry(message, &entry)
			updateResult, err := stmtUpdateSeen.Exec(entry)
			if err != nil {
				log.Print(err.Error())
			}
			rowsAffected, err := updateResult.RowsAffected()
			if err != nil {
				log.Print(err.Error())
			}
			if rowsAffected > 0 {
				log.Printf("  [m] Updated %d rows", rowsAffected)
			}

		} else {
			log.Print("  [m] Gateway already up to date")

		}

	}
}

func copyMessageToEntry(message types.GatewayStatus, entry *types.MysqlGatewayStatus) {
	entry.GtwId = message.GtwId
	entry.Description = message.Description

	entry.Latitude = message.Location.Latitude
	entry.Longitude = message.Location.Longitude
	entry.Altitude = message.Location.Altitude
	entry.Accuracy = message.Location.Accuracy
	entry.Source = message.Location.Source

	// We assume we get the statuses in chronologically
	if message.LastSeen.GetTime().After(entry.LastSeen) {
		entry.LastSeen = message.LastSeen.GetTime()
	}

	if entry.FirstSeen.IsZero() {
		entry.FirstSeen = message.LastSeen.GetTime()
	}
}

func notifyGatewayNew(message types.GatewayStatus) {
	log.Printf("  [N] New gateway: %s", message.GtwId)
	newChannel <- message

}

func notifyGatewayMoved(message types.GatewayStatus) {
	log.Printf("  [M] Gateway moved: %s", message.GtwId)
	movedChannel <- message

}

func validateLocation(location types.LocationMetadata) error {

	// Null island. Will also catch 0,0
	if location.Latitude < 1 && location.Latitude > -1 && location.Longitude < 1 && location.Longitude > -1 {
		return errors.New("null island")
	}

	if math.Abs(float64(location.Latitude)) > 90 {
		return errors.New("latitude out of range")
	}
	if math.Abs(float64(location.Longitude)) > 180 {
		return errors.New("longitude out of range")
	}

	if location.Latitude == 52 && location.Longitude == 6 {
		return errors.New("default single channel gateway location")
	}

	if location.Latitude == 10 && location.Longitude == 20 {
		return errors.New("default Lorrier LR2 location")
	}

	if location.Latitude == 50.008724 && location.Longitude == 36.215805 {
		return errors.New("Ukrainian hack")
	}

	return nil
}

func distanceBetweenLocationsMetres(latOld float32, lonOld float32, altOld int32, latNew float32, lonNew float32, altNew int32) float64 {
	var dist float64

	p := geo.NewPoint(float64(latOld), float64(lonOld))
	p2 := geo.NewPoint(float64(latNew), float64(lonNew))

	dist = p.GreatCircleDistance(p2) * 1000.0

	dh := math.Abs(float64(altNew) - float64(altOld))
	dist = math.Sqrt(math.Pow(dist, 2) + math.Pow(dh, 2))

	return dist
}

func publishMovedGateways() {
	conn, err := amqp.Dial("amqp://" + myConfiguration.AmqpUser + ":" + myConfiguration.AmqpPassword + "@" + myConfiguration.AmqpHost + ":" + myConfiguration.AmqpPort + "/")
	//if err != nil {
	//	log.Print("Error connecting to RabbitMQ")
	//	return
	//}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"gateway_moved", // name
		"fanout",        // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)

	for {
		message := <-movedChannel
		log.Printf("   [A] Publishing message")

		data, err := json.Marshal(message)
		if err != nil {
			log.Printf("marshal failed: %s", err)
			continue
		}

		err = ch.Publish(
			"gateway_moved", // exchange
			"",              // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(data),
			})
		failOnError(err, "Failed to publish a gateway")

		log.Printf("   [A] Published: %s", data)
	}
}

func publishNewGateways() {
	conn, err := amqp.Dial("amqp://" + myConfiguration.AmqpUser + ":" + myConfiguration.AmqpPassword + "@" + myConfiguration.AmqpHost + ":" + myConfiguration.AmqpPort + "/")
	//if err != nil {
	//	log.Print("Error connecting to RabbitMQ")
	//	return
	//}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"gateway_new", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)

	for {
		message := <-newChannel
		log.Printf("   [A] Publishing message")

		data, err := json.Marshal(message)
		if err != nil {
			log.Printf("marshal failed: %s", err)
			continue
		}

		err = ch.Publish(
			"gateway_new", // exchange
			"",            // routing key
			false,         // mandatory
			false,         // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(data),
			})
		failOnError(err, "Failed to publish a gateway")

		log.Printf("   [A] Published: %s", data)
	}
}
