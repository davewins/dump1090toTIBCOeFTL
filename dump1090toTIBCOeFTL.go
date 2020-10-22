package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	// create a struct of the incoming CSV
	type sbs1message struct {
		MessageType      string
		TransmissionType string
		SessionID        string
		AircraftID       string
		HexIdent         string
		FlightID         string
		DateGenerated    string
		TimeGenerated    string
		DateLogged       string
		TimeLogged       string
		Callsign         string
		Altitude         int
		GroundSpeed      int
		Track            float32
		Latitude         float32
		Longitude        float32
		VerticalRate     string
		Squawk           string
		Alert            string
		Emergency        string
		SPI              string
		IsOnGround       string
	}

	type streamingMessage struct {
		//Format of the message for Spotfire Streaming
		//{"ICAO":"string","FlightId":"string","Altitude":"int","Latitude":"double","Longitude":"double","Heading":"double","Speed":"int","LastReceiveTime":"timestamp","StartReceiveTime":"timestamp","Region":"string","SourceID":"string"},"key":["CQSInternalID"]}}
		ICAO             string
		FlightId         string
		Altitude         int64
		Latitude         float64
		Longitude        float64
		Heading          float64
		Speed            int64
		LastReceiveTime  int64
		StartReceiveTime int64
		Region           string
		SourceID         string
	}

	dump1090URL := flag.String("dump1090URL", "", "The host:port URL of dump1090. e.g: -dump1090URL localhost:30003 (Required)")
	streamingHostURL := flag.String("streamingHostURL", "", "The host:port of the TIBCO Streaming server. e.g. -streamingHostURL https://streaming.spotfire-cloud.com:443 (Required)")
	streamingHostUsername := flag.String("streamingHostUsername", "", "The username you wish to authenticate against the TIBCO Streaming server. e.g. -streamingHostUsername foo (Required)")
	streamingHostPassword := flag.String("streamingHostPassword", "", "The password of the user to authenticate against the TIBCO Streaming server. e.g. -streamingHostPassword bar (Required)")
	flag.Parse()

	//Check we have each of the command line arguments
	if *dump1090URL == "" || *streamingHostURL == "" || *streamingHostUsername == "" || *streamingHostPassword == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// initialize http client
	client := &http.Client{}
	c, err := net.Dial("tcp", *dump1090URL)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		//Listen for TCP messages
		//message, _ := bufio.NewReader(c).ReadString('\n')
		message, _, _ := bufio.NewReader(c).ReadLine()
		if len(message) > 2 {
			//fmt.Print("->: " + string(message))
			r := csv.NewReader(strings.NewReader(string(message)))

			records, err1 := r.ReadAll()
			if err1 != nil {
				log.Fatal(err)
			}
			fmt.Println(records)
			//fmt.Println("CSV Field Count: ", len(records[0]), "last value = ", records[0][len(records[0])-1])
			//expand the slice to all 22 entries if not all fields received
			for i := len(records[0]); i < 22; i++ {
				records[0] = append(records[0], "")
			}
			//fmt.Println("New CSV Field Count: ", len(records[0]), "last value = ", records[0][len(records[0])-1])
			ICAO := records[0][4]
			//Sometimes the TCP read doesn't read the full record properly, so check to see if the ICAO code is exaclty 6 chars in length
			if len(ICAO) == 6 {

				altitude, _ := strconv.ParseInt(records[0][11], 0, 64)
				latitude, _ := strconv.ParseFloat(records[0][14], 64)
				longitude, _ := strconv.ParseFloat(records[0][15], 64)
				heading, _ := strconv.ParseFloat(records[0][13], 64)
				speed, _ := strconv.ParseInt(records[0][12], 0, 64)

				var streamingMessages = []streamingMessage{
					streamingMessage{
						ICAO:             records[0][4],
						FlightId:         records[0][10],
						Altitude:         altitude,
						Latitude:         latitude,
						Longitude:        longitude,
						Heading:          heading,
						Speed:            speed,
						LastReceiveTime:  time.Now().UnixNano() / 1000000,
						StartReceiveTime: time.Now().UnixNano() / 1000000,
						Region:           "Gloucestershire, United Kingdom",
						SourceID:         "davewinstone"}}

				// use MarshalIndent to reformat slice array as JSON
				sbMessage, _ := json.Marshal(streamingMessages)
				// print the reformatted struct as JSON
				fmt.Printf("%s\n", sbMessage)

				// Publish message to TIBCO Streaming
				req, err := http.NewRequest(http.MethodPut, *streamingHostURL, bytes.NewBuffer(sbMessage))
				req.SetBasicAuth(*streamingHostUsername, *streamingHostPassword)
				// set the request header Content-Type for json
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
				req.Header.Set("Accept", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					panic(err)
				}
				defer resp.Body.Close()
				bodyBytes, err := ioutil.ReadAll(resp.Body)
				fmt.Println("TIBCO Streaming Response:", resp.StatusCode, ":", string(bodyBytes))
			}
		}
	}
}
