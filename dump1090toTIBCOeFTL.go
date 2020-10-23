package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"tibco.com/eftl"
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

	//	{ "now" : 1603405039.7,
	//  "messages" : 59125883,
	//  "aircraft" : [
	//	{"hex":"40055f","flight":"AWC2B   ","alt_baro":32475,"alt_geom":32350,"gs":437.7,"ias":268,"tas":434,"mach":0.752,"track":124.7,"track_rate":-0.03,"roll":-0.9,"mag_heading":130.6,"baro_rate":-1024,"geom_rate":-992,"squawk":"6316","emergency":"none","category":"A3","nav_altitude_mcp":20000,"nav_heading":130.8,"lat":52.883469,"lon":-1.991577,"nic":8,"rc":186,"seen_pos":0.3,"version":2,"nic_baro":1,"nac_p":10,"nac_v":2,"sil":3,"sil_type":"perhour","gva":2,"sda":2,"mlat":[],"tisb":[],"messages":2582,"seen":0.2,"rssi":-29.3}
	//]
	//}
	//check out dump1090-fa websites/forums to find out more about these fields!
	type aircraft struct {
		Hex            string  `json:"hex"`
		Flight         string  `json:"flight"`
		Altbaro        int64   `json:"alt_baro"`
		Altgeom        int64   `json:"alt_geom"`
		Gs             float64 `json:"gs"`
		Ias            int     `json:"ias"`
		Tas            int     `json:"tas"`
		Mach           float64 `json:"mach"`
		Track          float64 `json:"track"`
		Trackrate      float64 `json:"track_rate"`
		Roll           float64 `json:"roll"`
		Magheading     float64 `json:"mag_heading"`
		Barorate       int64   `json:"baro_rate"`
		Geomrate       int64   `json:"geom_rate"`
		Squawk         string  `json:"squawk"`
		Emergency      string  `json:"emergency"`
		Category       string  `json:"category"`
		Navaltitudemcp int64   `json:"nav_altitude"`
		Navheading     float64 `json:"nav_heading"`
		Lat            float64 `json:"lat"`
		Lon            float64 `json:"lon"`
		Nic            int     `json:"nic"`
		Seenpos        float64 `json:"seen_pos"`
		Navqnh         float64 `json:"nav_qnh"`
		Version        int     `json:"version"`
		Siltype        string  `json:"sil_type"`
		Messages       int64   `json:"messages"`
		Rssi           float64 `json:"rssi"`
		Seen           float64 `json:"seen"`
		Nicbaro        int     `json:"nic_baro"`
		Nacp           int     `json:"nac_p"`
		Nacv           int     `json:"nac_v"`
		Gva            int     `json:"gva"`
		Sda            int     `json:"sda"`
		Sil            int     `json:"sil"`
		NavAltitudeFMS int64   `json:"nav_altitude_fms"`
	}

	type aircraftJSON struct {
		Now      float64    `json:"now"`
		Messages int64      `json:"messages"`
		Aircraft []aircraft `json:"aircraft"`
	}

	dump1090URL := flag.String("dump1090URL", "", "The URL of dump1090. e.g: -dump1090URL http://localhost:8080 (Required)")
	eFTLURL := flag.String("eFTLURL", "", "The host:port of the TIBCO eFTL server. e.g. -eFTLURL https://streaming.spotfire-cloud.com:443 (Required)")
	eFTLKey := flag.String("eFTLKey", "", "The key to authenticate against the TIBCO eFTL server. e.g. -eFTLKey bar (Required)")
	interval := flag.Duration("interval", 5, "How many seconds between checks. Default of 5 seconds. e.g. -interval 5")
	flag.Parse()

	//Check we have each of the command line arguments
	if *dump1090URL == "" || *eFTLURL == "" || *eFTLKey == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	opts := &eftl.Options{
		Password: *eFTLKey,
		ClientID: "FlightTracker",
	}

	errChan := make(chan error, 1)

	conn, err := eftl.Connect(*eFTLURL, opts, errChan)
	if err != nil {
		fmt.Println("TIBCO Cloud Messaging connect failed: ", err)
	} else {
		fmt.Println("Connected to : ", *eFTLURL)
	}

	// Disconnect from TIBCO Cloud Messaging when done
	defer conn.Disconnect()

	intervalinSeconds := *interval * time.Second
	var aircraftJSONResponse aircraftJSON
	aircraftURL := *dump1090URL + "/data/aircraft.json"
	for {
		response, err := http.Get(aircraftURL)
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {
			data, _ := ioutil.ReadAll(response.Body)
			decoder := json.NewDecoder(bytes.NewBuffer(data))

			if err := decoder.Decode(&aircraftJSONResponse); err != nil {
				if terr, ok := err.(*json.UnmarshalTypeError); ok {
					fmt.Printf("Failed to unmarshal field %s \n", terr.Field)
				} else {
					fmt.Println(err)
				}
			} else {
				//fmt.Println("Time Now: ", aircraftJSONResponse.Now)
				//fmt.Println("Messages: ", aircraftJSONResponse.Messages)
				//fmt.Println("Number of Aircraft: ", len(aircraftJSONResponse.Aircraft))
				for i := range aircraftJSONResponse.Aircraft {
					fmt.Println("Aircraft: ", aircraftJSONResponse.Aircraft[i].Hex)
					lastSeen := time.Duration(aircraftJSONResponse.Aircraft[i].Seen) * time.Second

					//if lat/lon is not set or = 0,0 then we're not interested - it can happen, but unlikely I'll receive a signal from there!
					//OR if lastSeen is more than 60 seconds
					if (aircraftJSONResponse.Aircraft[i].Lat != 0 && aircraftJSONResponse.Aircraft[i].Lon != 0) || lastSeen <= 60 {
						//fmt.Print("Raw Seen: ", aircraftJSONResponse.Aircraft[i].Seen, ": ")
						//fmt.Println("Last Seen: ", lastSeen)
						//Aircraft Category‘A1’ : ‘light’,‘A2’ : ‘medium’,‘A3’ : ‘medium’,‘A5’ : ‘heavy’,‘A7’ : ‘rotorcraft’
						err := conn.Publish(eftl.Message{
							"_dest":       "flightdata",
							"CurrentTime": aircraftJSONResponse.Now,
							"ICAO":        aircraftJSONResponse.Aircraft[i].Hex,
							"Flight":      aircraftJSONResponse.Aircraft[i].Flight,
							"Lattitude":   aircraftJSONResponse.Aircraft[i].Lat,
							"Longitude":   aircraftJSONResponse.Aircraft[i].Lon,
							"Speed":       aircraftJSONResponse.Aircraft[i].Gs,
							"Track":       aircraftJSONResponse.Aircraft[i].Track,
							"Category":    aircraftJSONResponse.Aircraft[i].Category,
							"Squawk":      aircraftJSONResponse.Aircraft[i].Squawk,
							//"LastSeen":    time.Now().Add(-lastSeen * time.Second),
							"SourceID": "davewins",
							"Region":   "U.K.",
						})
						if err != nil {
							log.Println("publish failed: ", err)
						}
					}
				}
			}
		}
		time.Sleep(intervalinSeconds)
	}
}
