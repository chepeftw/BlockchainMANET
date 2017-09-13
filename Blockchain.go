package main

import (
	"github.com/op/go-logging"
	"net"
	"github.com/chepeftw/bchainlibs"
	"os"
	"github.com/chepeftw/treesiplibs"
	"encoding/json"
	"time"
	"strconv"
)

// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("blockchain")

// +++++++++ Global vars
var me net.IP = net.ParseIP(bchainlibs.LocalhostAddr)
var cryptoPiece = "00"
var rootNode = "10.12.0.1"

// +++++++++ Channels
// For the Miner the Input and Output will be to Router
var input = make(chan string)
var output = make(chan string)
var done = make(chan bool)

var blockchain []bchainlibs.Packet
var queries map[string]bchainlibs.Packet = make(map[string]bchainlibs.Packet)

func toOutput(payload bchainlibs.Packet) {
	log.Debug("Sending Packet with TID " + payload.TID + " to channel output")
	bchainlibs.SendGeneric( output, payload, log )
}

func attendOutputChannel() {
	log.Debug("Starting output channel")
	bchainlibs.SendToNetwork( me.String(), bchainlibs.RouterPort, output, false, log, me)
}

func resolveQuery() {
	log.Info("Resolving GraphQL")

	// Parse query
	// Am I eligible to reply?
	// 	Collect data
	// 	SendData -> val := true

	// For the moment I'm testing what if one packet is generated.
	val1 := me.String() == "10.12.0.10"
	//val2 := me.String() == "10.12.0.15"

	if val1 {
		packet := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
		toOutput(packet)
	}
	//else if val2 {
	//	packet := bchainlibs.AssembleUnverifiedBlock(me, "data2", "function2")
	//	toOutput(packet)
	//}
}


// Function that handles the buffer channel
func attendInputChannel() {
	log.Debug("Starting input channel")
	for {
		j, more := <-input
		if more {
			// First we take the json, unmarshal it to an object
			payload := bchainlibs.Packet{}
			json.Unmarshal([]byte(j), &payload)

			//source := payload.Source
			tid := payload.TID

			//log.Debug("Incoming payload with TID = " + tid)

			switch payload.Type {

			case bchainlibs.QueryType:
				log.Debug("Packet with QueryType")
				queries[tid] = payload
				resolveQuery()
			break

			case bchainlibs.VBlockType:
				log.Debug("Packet with VBlockType")
				if payload.IsValid(cryptoPiece) {

					// Add the validation against the actual last block FROM the blockchain
					// Check the timestamp
					// Check if timestamp then check the following blocks, the bigger chain "should" remain

					log.Debug("Payload IS Valid")
					blockchain = append( blockchain, payload )

					copyPayload := payload.Duplicate()
					copyPayload.Type = bchainlibs.LastBlockType
					toOutput(copyPayload) // SendLastBlock() basically

					log.Debug("----- This is the blockchain")
					for index, element := range blockchain {
						log.Debug( string(index) + " "+ element.String() )
					}
					log.Debug("----- --------")

					// IF I'm the query generator, does this solves my query?
					// checkQueryCompleteness()?

					// After a validated block, just re run everything to get new data
					log.Debug("PLEASE_EXIT=1234")

				} else {
					log.Debug("Payload NOT Valid")

					// Then we re-add it to the unverified stuff maybe?

				}
			break

			case bchainlibs.InternalPing:
				log.Info("Receiving PING from router with TID = " + tid)
				payload := bchainlibs.AssemblePong(me)
				toOutput(payload)
			break

			}

		} else {
			log.Debug("closing channel")
			done <- true
			return
		}

	}
}

func selectLeaderOfTheManet() {

	// If I AM NEO ... send the first query
	if me.String() == rootNode {

		log.Info("The leader has been chosen!!! All hail the new KING!!! " + me.String())
		time.Sleep(time.Second * 5)

		query := bchainlibs.AssembleQuery(me, "function")
		toOutput(query)
		log.Info("QUERY_START=" + strconv.FormatInt(time.Now().UnixNano(), 10))

		// and QUERY END????
	}
}

func main() {

	confPath := "/app/conf.yml"
	if len(os.Args[1:]) >= 1 {
		confPath = os.Args[1]
	}
	var c bchainlibs.Conf
	c.GetConf( confPath )

	targetSync := c.TargetSync
	logPath := c.LogPath
	cryptoPiece = c.CryptoPiece
	rootNode = c.RootNode

	// Logger configuration
	f := bchainlibs.PrepareLog( logPath, "monitor" )
	defer f.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")
	logging.SetBackend( backendLeveled )

	log.Info("")
	log.Info("------------------------------------------------------------------------")
	log.Info("")
	log.Info("Starting Blockchain process, waiting some time to get my own IP...")

	// Wait for sync
	bchainlibs.WaitForSync( targetSync, log )

	// But first let me take a selfie, in a Go lang program is getting my own IP
	me = treesiplibs.SelfieIP()
	log.Info("Good to go, my ip is " + me.String())

	// Lets prepare a address at any address at port 10000
	ServerAddr,err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.BlockCPort)
	treesiplibs.CheckError(err, log)

	// Now listen at selected port
	ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
	treesiplibs.CheckError(err, log)
	defer ServerConn.Close()

	// Run the Input!
	go attendInputChannel()
	// Run the Output channel! The direct messages to the router layer
	go attendOutputChannel()

	// Run the election of the leader!
	go selectLeaderOfTheManet()

	buf := make([]byte, 1024)

	for {
		n,_,err := ServerConn.ReadFromUDP(buf)
		input <- string(buf[0:n])
		treesiplibs.CheckError(err, log)
	}

	close(input)
	close(output)

	<-done
}