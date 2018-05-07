package main

import (
	"github.com/op/go-logging"
	"github.com/chepeftw/bchainlibs"
	"net"
	"os"
	"encoding/json"
	"time"
	"strconv"
	"math/rand"
)

// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("blockchain")

// +++++++++ Global vars
var me = net.ParseIP(bchainlibs.LocalhostAddr)
var rootNode = "10.12.0.1"
var randomGen = rand.New(rand.NewSource(time.Now().UnixNano()))

// +++++++++ Channels
var input = make(chan string)
var output = make(chan string)
var done = make(chan bool)

var blockchain []bchainlibs.Block
var queries = make(map[string]bchainlibs.Query)
var transactions = make(map[string][]bchainlibs.Transaction)

func toOutput(payload bchainlibs.Packet) {
	log.Debug("Sending Packet with ID " + payload.ID + " to channel output")
	bchainlibs.SendGeneric(output, payload, log)
}

func attendOutputChannel() {
	log.Debug("Starting output channel")
	bchainlibs.SendToNetwork(me.String(), bchainlibs.RouterPort, output, false, log, me)
}

// ------------------------------------------------------------------------------
// Generating the Query with the function selectLeaderOfTheManet
// And answering the query with the function resolveQuery
// ------------------------------------------------------------------------------

func resolveQuery(query bchainlibs.Query) {
	log.Info("Resolving GraphQL")

	duration := randomGen.Intn(100000) / 500
	log.Debug("But first waiting for " + strconv.Itoa(duration) + "ms")
	time.Sleep(time.Millisecond * time.Duration(duration))

	node1 := "10.12.0.5"
	node2 := "10.12.0.10"
	node3 := "10.12.0.15"
	node4 := "10.12.0.20"

	// Parse query
	// Am I eligible to reply?
	// 	Collect data
	// 	SendData -> val := true

	packet := bchainlibs.CreateTransaction(me)
	sendIt := false

	packet.Transaction.QueryID = query.ID

	switch me.String() {
	case node1:
		packet.Transaction.Order = 1
		packet.Transaction.ActualHop = net.ParseIP(node1)
		packet.Transaction.PreviousHop = net.ParseIP(bchainlibs.NullhostAddr)
		sendIt = true
	case node2:
		packet.Transaction.Order = 2
		packet.Transaction.ActualHop = net.ParseIP(node2)
		packet.Transaction.PreviousHop = net.ParseIP(node1)
		sendIt = true
	case node3:
		packet.Transaction.Order = 3
		packet.Transaction.ActualHop = net.ParseIP(node3)
		packet.Transaction.PreviousHop = net.ParseIP(node2)
		sendIt = true
	case node4:
		packet.Transaction.Order = 4
		packet.Transaction.ActualHop = net.ParseIP(node4)
		packet.Transaction.PreviousHop = net.ParseIP(node3)
		sendIt = true
	}

	if sendIt {
		log.Info("Node " + me.String() + " reporting for duty!!!")
		toOutput(packet)
	}

}

func selectLeaderOfTheManet() {
	// If I AM NEO ... send the first query
	if me.String() == rootNode {
		log.Info("The leader has been chosen!!! All hail the new KING!!! " + me.String())
		time.Sleep(time.Second * 2)

		query := bchainlibs.CreateQuery(me)
		toOutput(query)
		log.Debug("QUERY_START=" + strconv.FormatInt(time.Now().UnixNano(), 10))
	}
}

// ------------------------------------------------------------------------------
// Function that handles the buffer channel
// ------------------------------------------------------------------------------

func attendInputChannel() {
	log.Info("Starting input channel")
	for {
		j, more := <-input
		if more {
			// First we take the json, unmarshal it to an object
			payload := bchainlibs.Packet{}
			json.Unmarshal([]byte(j), &payload)

			//source := payload.Source
			id := payload.ID

			//log.Debug("Incoming payload with ID = " + id)

			switch payload.Type {

			case bchainlibs.QueryType:
				log.Info("Packet with QueryType")
				query := *payload.Query
				queries[query.ID] = query
				resolveQuery(query)
				break

			case bchainlibs.BlockType:
				log.Info("Packet with BlockType, with PacketID: " + payload.ID + ", BlockID: " + payload.Block.ID)
				log.Info("Previous Block ID: " + payload.Block.PreviousID + ", Nonce: " + payload.Block.Nonce + ", MerkleTreeRoot " + payload.Block.MerkleTreeRoot)

				// Checking the actual last block of the blockchain against the received one
				// The bigger block should be the new one
				payloadIsValid := false

				if len(blockchain) > 0 {
					lastB := blockchain[len(blockchain)-1]

					// The check for the created time needs some improvements.
					//if payload.PrID == lastB.BID && payload.Block.Created > lastB.Block.Created {
					if payload.Block.PreviousID == lastB.ID {
						payloadIsValid = true
					}
				} else if len(blockchain) == 0 {
					payloadIsValid = true
				} else {
					log.Debug("Stranger things are happening!")
				}

				// I could and I can add MORE validations

				if payloadIsValid {

					log.Debug("Payload IS Valid")
					blockchain = append(blockchain, *payload.Block)

					copyPayload := payload
					copyPayload.Type = bchainlibs.LastBlockType
					toOutput(copyPayload) // SendLastBlock() basically

					log.Debug("----- This is the blockchain")
					for index, element := range blockchain {
						log.Debug(string(index) + " " + element.String())
					}
					log.Debug("----- --------")

					// This is basically to end the program BUT I need to find another way

					// IF I'm the query generator, does this solves my query?
					// checkQueryCompleteness()?
					//if payload.Block.ActualHop.String() == payload.Block.Destination.String() {
					//	// After a validated block, just re run everything to get new data
					//	log.Debug("QUERY COMPLETED, the blockchain length is " + strconv.Itoa(len(blockchain)) )
					//	log.Info("QUERY_END=" + strconv.FormatInt(time.Now().UnixNano(), 10))
					//	log.Debug("PLEASE_EXIT=1234")
					//}

				} else {
					log.Debug("Payload NOT Valid")
				}

				break

			case bchainlibs.TransactionType:
				log.Info("Packet with TransactionType, with PacketID: " + payload.ID)
				queryId := payload.Transaction.QueryID
				transactions[queryId] = append(transactions[queryId], *payload.Transaction)
				query := queries[queryId]

				if len(transactions[queryId]) >= query.NumberLimit {
					log.Info("Launching election by TRANSACTION NUMBER limit reached!!!")
					electionStart := bchainlibs.CreateLaunchElectionPacket(me, query, transactions[queryId])
					toOutput(electionStart)
				} else if time.Now().Unix() >= (query.Created + query.TimeLimit)  {
					log.Info("Launching election by TIME limit reached!!!")
					electionStart := bchainlibs.CreateLaunchElectionPacket(me, query, transactions[queryId])
					toOutput(electionStart)
				} else {
					log.Info("Criteria has not been met for the elections!!!")
				}

				break

			case bchainlibs.InternalPing:
				log.Info("Receiving PING from router with ID = " + id)
				payload := bchainlibs.BuildPong(me)
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

func main() {

	confPath := "/app/conf.yml"
	if len(os.Args[1:]) >= 1 {
		confPath = os.Args[1]
	}
	var c bchainlibs.Conf
	c.GetConf(confPath)

	targetSync := c.TargetSync
	logPath := c.LogPath
	rootNode = c.RootNode

	// Logger configuration
	f := bchainlibs.PrepareLog(logPath, "monitor")
	defer f.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")
	logging.SetBackend(backendLeveled)

	log.Info("")
	log.Info("logPath = " + logPath)
	log.Info("rootNode = " + rootNode)
	log.Info("")
	log.Info("------------------------------------------------------------------------")
	log.Info("")
	log.Info("Starting Blockchain process, waiting some time to get my own IP...")

	// Wait for sync
	bchainlibs.WaitForSync(targetSync, log)

	// But first let me take a selfie, in a Go lang program is getting my own IP
	me = bchainlibs.SelfieIP()
	log.Info("Good to go, my ip is " + me.String())

	// Lets prepare a address at any address at port 10000
	ServerAddr, err := net.ResolveUDPAddr(bchainlibs.Protocol, bchainlibs.BlockchainPort)
	bchainlibs.CheckError(err, log)

	// Now listen at selected port
	ServerConn, err := net.ListenUDP(bchainlibs.Protocol, ServerAddr)
	bchainlibs.CheckError(err, log)
	defer ServerConn.Close()

	// Run the Input!
	go attendInputChannel()
	// Run the Output channel! The direct messages to the router layer
	go attendOutputChannel()

	// Run the election of the leader!
	go selectLeaderOfTheManet()

	buf := make([]byte, 1024)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		input <- string(buf[0:n])
		bchainlibs.CheckError(err, log)
	}

	close(input)
	close(output)

	<-done
}
