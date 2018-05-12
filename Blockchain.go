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
	"crypto/sha256"
	"encoding/hex"
)

// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("blockchain")

// +++++++++ Global vars
var me = net.ParseIP(bchainlibs.LocalhostAddr)
var rootNode = "10.12.0.1"
var randomGen = rand.New(rand.NewSource(time.Now().UnixNano()))
var queryCount = 0
var queryStart = int64(0)

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
	log.Info("But first waiting for " + strconv.Itoa(duration) + "ms")
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
		//log.Info("---------------------------------------------------------------------------")
		//log.Info("---------------------------------------------------------------------------")
		log.Info("Node " + me.String() + " reporting for duty!!!")
		//log.Info("JSON.Marshal(packet) ==> ")
		//js, _ := json.Marshal(packet)
		//log.Info(string(js))
		//log.Info("---------------------------------------------------------------------------")
		//log.Info("---------------------------------------------------------------------------")
		toOutput(packet)
	}

}

func selectLeaderOfTheManet() {
	futureLeaders := [7]string{ "10.12.0.1", "10.12.0.3", "10.12.0.7", "10.12.0.9", "10.12.0.13", "10.12.0.17", "10.12.0.19"}

	// If I AM NEO ... send the first query
	if me.String() == rootNode {
		queryCount += 1
		time.Sleep(time.Second * 5)
		log.Info("The leader has been chosen!!! All hail the new KING!!! " + me.String())

		queryStart = time.Now().UnixNano()
		query := bchainlibs.CreateQuery(me)

		rand.Seed(time.Now().UnixNano())
		query.Query.Next = futureLeaders[ rand.Intn(7) ]

		toOutput(query)
		log.Debug("QUERY_START=" + strconv.FormatInt(queryStart, 10))
	}
}

func measureQueryCompletitionTime() {
	if me.String() == rootNode {
		queryEnd := time.Now().UnixNano() - queryStart
		log.Debug("QUERY_COMPLETE=" + strconv.FormatInt(queryEnd, 10))
	}
}

func continuity() {
	if queryCount < 5 {
		time.Sleep(time.Second * 3)
		log.Info("New query, count = " + strconv.Itoa(queryCount))
		selectLeaderOfTheManet()
	} else {
		log.Info("PLEASE_EXIT=1234")
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
				queryCount += 1
				log.Info("Packet with QueryType")
				query := *payload.Query
				queries[query.ID] = query
				rootNode = query.Next // Changing root node!
				resolveQuery(query)
				break

			case bchainlibs.BlockType:

				log.Info("Packet with BlockType with ")
				log.Info("PacketID: " + payload.ID)
				log.Info("BlockID: " + payload.Block.ID)
				log.Info("Previous Block ID: " + payload.Block.PreviousID)
				log.Info("Nonce: " + payload.Block.Nonce)
				//log.Info("MerkleTreeRoot " + payload.Block.MerkleTreeRoot)
				log.Info("---------------------------------------")

				// ------------------------------------------------------------------------
				// ------------------------------------------------------------------------
				// Checking the actual last block of the blockchain against the received one
				payloadIsValid := false

				if len(blockchain) > 0 {
					lastB := blockchain[len(blockchain)-1]

					if payload.Block.PreviousID == lastB.ID {
						payloadIsValid = true
						log.Info("payload.Block.PreviousID == lastB.ID (TRUE)")
					}
				} else if len(blockchain) == 0 {
					payloadIsValid = true
					log.Info("len(blockchain) == 0 (TRUE)")
				} else {
					log.Error("Stranger things are happening!")
				}

				// ------------------------------------------------------------------------
				// ------------------------------------------------------------------------
				// Checking "THE ID"
				concat := payload.Block.PreviousID
				concat += payload.Block.Nonce
				concat += strconv.FormatInt(payload.Block.Timestamp, 10)
				concat += bchainlibs.GetMerkleTreeRoot(transactions[payload.Block.QueryID])

				sum1 := sha256.Sum256([]byte( concat ))
				sum2 := sha256.Sum256(sum1[:])

				testID := hex.EncodeToString(sum2[:])

				if testID == payload.Block.ID {
					payloadIsValid = true
					log.Info("testID == payload.Block.ID (TRUE)")
				} else {
					payloadIsValid = false
					log.Error("testID == payload.Block.ID (FALSE)")
				}

				// ------------------------------------------------------------------------
				// ------------------------------------------------------------------------
				// Checking Merkle Tree Root
				//testMerkleTreeRoot := bchainlibs.GetMerkleTreeRoot(transactions[payload.Block.QueryID])
				//
				//if testMerkleTreeRoot == payload.Block.MerkleTreeRoot {
				//	payloadIsValid = true
				//	log.Info("testMerkleTreeRoot == payload.Block.MerkleTreeRoot (TRUE)")
				//} else {
				//	payloadIsValid = false
				//	log.Error("testMerkleTreeRoot == payload.Block.MerkleTreeRoot (FALSE)")
				//}

				// I could and I can add MORE validations

				if payloadIsValid {

					log.Info("Payload IS Valid")
					log.Debug("BLOCK_VALID_" + payload.Block.QueryID + "=1")
					blockchain = append(blockchain, *payload.Block)

					measureQueryCompletitionTime()

					copyPayload := payload
					copyPayload.Type = bchainlibs.LastBlockType
					toOutput(copyPayload) // SendLastBlock() basically

					log.Info("----- This is the blockchain")
					for index, element := range blockchain {
						log.Info(string(index) + " " + element.String())
					}
					log.Info("----- --------")

					go continuity()

					// This is basically to end the program BUT I need to find another way

					//	// After a validated block, just re run everything to get new data
					//	log.Debug("QUERY COMPLETED, the blockchain length is " + strconv.Itoa(len(blockchain)) )
					//	log.Info("QUERY_END=" + strconv.FormatInt(time.Now().UnixNano(), 10))
					//	log.Debug("PLEASE_EXIT=1234")
					//}

				} else {
					log.Info("Payload NOT Valid")
					log.Debug("BLOCK_INVALID_" + payload.Block.QueryID + "=1")
				}

				break

			case bchainlibs.TransactionType:
				log.Info("Packet with TransactionType, with PacketID: " + payload.ID)
				log.Info("JSON ALERT ----> ")
				log.Info(j)
				log.Info("--------------------")
				if nil != payload.Transaction {
					log.Info("Transaction IS NOT EMPTY ... MAYBE")
					if payload.Transaction.QueryID != "" {
						log.Info("It should be safe to say that Transaction is INDEED NOT EMPTY")
						queryId := payload.Transaction.QueryID
						transactions[queryId] = append(transactions[queryId], *payload.Transaction)
						query := queries[queryId]

						log.Info(" -- len(transactions[queryId]) = " + strconv.Itoa(len(transactions[queryId])))
						log.Info(" -- query.NumberLimit = " + strconv.Itoa(query.NumberLimit))

						if len(transactions[queryId]) >= query.NumberLimit {
							log.Info("Launching election by TRANSACTION NUMBER limit reached!!!")
							electionStart := bchainlibs.CreateLaunchElectionPacket(me, query)
							toOutput(electionStart)
						} else if time.Now().Unix() >= (query.Created + query.TimeLimit) {
							log.Info("Launching election by TIME limit reached!!!")
							electionStart := bchainlibs.CreateLaunchElectionPacket(me, query)
							toOutput(electionStart)
						} else {
							log.Info("Criteria has not been met for the elections!!!")
						}
					} else {
						log.Error("BOOM!!! It was empty after all!")
					}

				} else {
					log.Error("Transaction IS EMPTY")
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
	logName := "monitor"
	f := bchainlibs.PrepareLogGen(logPath, logName, "data")
	defer f.Close()
	f2 := bchainlibs.PrepareLog(logPath, logName)
	defer f2.Close()
	backend := logging.NewLogBackend(f, "", 0)
	backend2 := logging.NewLogBackend(f2, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, bchainlibs.LogFormat)

	backend2Formatter := logging.NewBackendFormatter(backend2, bchainlibs.LogFormatPimp)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.DEBUG, "")

	// Only errors and more severe messages should be sent to backend1
	backend2Leveled := logging.AddModuleLevel(backend2Formatter)
	backend2Leveled.SetLevel(logging.INFO, "")

	logging.SetBackend(backendLeveled, backend2Leveled)

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
