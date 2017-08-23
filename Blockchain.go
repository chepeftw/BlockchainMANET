package main

import (
	"github.com/op/go-logging"
	"net"
	"github.com/chepeftw/bchainlibs"
	"os"
	"github.com/chepeftw/treesiplibs"
	"encoding/json"
)

// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("blockchain")

// +++++++++ Global vars
var me net.IP = net.ParseIP(bchainlibs.LocalhostAddr)

// +++++++++ Channels
// For the Miner the Input and Output will be to Router
var input = make(chan string)
var output = make(chan string)
var done = make(chan bool)

var blockchain []bchainlibs.Packet
var queries map[string]bchainlibs.Packet = make(map[string]bchainlibs.Packet)

func toOutput(payload bchainlibs.Packet) {
	bchainlibs.SendGeneric( output, payload, log )
}

func attendOutputChannel() {
	bchainlibs.SendToNetwork( me.String(), bchainlibs.RouterPort, output, false, log, me)
}

func resolveQuery() {
	log.Info("Resolving GraphQL")

	// Parse query
	// Am I eligible to reply?
	// 	Collect data
	// 	SendData -> val := true

	val := false

	if val {
		packet := bchainlibs.AssembleUnverifiedBlock(me, "data", "function")
		toOutput(packet)
	}
}


// Function that handles the buffer channel
func attendInputChannel() {

	for {
		j, more := <-input
		if more {
			// First we take the json, unmarshal it to an object
			payload := bchainlibs.Packet{}
			json.Unmarshal([]byte(j), &payload)

			//source := payload.Source
			tid := payload.TID

			switch payload.Type {

			case bchainlibs.QueryType:
				queries[tid] = payload
				resolveQuery()
			break

			case bchainlibs.VBlockType:
				if payload.IsValid() {
					blockchain = append( blockchain, payload )

					copy := payload.Duplicate()
					copy.Type = bchainlibs.LastBlockType
					toOutput(copy) // SendLastBlock() basically
				}
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
	c.GetConf( confPath )

	targetSync := c.TargetSync


	// Logger configuration
	f := bchainlibs.PrepareLog( "blockchain" )
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