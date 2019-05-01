package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	client "github.com/UmarFarooq-Ch/PDC_Project/Client"
	"github.com/golang-collections/go-datastructures/queue"
	//"o«s"
)

//-------Utility functions-------
func RemoveIndex(s []Slave, index int) []Slave {
	return append(s[:index], s[index+1:]...)
}

func PopulateMap(m map[string]int, s []string) {
	for _, v := range s {
		if _, ok := m[v]; !ok {
			m[v] = 1
		}
	}
}

func UnPopulateMap(m map[string]int, s []string) {
	for _, v := range s {
		delete(m, v)
	}
}

func setResponse(res string, slaveSlice *[]Slave, msg Msg) {
	//set response of slave in slaveSlice[index].id
	index := -1
	for i, v := range *slaveSlice {
		if msg.connection == v.connection {
			index = i
			break
		}
	}
	(*slaveSlice)[index].response = res

}

//check all responses from slaves are received or not
func areResponsesReceived(slaveSlice []Slave) bool {
	for _, v := range slaveSlice {
		if v.response == "nr" {
			return false
		}
	}
	return true
}

//set responses to not received
func resetResponses(slaveSlice *[]Slave) {
	for i := 0; i < len(*slaveSlice); i++ {
		(*slaveSlice)[i].response = "nr"
	}

}

//-------End of Util func--------

//Slave is something
type chunks []string
type rchunks []string
type Slave struct {
	connection net.Conn
	chunks     //original chunks
	rchunks    //replicated chunks
	response   string
	channel    chan string
}
type Msg struct {
	connection net.Conn
	msg        string
}

/*
*	get chunks names
*	send password to search
 */
func handleSlaveConnection(c net.Conn, msgchan chan Msg, addchan chan Slave) {

	log.Printf("Handling new slave connection...\n")
	slaveReader := bufio.NewReader(c)
	//get number of files
	buff1, _, _ := slaveReader.ReadLine() //get number of original files
	buff2, _, _ := slaveReader.ReadLine() //get number of replicated files
	size, _ := strconv.Atoi(string(buff1))
	rsize, _ := strconv.Atoi(string(buff2))
	log.Printf("Size: %d\nRSize: %d\n", size, rsize)
	//create new slave
	newClient := Slave{
		connection: c,
		chunks:     make([]string, 0, size),
		rchunks:    make([]string, 0, rsize),
		channel:    make(chan string, 2),
		response:   "nr"}
	newMsg := Msg{
		connection: c}
	//receive original file names from slave
	for i := 0; i < size; i++ {
		name, _, _ := slaveReader.ReadLine() //get Name of chunks
		newClient.chunks = append(newClient.chunks, string(name))
		log.Printf("Chunk # %d , Name: %s \n", i, string(name))
	}
	//receive replicated files names from slave
	for i := 0; i < rsize; i++ {
		name, _, _ := slaveReader.ReadLine() //get Name of chunks
		newClient.rchunks = append(newClient.rchunks, string(name))
		log.Printf("RChunk # %d , Name: %s \n", i, string(name))
	}
	log.Print(newClient)
	//sending new slave information to master thread
	addchan <- newClient

	command := "nil"
	for {
		select {
		case x := <-newClient.channel:
			command = x
			fmt.Println("setting new command: " + command)
		default:

			c.Write([]byte(command + "\n"))

			cc, _, err := slaveReader.ReadLine()
			response := string(cc)
			//check for disconnectivity
			if response == "" || err != nil {
				log.Println("Slave disconnected...")
				newMsg.msg = "rmv"
				msgchan <- newMsg
				return
			}
			//Parse reponse
			prsResp := strings.Split(response, ":")
			log.Print(prsResp[0])
			//set response of not found
			if prsResp[0] == "not" {
				command = "nil"
				newMsg.msg = response
				msgchan <- newMsg
			} else if prsResp[0] == "done" {
				command = "nil"
				newMsg.msg = response
				msgchan <- newMsg
			} else if prsResp[0] == "nil" {
				command = "nil"
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// Master slave thread
func handleSlaves(msgchan chan Msg, addchan chan Slave, reqchan, rreqChan chan string) {
	slaveSlice := make([]Slave, 0, 20)
	fNameMap := make(map[string]int) //contain names of original files of slaves who disconnects
	Q := queue.New(100)              //conatins id:password max capacity is 100
	workingOn := ""                  //working on this password (id:password)
	findingInReplica := false
	for {
		select {

		case newSlave := <-addchan:
			//before adding remove files from fNameMap which this slave contains
			UnPopulateMap(fNameMap, newSlave.chunks)
			//register new slave
			slaveSlice = append(slaveSlice, newSlave)
			log.Printf("New slave added...")

		case msg := <-msgchan:
			if msg.msg == "rmv" {
				log.Printf("Slave remove request received")
				for i, v := range slaveSlice {
					if v.connection == msg.connection {
						//before removing save file of slave in fNameMap map
						PopulateMap(fNameMap, v.chunks)
						slaveSlice[i].response = "nr"
						slaveSlice = append(slaveSlice[:i], slaveSlice[i+1:]...)
						break
					}
				}
				log.Printf("Slave removed")
			} else if msg.msg[:3] == "not" {
				setResponse("not", &slaveSlice, msg)
				// fmt.Println("after not: ", slaveSlice, msg, workingOn, findingInReplica)
			} else if len(msg.msg) > 4 && msg.msg[:4] == "done" {
				setResponse(msg.msg, &slaveSlice, msg)
				// fmt.Println(slaveSlice, msg)

				//stop others
				for _, v := range slaveSlice {
					if v.response == "nr" {
						v.channel <- "stp"
					}
				}
				resetResponses(&slaveSlice)
				findingInReplica = false
				// println("sending ###: ", msg.msg)
				rreqChan <- (msg.msg[5:])
				workingOn = ""
			}

		case req := <-reqchan:
			Q.Put(req)

		default:
			//give command to work for first time
			if workingOn == "" && !Q.Empty() {
				v, _ := Q.Get(1)
				workingOn = fmt.Sprint(v[0])
				println("workingon: ", workingOn)
				slc := strings.Split(workingOn, ":")
				//send command to all slaves
				resetResponses(&slaveSlice)
				for _, v := range slaveSlice {
					v.channel <- "org:" + slc[0]
				}
			} else if areResponsesReceived(slaveSlice) && !findingInReplica {
				//if all responses received and
				//any slave disconnect between execution then
				//locate its files and send search command to them

				splitworkingon := strings.Split(workingOn, ":")
				if len(fNameMap) != 0 && workingOn != "" {
					findingInReplica = true
					var ind []int
					for i, slave := range slaveSlice {
						itContains := false
						command := "rep:" + splitworkingon[0] + ":"
						for _, filename := range slave.rchunks {
							if _, ok := fNameMap[filename]; ok {
								command = command + "," + filename
								itContains = true
							}
						}
						if itContains {
							//send this command and reset response
							slave.response = "nr"
							ind = append(ind, i)
							findingInReplica = true
							// fmt.Println("slave: ", slaveSlice)
							slave.channel <- command
						}
					}
					for _, i := range ind {
						slaveSlice[i].response = "nr"
					}
				} else if workingOn != "" {
					resetResponses(&slaveSlice)
					findingInReplica = false
					rreqChan <- ("not")
					workingOn = ""
				}

				// fmt.Println("bahir nikle ke baad: ", slaveSlice)

			} else if areResponsesReceived(slaveSlice) && findingInReplica {
				// if workingOn != "" {
				resetResponses(&slaveSlice)
				findingInReplica = false
				rreqChan <- ("not")
				workingOn = ""
			}

		}
	}

}

//Register new slaves //main function for slave server
func handleNewSlaves(port string, reqChan, rreqChan chan string) {

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Slave Master running on port: " + port)
	addChan := make(chan Slave)
	msgChan := make(chan Msg)
	go handleSlaves(msgChan, addChan, reqChan, rreqChan)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleSlaveConnection(conn, msgChan, addChan)
	}
}
func handleNewClients(port string, reqChan, rreqChan chan string) {

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Cleint Server running on port: " + port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleClientConnection(conn, reqChan, rreqChan)
	}
}

func handleClientConnection(c net.Conn, reqChan, rreqChan chan string) {
	log.Printf("Handling new client connection...\n")
	slaveReader := bufio.NewReader(c)

	for {

		c.Write([]byte("Enter password to search: "))
		buff, _, _ := slaveReader.ReadLine()
		reqChan <- string(buff) + ":1"
		resp := <-rreqChan
		c.Write([]byte(resp))
	}

	// c.Write([]byte(command + "\n"))

}

func main() {
	log.Print("To run client server on 3000 use cport=3000")
	log.Print("To run slave  server on 6000 use sport=6000")
	args := os.Args
	var sport string = "3000" //give default port to slave server
	var cport string = "6000" //give default port to client server
	for _, v := range args[1:] {
		r := strings.Split(v, "=")
		if r[0] == "cport" {
			cport = r[1]
		}
		if r[0] == "sport" {
			sport = r[1]
		}
	}

	reqChan := make(chan string, 5)
	rreqChan := make(chan string, 5) //response of reqChan

	go handleNewSlaves(sport, reqChan, rreqChan)
	client.ClientServer(cport, reqChan, rreqChan)
	//handleNewClients(cport, reqChan, rreqChan)

	cport = cport + "1"
	// go handleConnection(conn, msgchan, addchan)
	// }
}
