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

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func unique(s []string) []string {
	var m = map[string]int{}
	for _, value := range s {
		m[value]++
	}
	for key := range m {
		if m[key] != 1 {
			delete(m, key)
		}
	}
	var uniqueSlice []string
	for key := range m {
		uniqueSlice = append(uniqueSlice, key)
	}
	return uniqueSlice
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
					var files = map[int]map[string]int{}
					for i, slave := range slaveSlice {
						itContains := false
						//command := "rep:" + splitworkingon[0] + ":"
						for _, filename := range slave.rchunks {
							if _, ok := fNameMap[filename]; ok {
								if !itContains {
									files[i] = map[string]int{}
								}
								files[i][filename] = 0 //command = command + "," + filename
								itContains = true
							}
						}
						if itContains {
							// send this command and reset response
							// slave.response = "nr"
							ind = append(ind, i)
							findingInReplica = true
						}
					}
					//load balancer
					//first schedule slaves having 1 chunk only
					//if 2 slaves have same 1 chunk, then it will schedule on 1st slave
					var ready_command = map[int]map[string]int{}
					var scheduled []string //all chunks which are scheduled
					again := true
					for again {
						again = false
						for i, m := range files {
							if len(m) == 1 {
								for key := range m {
									if !contains(scheduled, key) {
										if ready_command[i] == nil {
											ready_command[i] = map[string]int{}
										}
										ready_command[i][key] = 0
										scheduled = append(scheduled, key)
										again = true
										delete(files[i], key)
									}
								}
							}
						}
						// now find unique chunks
						//get all chunks in array (except already scheduled) and find unique
						var allchunks []string
						for _, m := range files {
							for key := range m {
								if !contains(scheduled, key) {
									allchunks = append(allchunks, key)
								}
							}
						}
						uniqueChunks := unique(allchunks)
						//now add these chunks into map ready_command
						for _, uniqueChunk := range uniqueChunks {
							//find index of slave which contain this chunk
							for i, m := range files {
								for key := range m {
									if key == uniqueChunk {
										//initialize inner map if not initialized
										if ready_command[i] == nil {
											ready_command[i] = map[string]int{}
										}
										ready_command[i][key] = 0
										scheduled = append(scheduled, key)
										again = true
										delete(files[i], key)
									}
								}
							}
						}
					}
					//now add remaining schedules
					//first make following map
					//{ chunk_0 : [1,2,3]} chunk0 is present in 1, 2, 3 slave
					chunkTracker := make(map[string][]int)
					for i, m := range files {
						for key := range m {
							if !contains(scheduled, key) {
								chunkTracker[key] = append(chunkTracker[key], i)
							}
						}
					}
					//now select each chuck and
					//check which slave number has less schedules in map ready_command
					for chunkName, lst := range chunkTracker {
						firstTime := true
						var SNWHLS int //SNWHLS == Slave number which has less schedules
						var NOS int    //Number of schedules
						//SNFL == slave number from list
						for _, SNFL := range lst {
							if firstTime {
								if ready_command[SNFL] == nil {
									SNWHLS = SNFL
									break
								}
								SNWHLS = SNFL
								NOS = len(ready_command[SNFL])
								firstTime = false
							} else {
								if NOS > len(ready_command[SNFL]) {
									NOS = len(ready_command[SNFL])
									SNWHLS = SNFL
								}
							}
						}
						//Now give search command to slave number == SNWHLS
						if ready_command[SNWHLS] == nil {
							ready_command[SNWHLS] = map[string]int{}
						}
						ready_command[SNWHLS][chunkName] = 0
						scheduled = append(scheduled, chunkName)
					}

					//now send commands
					for slavenumber, m := range ready_command {
						command := "rep:" + splitworkingon[0] + ":"
						if len(m) > 0 {
							slaveSlice[slavenumber].response = "nr"

							for key := range m {
								command = command + "," + key
							}
							slaveSlice[slavenumber].channel <- command
						}
					}
					// for _, i := range ind {
					// 	slaveSlice[i].response = "nr"
					// 	//todo send command
					// 	// slave.channel <- command
					// }
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

func main() {

	log.Print("To run client server on 3000 use cport=3000")
	log.Print("To run slave  server on 6000 use sport=9000")
	args := os.Args
	sport := "3000" //give default port to slave server
	cport := "9000" //give default port to client server
	for _, v := range args[1:] {
		r := strings.Split(v, "=")
		if r[0] == "cport" {
			cport = r[1]
		}
		if r[0] == "sport" {
			sport = r[1]
		}
	}

	//initialize maps array

	reqChan := make(chan string, 5)
	rreqChan := make(chan string, 5) //response of reqChan

	go handleNewSlaves(sport, reqChan, rreqChan)
	client.ClientServer(cport, reqChan, rreqChan)
}

// var m = map[int]map[string]int{}
// m[1] = map[string]int{}
// // m[0] = map[string]int{}
// m[1]["chunk_2"] = 0
// m[1]["chunk_3"] = 0
// m[2] = map[string]int{}
// m[2]["chunk_2"] = 0
// m[2]["chunk_3"] = 0
// m[2]["chunk_4"] = 0

// for i, m := range m {
// 	fmt.Println(i, m)
// }
// fmt.Println(len(m[2]))
// fmt.Println(m[0] == nil)
// var ss []string //{"1", "2","3","4","3","4" }
// ss = append(ss, "1")
// ss = append(ss, "2")
// ss = append(ss, "3")
// ss = append(ss, "4")
// ss = append(ss, "1")
// ss = append(ss, "2")
// ss = append(ss, "3")
// ss = append(ss, "7")

// fmt.Println(unique(ss))
