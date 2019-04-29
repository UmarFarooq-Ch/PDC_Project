package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	//"o«s"
)

//-------Utility functions-------
func RemoveIndex(s []Slave, index int) []Slave {
	return append(s[:index], s[index+1:]...)
}

//-------End of Util func--------

//Slave is something
type chunks []string
type rchunks []string
type Slave struct {
	connection net.Conn
	chunks     //original chunks
	rchunks    //replicated chunks
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
		rchunks:    make([]string, 0, rsize)}
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

	command := "rep:STE12592:chunk_13,chunk_14,chunk_15,chunk_16,chunk_17"
	for {
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
			// break
		} else if prsResp[0] == "done" {
			command = "nil"
			newMsg.msg = response
			msgchan <- newMsg
		} else if prsResp[0] == "nil" {
			command = "nil"
		}
		time.Sleep(1 * time.Second)
	}
	// buf := make([]byte, 4096)
	// for {
	// 	n, err := c.Read(buf)
	// 	if err != nil || n == 0 {
	// 		c.Close()
	// 		rmvChan <- newClient
	// 		break
	// 	}
	// msgchan <- newClient.nickname + string(buf[0:n])
	// ...
	// }
}

// Master slave thread
func handleSlaves(msgchan chan Msg, addchan chan Slave) {
	slaveSlice := make([]Slave, 0, 20)

	for {
		select {
		// case msg := <-msgchan:
		// 	log.Printf("new message: %s", msg)
		// 	for _, someClient := range slaveSlice {
		// 		someClient.connection.Write([]byte(msg))
		// 	}

		case newSlave := <-addchan:
			slaveSlice = append(slaveSlice, newSlave)
			log.Printf("New slave added...")

		case msg := <-msgchan:
			if msg.msg == "rmv" {
				log.Printf("Slave remove request received")
				for i, v := range slaveSlice {
					if v.connection == msg.connection {
						slaveSlice = RemoveIndex(slaveSlice, i)
						break
					}
				}
				log.Printf("Slave removed")
			} //else if msg.msg == "not"

		}
	}

}

//Register new slaves //main function for slave server
func handleNewSlaves(port string) {

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Slave Master running on port: " + port)
	addChan := make(chan Slave)
	msgChan := make(chan Msg)
	go handleSlaves(msgChan, addChan)

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

	handleNewSlaves(sport)
	cport = cport + "1"
	// go handleConnection(conn, msgchan, addchan)
	// }
}
