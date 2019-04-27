package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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

/*
*	get chunks names
*	send password to search
 */
func handleSlaveConnection(c net.Conn, msgchan chan<- string, addchan, rmvChan chan Slave) {

	log.Printf("Handling new slave connection...\n")
	clientReader := bufio.NewReader(c)
	//get number of files
	buff1, _, _ := clientReader.ReadLine() //get number of original files
	buff2, _, _ := clientReader.ReadLine() //get number of replicated files
	size, _ := strconv.Atoi(string(buff1))
	rsize, _ := strconv.Atoi(string(buff2))
	log.Printf("Size: %d\nRSize: %d\n", size, rsize)
	//create new slave
	newClient := Slave{connection: c,
		chunks:  make([]string, 0, size),
		rchunks: make([]string, 0, rsize)}
	//receive original file names from slave
	for i := 0; i < size; i++ {
		name, _, _ := clientReader.ReadLine() //get Name of chunks
		newClient.chunks = append(newClient.chunks, string(name))
		log.Printf("Chunk # %d , Name: %s \n", i, string(name))
	}
	//receive replicated files names from slave
	for i := 0; i < rsize; i++ {
		name, _, _ := clientReader.ReadLine() //get Name of chunks
		newClient.rchunks = append(newClient.rchunks, string(name))
		log.Printf("RChunk # %d , Name: %s \n", i, string(name))
	}
	log.Print(newClient)
	//sending new slave information to master thread
	addchan <- newClient

	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if err != nil || n == 0 {
			c.Close()
			rmvChan <- newClient
			break
		}
		// msgchan <- newClient.nickname + string(buf[0:n])
		// ...
	}
}

// Master slave thread
func handleSlaves(msgchan <-chan string, addchan, rmvChan chan Slave) {
	slaveSlice := make([]Slave, 0, 20)

	for {
		select {
		case msg := <-msgchan:
			log.Printf("new message: %s", msg)
			for _, someClient := range slaveSlice {
				someClient.connection.Write([]byte(msg))
			}

		case newSlave := <-addchan:
			slaveSlice = append(slaveSlice, newSlave)
			log.Printf("New slave added...")

		case rmvSlave := <-rmvChan:
			log.Printf("Slave remove request received")
			// log.Print(slaveSlice)
			for i, v := range slaveSlice {
				if v.connection == rmvSlave.connection {
					slaveSlice = RemoveIndex(slaveSlice, i)
					break
				}
			}
			// log.Print(slaveSlice)
			log.Printf("Slave removed")

		}
	}

}

/*
* Register new slaves
*
 */
func handleNewSlaves(port string) {

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Slave Master running on port: " + port)
	addChan := make(chan Slave)
	msgChan := make(chan string)
	rmvChan := make(chan Slave)
	go handleSlaves(msgChan, addChan, rmvChan)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleSlaveConnection(conn, msgChan, addChan, rmvChan)
	}
}
func main() {

	// ln, err := net.Listen("tcp", ":3000")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// // addchan := make(chan Client)
	// msgchan := make(chan string)
	// // go printMessages(msgchan, addchan)

	// for {
	// 	conn, err := ln.Accept()
	// 	if err != nil {
	// 		log.Println(err)
	// 		continue
	// 	}

	// [1] Port number to handle new slaves
	// [2] Port number to handle new clients
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

	// if args[1] == nil{
	// 	slavePort = 3000
	// }
	// slavePort = 3000 if args[1]
	// if args[2] == nil{
	// 	clientPort = 5000
	// }
	// slavePort := int(args[1])
	handleNewSlaves(sport)
	cport = cport + "1"
	// go handleConnection(conn, msgchan, addchan)
	// }
}
