package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	log.Print("To connect with slave server running on ip 192.168.1.10 and port 3000 use arguments: cport=3000 and ip=192.168.1.10")
	args := os.Args
	var sport string = "3000" //give default port to slave server
	var ip string = "localhost"
	for _, v := range args[1:] {
		r := strings.Split(v, "=")
		if r[0] == "sport" {
			sport = r[1]
		}
		if r[0] == "ip" {
			ip = r[1]
		}
	}
	//get files
	originalFiles, err1 := ioutil.ReadDir("original/.")
	replicatedFiles, err2 := ioutil.ReadDir("replicated/.")

	if err1 != nil {
		log.Fatal(err1)
	}
	if err2 != nil {
		log.Fatal(err2)
	}
	log.Print("Count of original files: ", len(originalFiles))
	log.Print("Count of replicated files: ", len(replicatedFiles))

	//establish connection
	conn, err := net.Dial("tcp", ip+":"+sport)
	if err != nil {
		log.Println(err)
	}
	//send counts of files
	log.Print(strconv.Itoa(len(originalFiles)))
	// fmt.Fprintf(conn, strconv.Itoa(len(originalFiles))+"\n")
	conn.Write([]byte(strconv.Itoa(len(originalFiles)) + "\n"))
	conn.Write([]byte(strconv.Itoa(len(replicatedFiles)) + "\n"))
	for _, file := range originalFiles {
		conn.Write([]byte(file.Name() + "\n"))
		fmt.Println(file.Name())
	}
	for _, file := range replicatedFiles {
		conn.Write([]byte(file.Name() + "\n"))
		fmt.Println(file.Name())
	}

	//
	// go handleConnection(conn, msgchan, addchan)
	// }
}
