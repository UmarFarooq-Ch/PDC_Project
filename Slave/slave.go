package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

//-----Util function------
func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

//------end of util-------
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
	namesOriginals := make([]string, 0, 0) //conatins names of original files
	namesReplicas := make([]string, 0, 0)  //contains names of replicated files
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
	// fmt.Fprintf(conn, strconv.Itoa(len(originalFiles))+"\n")
	conn.Write([]byte(strconv.Itoa(len(originalFiles)) + "\n"))
	conn.Write([]byte(strconv.Itoa(len(replicatedFiles)) + "\n"))
	//send file names
	for _, file := range originalFiles {
		conn.Write([]byte(file.Name() + "\n"))
		fmt.Println(file.Name())
		namesOriginals = append(namesOriginals, file.Name())
	}
	for _, file := range replicatedFiles {
		conn.Write([]byte(file.Name() + "\n"))
		fmt.Println(file.Name())
		namesReplicas = append(namesReplicas, file.Name())
	}

	msgChan := make(chan string) //contains msg from the master
	go search(msgChan, namesOriginals, namesReplicas)

	var msgtobesent string
	masterReader := bufio.NewReader(conn)

	//heartbeat
	for {
		//wait to get command from master
		command, _, _ := masterReader.ReadLine()
		// possible commands from master
		// org:pass "find 'pass' in original files"
		// rep:pass:filenames "find 'pass' in replicated files"
		// nil "no work, enjoy holiday"
		// stp "stop finding"
		msgChan <- string(command)

		// }
		//get response from searching thread
		msgtobesent = <-msgChan
		conn.Write([]byte(msgtobesent + "\n"))
	}
	//
	// go handleConnection(conn, msgchan, addchan)
	// }
}

func countLines(namesOriginals, namesReplicas []string) ([]int, []int) {
	var originalSize []int
	var replicaSize []int
	for _, v := range namesOriginals {
		re, err := os.Open("original/" + v)
		defer re.Close()
		if err != nil {
			log.Println("error while opening :"+v, err)
		}
		count, err := lineCounter(re)
		if err != nil {
			log.Println("error while counting :"+v, err)
		}
		originalSize = append(originalSize, count)
		re.Close()
	}

	for _, v := range namesReplicas {
		re, err := os.Open("replicated/" + v)
		defer re.Close()
		if err != nil {
			log.Println("error while opening :"+v, err)
		}
		count, err := lineCounter(re)
		if err != nil {
			log.Println("error while counting :"+v, err)
		}
		replicaSize = append(replicaSize, count)
		re.Close()
	}
	return originalSize, replicaSize
}
func search(msgChan chan string, namesOriginals, namesReplicas []string) {

	//get number of lines of each file
	originalSize, replicaSize := countLines(namesOriginals, namesReplicas)
	log.Print(originalSize, replicaSize)
	var msgtobesent string
	var fileIndex int
	var lineIndex int
	var r []string
	var wholefile []string
	for {
		// log.Print(lineIndex)
		select {
		case msg := <-msgChan:

			println("k: ", msgtobesent, msg, len(msgtobesent) != 0, msgtobesent != "not")
			if !(strings.Index(msgtobesent, ":") > -1) && msgtobesent != "not" {
				println("ms:", msgtobesent)
				r = strings.Split(string(msg), ":")
				println("r: ", r[0])
				switch r[0] {
				case "stp":
					msgtobesent = "nil"
				case "nil":
					msgtobesent = "nil"
				case "rep":
					msgtobesent = "wrk"
				case "org":
					msgtobesent = "wrk"
				}
			}
			println("l", msgtobesent)
			msgChan <- msgtobesent
			if msgtobesent == "not" || (len(msgtobesent) > 0 && string(msgtobesent[0]) == "d") {
				// log.Panic("in reset state")

				msgtobesent = "" //reset
			}
		default:
			if msgtobesent == "wrk" {
				//check for original or replica
				if r[0] == "org" {
					//open new file if previous file search complete
					if lineIndex == 0 {
						log.Print("Loading file " + namesOriginals[fileIndex] + " in memory")
						file, err := ioutil.ReadFile("original/" + namesOriginals[fileIndex])
						if err != nil {
							log.Fatal(err)
						} else {
							wholefile = strings.Split(string(file), "\n")
							log.Print("File " + namesOriginals[fileIndex] + " loaded")
							log.Print("Starting search in file: " + namesOriginals[fileIndex])
						}

					}
					//match password
					if wholefile[lineIndex] == r[1] {
						msgtobesent = "done:" + namesOriginals[fileIndex]
						println("found", namesOriginals[fileIndex], lineIndex)

					}
					lineIndex++
					if lineIndex == originalSize[fileIndex] {
						log.Print("File completed.", lineIndex)
						lineIndex = 0
						fileIndex++
					}

					if fileIndex == len(namesOriginals) {
						// fileIndex = 0
						// lineIndex = 0
						// if msgtobesent != "done" {
						msgtobesent = "not"
						// }
					}

				} else {
					print("hi")
				}
			} else {
				print("m: ", msgtobesent)
				if len(msgtobesent) == 0 {

					log.Panic("in reset state")
				}
				fileIndex = 0
				lineIndex = 0
			}
		}
	}
}
