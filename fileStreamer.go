package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

type Server struct {
	addr         string
	handlerChans map[int]chan []byte
}

const infile = "./in.txt"
const outfile = "./out.txt"
const parallelpage = 10
const chunk = 5

func (s *Server) Listen() {
	fmt.Println("started listening ")
	s.handlerChans = make(map[int]chan []byte)
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic(fmt.Errorf("failed to listen to add : %s \n %w", s.addr, err))
	}

	for i := 0; i < parallelpage; i++ {
		conn, err := ln.Accept()
		if err != nil {
			panic(fmt.Errorf("unable to accept %d th connection \n %w", i, err))
		}
		chn := make(chan []byte)
		s.handlerChans[i] = chn
		go s.fileRcvHandler(i, conn, chn)
	}
	s.filewriter()
}

func (s *Server) filewriter() {
	fmt.Println("starting file writer process ")
	file, err := os.Create(outfile)
	if err != nil {
		panic(fmt.Errorf("unable to open file for writing \n %w", err))
	}

	for {
		for i := 0; i < parallelpage; i++ {
			data, ok := <-s.handlerChans[i]
			if !ok {
				fmt.Printf("channel got closed for channel : %d ;; %s\n", i, string(data))
				return

			}
			//fmt.Println(i, data, string(data))
			_, err := file.Write(data)
			if err != nil {
				panic(fmt.Errorf("unable to write into a file \n %w", err))
			}

		}
	}
}

func (s *Server) fileRcvHandler(id int, conn net.Conn, chn chan []byte) {
	fmt.Printf("starting fileRcvHandler id : %d \n", id)
	iscompleted := false
	conreader := bufio.NewReaderSize(conn, 2*chunk)
	for {
		data := make([]byte, chunk)
		n, err := io.ReadFull(conreader, data)
		//fmt.Printf("data received in file rcv handler %d: %s \n", id, string(data))
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				iscompleted = true
			} else if err == io.EOF {
				fmt.Printf("got connection close for fileRcvHandler id : %d \n", id)
				close(chn)
				return
			} else {
				panic(fmt.Errorf("uanble to read data from conn id : %d \n %w", id, err))
			}
		}
		chn <- data[:n]
		if iscompleted {
			return
		}
	}
}

func (s *Server) send() {
	s.handlerChans = make(map[int]chan []byte)
	for i := 0; i < parallelpage; i++ {
		conn, err := net.Dial("tcp", s.addr)
		if err != nil {
			panic(fmt.Errorf(" unable to create connection id : %d \n %w", i, err))
		}
		chn := make(chan []byte)
		s.handlerChans[i] = chn
		go s.sendHandler(i, conn, chn)
	}

	file, _ := os.Open(infile)
	filereader := bufio.NewReaderSize(file, 2*chunk)
	readcompleted := false
	chanclosed := make(map[int]bool)
	closedchans := 0
	for i := 0; i < parallelpage; i++ {
		chanclosed[i] = false
	}
	for {
		for i := 0; i < parallelpage; i++ {
			if readcompleted == true && chanclosed[i] == false {
				closedchans += 1
				close(s.handlerChans[i])
				chanclosed[i] = true
				continue
			}
			data := make([]byte, chunk)
			n, err := io.ReadFull(filereader, data)
			//fmt.Println(i, data, string(data))
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					readcompleted = true
				}
			}
			s.handlerChans[i] <- data[:n]

		}
		if closedchans == parallelpage {
			time.Sleep(time.Minute)
			return
		}
	}
}

func (s *Server) sendHandler(id int, con net.Conn, chn chan []byte) {
	fmt.Printf("starting sendHanlder id : %d \n", id)
	for {
		data := <-chn
		//fmt.Printf("data about to be sent %s \n ", string(data))
		con.Write(data)
	}
}

func main() {
	transferType := os.Args[1]
	addr := os.Args[2]
	if transferType == "-send" {
		s := &Server{addr: addr}
		s.send()
	} else if transferType == "-listen" {
		s := &Server{addr: addr}
		s.Listen()
	}

}
