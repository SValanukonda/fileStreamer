package filestreamer

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Server struct {
	addr         string
	handlerChans map[int]chan []byte
}

const outfile = "./out"
const parallelpage = 10
const chunk = 500 * 1024 * 1024 * 1024

func (s *Server) Listen() {

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic(fmt.Errorf("failed to listen to add : %s \n %w", s.addr, err))
	}
	go s.filewriter()
	for i := 0; i < parallelpage; i++ {
		conn, err := ln.Accept()
		if err != nil {
			panic(fmt.Errorf("unable to accept %d th connection \n %w", i, err))
		}
		s.handlerChans[i] = make(chan []byte)
		go s.fileRcvHandler(i, conn)
	}
}

func (s *Server) filewriter() {
	fmt.Println("starting file writer process ")
	file, err := os.Open(outfile)
	if err != nil {
		panic(fmt.Errorf("unable to open file for writing \n %w", err))
	}
	for {
		for i := 0; i < parallelpage; i++ {
			data := <-s.handlerChans[i]
			file.Write(data)
		}
	}
}

func (s *Server) fileRcvHandler(id int, conn net.Conn) {
	fmt.Printf("starting handler id : %d \n", id)
	chn := s.handlerChans[id]
	iscompleted := false
	conreader := bufio.NewReaderSize(conn, 2*chunk)
	for {
		data := make([]byte, chunk)
		_, err := io.ReadFull(conreader, data)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				iscompleted = true
			}
			panic(fmt.Errorf("uanble to read data from conn id : %d \n %w", id, err))
		}
		chn <- data
		if iscompleted {
			return
		}
	}
}
