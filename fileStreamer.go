package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

const infile = "./in.txt"
const outfile = "./out.txt"
const parallelpage = 1
const chunk = 500 * 1024 * 1024

type TcpServer struct {
	addr         string
	datachannels map[int]*dataChannel
	synContext   *synContext
}

type dataChannel struct {
	fromaddr string
	isclosed bool
	channel  chan []byte
}

type synContext struct {
	wg    sync.WaitGroup
	mutex sync.Mutex
}

func (s *TcpServer) ListenForTransfer() error {
	fmt.Println("listening for file transfer ")
	ln, err := net.Listen("tcp", s.addr)
	s.synContext = &synContext{}
	s.datachannels = make(map[int]*dataChannel)
	if err != nil {
		return fmt.Errorf("unable to create listner : %w", err)
	}
	for i := 0; i < parallelpage; i++ {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept connnection id :%d \n %w", i, err)
		}
		s.synContext.wg.Add(1)
		s.datachannels[i] = &dataChannel{
			fromaddr: conn.RemoteAddr().String(),
			isclosed: false,
			channel:  make(chan []byte),
		}
		go s.recvHandler(i, conn, s.datachannels[i])
	}
	errf := s.fileWriter()
	fmt.Printf("awaiting ")
	s.synContext.wg.Wait()
	return errf
}

func (s *TcpServer) recvHandler(id int, conn net.Conn, dataChnl *dataChannel) {
	fmt.Printf("starting recvHanlder id : %d\n", id)
	defer s.synContext.wg.Done()
	buffConn := bufio.NewReaderSize(conn, 2*chunk)
	for {
		if dataChnl.isclosed {
			fmt.Printf("channel closed for recv handler id : %d \n", id)
			return
		}
		data := make([]byte, chunk)
		n, err := io.ReadFull(buffConn, data)
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF {
				dataChnl.isclosed = true
				close(dataChnl.channel)
				fmt.Printf("channel closed for id : %d \n", id)
				if err == io.EOF {
					return
				}
			} else {
				panic(fmt.Errorf("unable to read data from buffered connection for id : %d \n %w", id, err))
			}
		}
		dataChnl.channel <- data[:n]
		if err == io.ErrUnexpectedEOF {
			dataChnl.isclosed = true

		}

	}

}

func (s *TcpServer) fileWriter() error {
	fmt.Println("starting file writer process ")
	file, err := os.Create(outfile)
	if err != nil {
		return fmt.Errorf("unable to open file for writing \n %w", err)
	}
	for {
		openchans := parallelpage
		for i := 0; i < parallelpage; i++ {
			if s.datachannels[i].isclosed {
				openchans -= 1
			} else {
				d := <-s.datachannels[i].channel
				file.Write(d)
			}
		}
		if openchans == 0 {
			return nil
		}
	}

}

func (s *TcpServer) sendForTransfer() error {
	fmt.Println("starting send for transfer")
	s.synContext = &synContext{}
	s.datachannels = make(map[int]*dataChannel)
	for i := 0; i < parallelpage; i++ {
		conn, err := net.Dial("tcp", s.addr)
		if err != nil {
			return fmt.Errorf("unable to create a connection id :%d \n %w", i, err)
		}
		s.synContext.wg.Add(1)
		s.datachannels[i] = &dataChannel{
			fromaddr: conn.RemoteAddr().String(),
			isclosed: false,
			channel:  make(chan []byte),
		}
		go s.sendHandler(i, conn, s.datachannels[i])
	}
	err := s.sendfile()
	s.synContext.wg.Wait()
	return err
}

func (s *TcpServer) sendHandler(id int, conn net.Conn, dataChan *dataChannel) {
	defer s.synContext.wg.Done()
	fmt.Printf("starting send Handler id: %d \n", id)
	for data := range dataChan.channel {
		conn.Write(data)
	}
	fmt.Printf("got channel close for id:%d \n", id)
}

func (s *TcpServer) sendfile() error {
	fmt.Println("starting file send ")
	file, _ := os.Open(infile)
	filereader := bufio.NewReaderSize(file, 2*chunk)
	for {
		isreadcompleted := false
		for i := 0; i < parallelpage; i++ {
			data := make([]byte, chunk)
			n, err := io.ReadFull(filereader, data)
			if err != nil {
				if err == io.EOF {
					isreadcompleted = true
					break
				} else {
					if err == io.ErrUnexpectedEOF {
						isreadcompleted = true
					} else {
						return fmt.Errorf("unable to read data from file %w", err)
					}
				}
			}
			s.datachannels[i].channel <- data[:n]
		}
		if isreadcompleted == true {
			for _, val := range s.datachannels {
				val.isclosed = true
				close(val.channel)
			}
			return nil
		}
	}
}

func main() {
	transferType := os.Args[1]
	addr := os.Args[2]
	if transferType == "-send" {
		s := &TcpServer{addr: addr}
		err := s.sendForTransfer()
		if err != nil {
			panic(fmt.Errorf("error occured while sending file \n %w", err))
		}
		fmt.Printf("file sent succesfully \n")
	} else if transferType == "-listen" {
		s := &TcpServer{addr: addr}
		err := s.ListenForTransfer()
		if err != nil {
			panic(fmt.Errorf("error occured while listening for transfer \n %w", err))
		}
		fmt.Println("file received succesfully ")
	}

}
