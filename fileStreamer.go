package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

type Config struct {
	ServerAddr   string `json:"serverAddr"`
	InputFile    string `json:"inputFile"`
	OutputFile   string `json:"outputFile"`
	ParallelPage int    `json:"parallelpage"`
	Chunk        int    `json:"chunk"`
	ClientAddr   string `json:"clientAddr"`
}

type TcpServer struct {
	addr         string
	datachannels map[int]*dataChannel
	synContext   *synContext
	config       *Config
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

func (s *TcpServer) LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	config := &Config{}
	if err := decoder.Decode(config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return config, nil
}

func (s *TcpServer) ListenForTransfer() error {
	fmt.Println("listening for file transfer ")
	config, err := s.LoadConfig("./config.json")
	if err != nil {
		panic(fmt.Errorf("unable to read config files \n %w", err))
	}
	s.addr = config.ServerAddr
	s.config = config
	ln, err := net.Listen("tcp", s.addr)
	s.synContext = &synContext{}
	s.datachannels = make(map[int]*dataChannel)
	if err != nil {
		return fmt.Errorf("unable to create listner : %w", err)
	}
	for i := 0; i < s.config.ParallelPage; i++ {
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
	s.synContext.wg.Wait()
	return errf
}

func (s *TcpServer) recvHandler(id int, conn net.Conn, dataChnl *dataChannel) {
	fmt.Printf("starting recvHanlder id : %d\n", id)
	defer s.synContext.wg.Done()
	buffConn := bufio.NewReaderSize(conn, 2*s.config.Chunk)
	for {
		if dataChnl.isclosed {
			fmt.Printf("channel closed for recv handler id : %d \n", id)
			return
		}
		data := make([]byte, s.config.Chunk)
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
	file, err := os.Create(s.config.OutputFile)
	if err != nil {
		return fmt.Errorf("unable to open file for writing \n %w", err)
	}
	for {
		openchans := s.config.ParallelPage
		for i := 0; i < s.config.ParallelPage; i++ {
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
	config, err := s.LoadConfig("./config.json")
	if err != nil {
		panic(fmt.Errorf("unable to read config files \n %w", err))
	}
	s.config = config
	s.addr = s.config.ClientAddr
	s.synContext = &synContext{}
	s.datachannels = make(map[int]*dataChannel)
	for i := 0; i < s.config.ParallelPage; i++ {
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
	errr := s.sendfile()
	s.synContext.wg.Wait()
	return errr
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
	file, _ := os.Open(s.config.InputFile)
	filereader := bufio.NewReaderSize(file, 2*s.config.Chunk)
	for {
		isreadcompleted := false
		for i := 0; i < s.config.ParallelPage; i++ {
			data := make([]byte, s.config.Chunk)
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
	if transferType == "-send" {
		s := &TcpServer{}
		err := s.sendForTransfer()
		if err != nil {
			panic(fmt.Errorf("error occured while sending file \n %w", err))
		}
		fmt.Printf("file sent succesfully \n")
	} else if transferType == "-listen" {
		s := &TcpServer{}
		err := s.ListenForTransfer()
		if err != nil {
			panic(fmt.Errorf("error occured while listening for transfer \n %w", err))
		}
		fmt.Println("file received succesfully ")
	}

}
