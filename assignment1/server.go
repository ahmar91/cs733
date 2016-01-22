package main

import(
"sync"
"strconv"
"strings"
"bufio"
"fmt"
"net"
"time"
)

type File struct {
	vno int64
	starttime time.Time
	exptime int64
	contents []byte
}

var m map[string]File
var mlock sync.RWMutex

func serverMain(){
	m = make(map[string]File)
	go fileCleaner()	
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn){
	defer conn.Close()
	reader := bufio.NewReader(conn)	
	for {	
		buf,_,err := reader.ReadLine()
		if err != nil {
			conn.Close()
			return
		}
		str := string(buf)
		tokens := strings.Split(str," ")
		if tokens[0] == "write"{
			handleWrite(tokens,reader,conn)
		}else if tokens[0] == "read"{
			handleRead(tokens,conn)
		}else if tokens[0] == "cas"{
			handleCas(tokens,reader,conn)
		}else if tokens[0] == "delete"{
			handleDelete(tokens,conn)
		}else{
			s := fmt.Sprintf("ERR_CMD_ERR\r\n")
			conn.Write([]byte(s))
		}
	}
}

func handleWrite(tokens []string,reader *bufio.Reader, conn net.Conn){
	var b byte
	var err error 
	var barr []byte
	var s string	
	numbytes,_ := strconv.Atoi(tokens[2])
	barr = make([]byte,numbytes)
	for i := 0; i<numbytes; i++ {
		for {
			b,err = reader.ReadByte()
			if err != nil {
				continue
			} else {
				break
			}
		}
		barr[i] = b
	}
	b1,_ := reader.ReadByte()
	b2,_ := reader.ReadByte()
	mlock.Lock()		
	if b1 == '\r' && b2 == '\n' {
		f,status := m[tokens[1]]
		f.contents = barr
		f.starttime = time.Now()
		if len(tokens)<4{
			f.exptime = -1
		}else{
			f.exptime,_ = strconv.ParseInt(tokens[3],10,64)
		}
		if status {
			f.vno++
			m[tokens[1]] = f			
		}else{
			f.vno = 1
			m[tokens[1]] = f
		}
		s = fmt.Sprintf("OK %v\r\n",m[tokens[1]].vno)	
	}else{
		s = fmt.Sprintf("ERR_CMD_ERR\r\n")
	}
	conn.Write([]byte(s))
	mlock.Unlock()	
}

func handleRead(tokens []string, conn net.Conn){
	mlock.Lock()
	f,status := m[tokens[1]]
	temp := int64((time.Now().Sub(f.starttime)).Seconds())
	if status &&  (f.exptime == -1 || temp < f.exptime) {
		s := fmt.Sprintf("CONTENTS %v %v %v\r\n",f.vno,len(f.contents),f.exptime)
		conn.Write([]byte(s))
		conn.Write(f.contents)
		conn.Write([]byte("\r\n"))
	}else{
		s := fmt.Sprintf("ERR_FILE_NOT_FOUND\r\n")
		conn.Write([]byte(s))
		delete(m,tokens[1])
	}
	mlock.Unlock()
}

func handleCas(tokens []string,reader *bufio.Reader, conn net.Conn){
	var b byte
	var err error 
	var barr []byte
	var s string	
	numbytes,_ := strconv.Atoi(tokens[3])
	barr = make([]byte,numbytes)
	for i := 0; i<numbytes; i++ {
		for {
			b,err = reader.ReadByte()
			if err != nil {
				continue
			} else {
				break
			}
		}
		barr[i] = b
	}
	b1,_ := reader.ReadByte()
	b2,_ := reader.ReadByte()
	mlock.Lock()
	if b1 == '\r' && b2 == '\n' {
		f,status := m[tokens[1]]
		temp := int64((time.Now().Sub(f.starttime)).Seconds())
		if status && (f.exptime == -1 || temp < f.exptime){
			version,_ := strconv.ParseInt(tokens[2],10,64)
			if f.vno == version {
				f.contents = barr
				f.starttime = time.Now()
				if len(tokens)<5{
					f.exptime = -1
				}else{
					f.exptime,_ = strconv.ParseInt(tokens[4],10,64)
				}
				f.vno++
				s = fmt.Sprintf("OK %v\r\n",f.vno)
				m[tokens[1]] = f
			}else{
				s = fmt.Sprintf("ERR_VERSION %v\r\n",f.vno)
			}
		}else{
				if !status {
					s = fmt.Sprintf("ERR_FILE_NOT_FOUND\r\n")
				}else{
					delete(m,tokens[1])
				}
		}
	}else{
		s = fmt.Sprintf("ERR_CMD_ERR\r\n")
	}
	conn.Write([]byte(s))
	mlock.Unlock()
}

func handleDelete(tokens []string,conn net.Conn){
	var s string 	
	mlock.Lock()
	f,status := m[tokens[1]]
	temp := int64((time.Now().Sub(f.starttime)).Seconds())	
	if status && (f.exptime == -1 || temp < f.exptime){
		delete(m,tokens[1])
		s = fmt.Sprintf("OK\r\n")
	}else{
			if !status {
				s = fmt.Sprintf("ERR_FILE_NOT_FOUND\r\n")
			}else{
				delete(m,tokens[1])
			}
	}
	mlock.Unlock()
	conn.Write([]byte(s))
}

func fileCleaner() {
	for {
		time.Sleep(500*time.Second)
		mlock.Lock()
		for key,value := range m {
			temp := int64((time.Now().Sub(value.starttime)).Seconds())
			if value.exptime != -1 && value.exptime < temp{
				delete(m,key)
			}	
		}
		mlock.Unlock()
	}
}

func main(){
	serverMain()
}