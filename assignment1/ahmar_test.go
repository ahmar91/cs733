package main

import (
        "bufio"
        "fmt"
        "net"
        "strconv"
        "strings"
        "testing"
        "time"
)

//Output verification methods
func ReadVerify(t *testing.T,output string,version int64,contents string,outputcontents string) {
    arr := strings.Split(output, " ")
    expect(t, arr[0], "CONTENTS")
    expect(t, arr[1], strconv.FormatInt(int64(version),10)) // expect only accepts strings, convert int version to string    
    expect(t, arr[2], strconv.FormatInt(int64(len(contents)),10))
    expect(t, contents, outputcontents)
}

func WriteVerify(t *testing.T,output string) (int64){
    arr := strings.Split(output, " ") // split into OK and <version>
    expect(t, arr[0], "OK")
    version,err := strconv.ParseInt(arr[1],10,64)
    if err!=nil {
        t.Error("Version is not numeric")
    }
    return version
}

func DeleteVerify(t *testing.T,output string) {
    expect(t,output,"OK");
}

func CasVerify(t *testing.T,output string) (int64) {
    arr := strings.Split(output, " ") // split into OK and <version>
    expect(t, arr[0], "OK")
    version,err := strconv.ParseInt(arr[1],10,64)
    if err!=nil {
        t.Error("Version is not numeric")
    }
    return version
}

func CasVerErrVerify(t *testing.T,output string) (int64){
    arr := strings.Split(output, " ") // split into OK and <version>
    expect(t, arr[0], "ERR_VERSION")
    version,err := strconv.ParseInt(arr[1],10,64)
    if err!=nil {
        t.Error("Version is not numeric")
    }
    return version
}

func FileNotFoundVerify(t *testing.T,output string) {
    expect(t,output,"ERR_FILE_NOT_FOUND")
}

//thread creation method
func CreateConnections(t *testing.T,count int) ([]net.Conn) {   
    var err error
    conn := make([]net.Conn, count)
    for i:=0; i<count; i++ {
        conn[i],_ = net.Dial("tcp","localhost:8080")
        if err !=nil {
            t.Error(err.Error())
            break
        }
    }
    return conn
}



//main driver function
func TestMain(m *testing.T) {
       go serverMain()   // launch the server as a goroutine.
       time.Sleep(1 * time.Second) 
}

//start of testcases
// Simple serial check of read and write
func TestTCPSimple(t *testing.T) {
        name := "hi.txt"
        contents := "bye"
        exptime := 300000
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)

        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        version := WriteVerify(t,resp)

        // try read now
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()
        output := scanner.Text()
        scanner.Scan()     
        ReadVerify(t,output,version,contents,scanner.Text())
}

// Simple serial check of read,write,cas,delete including ERR_FILE_NOT_FOUND condition
func TestBasicOperations(t *testing.T) {
        name := "hi.txt"
        contents := "bye"
        exptime := 300000
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)


        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        version := WriteVerify(t,resp)


        // try read now
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()
        output := scanner.Text()
        scanner.Scan()     
        ReadVerify(t,output,version,contents,scanner.Text())

        // try cas now
        contents = "i am ahmar"
        fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n",name,version,len(contents),exptime,contents)
        scanner.Scan() // read first line
        version = CasVerify(t,scanner.Text())

        // try delete
        fmt.Fprintf(conn,"delete %v\r\n",name)
        scanner.Scan()
        DeleteVerify(t,scanner.Text())

        //try read file not found
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()     
        FileNotFoundVerify(t,scanner.Text())

        //try delete file not found
        fmt.Fprintf(conn,"delete %v\r\n",name)
        scanner.Scan()
        FileNotFoundVerify(t,scanner.Text())
}

//checking serial cas version error
func TestCasVerErr(t *testing.T) {
        name := "hi.txt"
        contents := "bye"
        exptime := 300000
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        
        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        version := WriteVerify(t,resp)

        // try a cas command with incorrect version number
        contents = "CS 733 Engineering a cloud"
        fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n",name,version+2,len(contents),exptime,contents)
        scanner.Scan() // read first line
        version = CasVerErrVerify(t,scanner.Text())
}

// test expiretime
func TestExpTime(t *testing.T) {
        name := "hi.txt"
        contents := "ahmar"
        exptime := 5
        conn, err := net.Dial("tcp", "localhost:8080")
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        // Write a file
        _,err = fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime ,contents)
        if err !=nil {
                fmt.Printf("error in writing in buffer\n")
        }
        scanner.Scan() // read first line
        resp := scanner.Text() // extract the text from the buffer
        _ = WriteVerify(t,resp)

        time.Sleep(10*time.Second)
        //try read file not found
        fmt.Fprintf(conn, "read %v\r\n", name)
        scanner.Scan()     
        FileNotFoundVerify(t,scanner.Text())
}

//concurrency testing
// general method for performaing cas operation
func DoCas(version int64,name string,contents string,conn net.Conn,dataChannel chan int) {    
    fmt.Fprintf(conn, "cas %v %v %v\r\n%v\r\n",name,version,len(contents),contents)
    scanner := bufio.NewScanner(conn)
    scanner.Scan()
    output := scanner.Text()
    arr := strings.Split(output," ")
    if arr[0] == "ERR_VERSION" {
            dataChannel <- 0
    } else {
            dataChannel <- 1
    }
}

// If more than one client change the same version,only one of them should succeed
func TestMultiCasSingleUpdate(t *testing.T) {
    dataChannel := make(chan int)
    num_of_conns := 5
    conn := CreateConnections(t,num_of_conns)
    name := "hi.txt"
    contents := "this is concurrency testing 0"
    scanner := bufio.NewScanner(conn[0])
    _,err := fmt.Fprintf(conn[0], "write %v %v\r\n%v\r\n", name, len(contents) ,contents)
    if err !=nil {
        t.Error(err.Error())
    }
    scanner.Scan()
    version := WriteVerify(t,scanner.Text())
    for i:=0;i<num_of_conns;i++ {
        contents = "this is concurrency testing " + strconv.Itoa(i+1);
        go DoCas(version,name,contents,conn[i],dataChannel)
    }
    // verify with read, value of x should be 1
    var x int = 0
    for i := 0; i < num_of_conns; i++ {
        x = x +  <-dataChannel
    }
    if x!=1 {
        t.Error(fmt.Sprintf("More than one client have changed version %v of file %v",version,name)) // t.Error is visible when running `go test -verbose`
    }
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}