# cs733
Firstly set the GOPATH and GOBIN variable and put the cs733 folder isnside <GOPATH>

Install:

go install GOPATH/cs733/assignment1

Now executable will be available in <GOBIN>

Go to bin folder and run the server using
./server


Assumptions:
-------------------------------------
1. Server is running on port no. 8080
2. Server stores the files in memory, so when server is closed, all the data is lost
3. Maximum number of bytes in a file should be 1000000000

-------------------------------------
Testing:

Test file name: ahmar_test.go
Run using -- go test $GOPATH/cs733/assignment1

If it outputs "ok" it means all test cases passed successfully.
