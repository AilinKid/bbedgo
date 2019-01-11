package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/timest/env"
	"os"
	"strconv"
	"strings"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "macbook"
	password = "612713tlx"
	dbname   = "postgres"
)

type CommandType int32

const(
	INVAILD CommandType = iota
	HELP
	SET_BLOCK
	SET_OFFSET
	SHOW_BLOCK
	SHOW_OFFSET
	EXIT
)

type command struct{
	cmd	CommandType
	args int
}

type config struct {
	PGDATA string 	`default:"-1"`
}


func main() {
	//test relName2Path ok
	fmt.Println("path:", relName2Path("remotepg"))
	//test the env
	fmt.Println("env:", getPgdataEnv())
	//
	ser := server{}
	ser.Run()
	//
	
}

type server struct{
	PGDATA string
	relName string
	blockNum int
	offset int
	stopChannel chan bool
}

func (s *server)Run(){
	s.run()
}

func (s *server)run(){
	//read line and analysis the input
	for{
		inputReader := bufio.NewReader(os.Stdin)
		fmt.Print("bbedgo=# ")
		input, _, err := inputReader.ReadLine()
		if err != nil{
			panic(err)
		}
		//fmt.Println("get string:",string(input))
		cmd := parser(string(input))
		s.handle(cmd)
	}

}

func (s *server)Stop(){
	s.stop()
}

func (s *server)stop(){

}

func (s *server)handle(cmd command){
	switch cmd.cmd {
	case INVAILD:
		fmt.Println("invalid command")
		s.doHelp()
	case HELP:
		s.doHelp()
	case SET_BLOCK:
		s.doSet(cmd)
	case SET_OFFSET:
		s.doSet(cmd)
	case SHOW_BLOCK:
		s.doShow(cmd)
	case SHOW_OFFSET:
		s.doShow(cmd)
	case EXIT:
		os.Exit(1)
	}
}


func parser(str string)command{
	//parse the str
	trimStr := strings.TrimSpace(str)
	lowerTrimStr := strings.ToLower(trimStr)
	tokens := strings.Fields(lowerTrimStr)
	if len(tokens) == 0{
		return command{cmd:INVAILD}
	}
	//it's ok
	switch tokens[0] {
	case "help":
		return command{cmd:HELP}
	case "set":
		if len(tokens)<=2{      //set needs 3 params
			return command{cmd:INVAILD}
		}else if res, err := strconv.Atoi(tokens[2]); err!=nil{
			return command{cmd:INVAILD}
		}else{
			switch tokens[1] {
			case "block":
				return command{cmd:SET_BLOCK, args:res}
			case "offset":
				return command{cmd:SET_OFFSET, args:res}
			}
		}
	case "show":
		if len(tokens)<=1{
			return command{cmd:INVAILD}
		}else{
			switch tokens[1] {
			case "block":
				return command{cmd:SHOW_BLOCK}
			case "offset":
				return command{cmd:SHOW_OFFSET}
			}
		}
	case "exit":
		return command{cmd:EXIT}
	}
	return command{cmd:INVAILD}
}


//go get the env path of "PGDATA"
func getPgdataEnv()(path string){
	cfg := new(config)
	env.IgnorePrefix()    //默认是用外部结构体CONFIG作为前缀的，需要把这个忽略
	err := env.Fill(cfg)
	if err != nil {
		panic(err)
	}
	path = cfg.PGDATA
	return
}


//odbc to PostgreSQL get the table path
func relName2Path(relName string)(path string){
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil{
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil{
		panic(err)
	}

	//exec the sql
	sql := "select pg_relation_filepath('"+relName+"');"
	res, err:= db.Query(sql)
	if err!=nil{
		panic(err)
	}
	//get one row and one col
	var temp = make([]interface{}, 1)
	var row = make([]string, 1)
	for i := 0; i<1; i++{
		temp[i] = &row[i]
	}
	path = ""
	for res.Next(){
		err := res.Scan(temp...)   //this params  only receive the interface
		if err != nil{
			panic(err)
		}else{
			//get the row
			path = row[0]
			break
		}
	}
	return
}


// do command functions
func (s *server)doHelp(){
	fmt.Print("BBED_GO: RELEASE 1.1.0 -BETA\n" +
		"Copyright (c) 2019/1/11, arenatlx and/or its affiliates.  All rights reserved.\n" +
		"********************Test for internal detection of PostgreSQL*****************\n" +
		"help                         : ask for help\n" +
		"set block [block number]     : set current pointer to specified block\n" +
		"set offset [offset in block] : set current pointer to specified pos in block\n" +
		"show block                   : show current block number\n" +
		"show offset                  : show current offset in block\n" +
		"exit                         : quit the system\n\n")
}


func (s *server)doShow(cmd command){
	if cmd.cmd == SHOW_OFFSET{
		fmt.Println("BLOCK OFFSET : ", s.offset)
	}
	if cmd.cmd == SHOW_BLOCK{
		fmt.Println("BLOCK NUMBER : ", s.blockNum)
	}
}

func (s *server)doSet(cmd command){
	if cmd.cmd == SET_BLOCK{
		s.blockNum = cmd.args
		fmt.Println("SET BLOCK : ", cmd.args)
	}
	if cmd.cmd == SET_OFFSET{
		s.offset = cmd.args
		fmt.Println("SET OFFSET : ", cmd.args)
	}
}
