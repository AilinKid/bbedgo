package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/timest/env"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "macbook"
	password = "612713tlx"
	dbname   = "postgres"
)

const (
	PAGESIZE   = 8192 //8k
	HEADERSIZE = 24
)

type CommandType int32

const (
	INVAILD CommandType = iota
	HELP
	SET_TABLE
	SET_BLOCK
	SET_OFFSET
	SHOW_BLOCK
	SHOW_OFFSET
	SHOW_PAGE_HEADER_DATA
	EXIT
)

type command struct {
	cmd  CommandType
	args interface{}
}

type config struct {
	PGDATA string `default:"-1"`
}

type pageHeader struct { //sizeof(pageHeader)= 24字节，后面就是数据了
	xlogid              uint32
	xrecoff             uint32
	pd_checksum         uint16
	pd_flags            uint16
	pd_lower            uint16
	pd_upper            uint16
	pd_special          uint16
	pd_pagesize_version uint16
	pd_prune_xid        uint32
}

type page struct {
	header  pageHeader
	pd_linp uint32
}

type linp struct {
	/*
		lp_off [15]bit
		lp_flags [2]bit
		lp_len [15]bit
	*/
	linp_val uint32
}

type tuple struct {
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
	//fmt.Println(unsafe.Sizeof(pageHeader{}))
	file, err := os.Open("/Users/macbook/postgres/data/base/12558/17544")
	if err != nil {
		panic(err)
	}
	file.Close()
}

type server struct {
	file        *os.File
	PGDATA      string
	relPath     string
	blockNum    int
	offset      int
	stopChannel chan bool
	data        [8192]byte
}

func (s *server) Run() {
	s.run()
}

func (s *server) run() {
	//read line and analysis the input
	for {
		inputReader := bufio.NewReader(os.Stdin)
		fmt.Print("bbedgo=# ")
		input, _, err := inputReader.ReadLine()
		if err != nil {
			panic(err)
		}
		//fmt.Println("get string:",string(input))
		cmd := parser(string(input))
		s.handle(cmd)
	}

}

func (s *server) Stop() {
	s.stop()
}

func (s *server) stop() {

}

func (s *server) handle(cmd command) {
	switch cmd.cmd {
	case INVAILD:
		fmt.Println("invalid command")
		s.doHelp()
	case HELP:
		s.doHelp()
	case SET_TABLE:
		s.doSet(cmd)
	case SET_BLOCK:
		s.doSet(cmd)
	case SET_OFFSET:
		s.doSet(cmd)
	case SHOW_BLOCK:
		s.doShow(cmd)
	case SHOW_OFFSET:
		s.doShow(cmd)
	case SHOW_PAGE_HEADER_DATA:
		s.doShow(cmd)
	case EXIT:
		os.Exit(1)
	}
}

func parser(str string) command {
	//parse the str
	trimStr := strings.TrimSpace(str)
	lowerTrimStr := strings.ToLower(trimStr)
	tokens := strings.Fields(lowerTrimStr)
	if len(tokens) == 0 {
		return command{cmd: INVAILD}
	}
	//it's ok
	switch tokens[0] {
	case "help":
		return command{cmd: HELP}
	case "set":
		if len(tokens) <= 2 { //set needs 3 params
			return command{cmd: INVAILD}
		} else {
			switch tokens[1] {
			case "block":
				res, err := strconv.Atoi(tokens[2])
				if err != nil {
					return command{cmd: INVAILD}
				}
				return command{cmd: SET_BLOCK, args: res}
			case "offset":
				res, err := strconv.Atoi(tokens[2])
				if err != nil {
					return command{cmd: INVAILD}
				}
				return command{cmd: SET_OFFSET, args: res}
			case "table":
				return command{cmd: SET_TABLE, args: tokens[2]}
			}
		}
	case "show":
		if len(tokens) <= 1 {
			return command{cmd: INVAILD}
		} else {
			switch tokens[1] {
			case "block":
				return command{cmd: SHOW_BLOCK}
			case "offset":
				return command{cmd: SHOW_OFFSET}
			case "phd":
				return command{cmd: SHOW_PAGE_HEADER_DATA}
			}
		}
	case "exit":
		return command{cmd: EXIT}
	}
	return command{cmd: INVAILD}
}

//go get the env path of "PGDATA"
func getPgdataEnv() (path string) {
	cfg := new(config)
	env.IgnorePrefix() //默认是用外部结构体CONFIG作为前缀的，需要把这个忽略
	err := env.Fill(cfg)
	if err != nil {
		panic(err)
	}
	path = cfg.PGDATA
	return
}

//odbc to PostgreSQL get the table path
func relName2Path(relName string) (path string) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	//exec the sql
	sql := "select pg_relation_filepath('" + relName + "');"
	res, err := db.Query(sql)
	if err != nil {
		panic(err)
	}
	//get one row and one col
	var temp = make([]interface{}, 1)
	var row = make([]string, 1)
	for i := 0; i < 1; i++ {
		temp[i] = &row[i]
	}
	path = ""
	for res.Next() {
		err := res.Scan(temp...) //this params  only receive the interface
		if err != nil {
			panic(err)
		} else {
			//get the row
			path = row[0]
			break
		}
	}
	return
}

// do command functions
func (s *server) doHelp() {
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

func (s *server) doShow(cmd command) {
	if cmd.cmd == SHOW_OFFSET {
		fmt.Println("BLOCK OFFSET : ", s.offset)
	}
	if cmd.cmd == SHOW_BLOCK {
		fmt.Println("BLOCK NUMBER : ", s.blockNum)
	}
	if cmd.cmd == SHOW_PAGE_HEADER_DATA {
		fmt.Println("reading block ... ", s.blockNum)
		s.doReadAtOffset()
		fmt.Println(s.data)
		s.mapPageHeaderData()
	}
}

func (s *server) doSet(cmd command) {
	if cmd.cmd == SET_BLOCK {
		s.blockNum = cmd.args.(int)
		fmt.Println("SET BLOCK : ", cmd.args)
	}
	if cmd.cmd == SET_OFFSET {
		s.offset = cmd.args.(int)
		fmt.Println("SET OFFSET : ", cmd.args)
	}
	if cmd.cmd == SET_TABLE {
		s.relPath = getPgdataEnv() + "/" + relName2Path(cmd.args.(string)) //interface和其他类型的转化
		fmt.Println("SET TABLE : ", cmd.args)
	}
}

func (s *server) doReadAtOffset() error {
	fmt.Println("path ", s.relPath)
	file, err := os.Open(s.relPath)
	if err != nil {
		panic(err)
	}
	s.file = file
	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fmt.Println("file size:", stat.Size())
	offset := s.blockNum * PAGESIZE
	byteBuf := make([]byte, PAGESIZE)
	n, err := s.file.ReadAt(byteBuf, int64(offset))
	if err != nil || n != PAGESIZE {
		return errors.New("read fail")
	}
	//数组和切片转化,而不是s.data = byteBuf.
	for i := 0; i < 8192; i++ {
		s.data[i] = byteBuf[i]
	}
	return nil
}

func (s *server) mapPageHeaderData() {
	header := (*pageHeader)(unsafe.Pointer(&s.data))
	fmt.Println("xlogid", header.xlogid)
	fmt.Println("xrecoff", header.xrecoff)
	fmt.Println("pd_checksum", header.pd_checksum)
	fmt.Println("pd_flags", header.pd_flags)
	fmt.Println("pd_lower", header.pd_lower)
	fmt.Println("pd_upper", header.pd_upper)
	fmt.Println("pd_special", header.pd_special)
	fmt.Println("pd_pagesize_version", header.pd_pagesize_version)
	fmt.Println("pd_prune_xid", header.pd_prune_xid)

	s.mapLinpHeaderData()
}

func (s *server) mapLinpHeaderData() {
	//这个指针转化真的骚
	linp := (*linp)(unsafe.Pointer(uintptr(unsafe.Pointer(&s.data)) + uintptr(6*unsafe.Sizeof(int32(0)))))
	linp_v := linp.linp_val
	fmt.Println("linp_v", linp_v)
	//位解析
	bitString := ""
	var temp_bit uint32
	temp_bit = 1
	for i := 0; i < 32; i++ { //字节内正常存储，字节间按大端存储
		if temp_bit&linp_v != 0 {
			bitString += "1"
		} else {
			bitString += "0"
		}
		temp_bit = temp_bit << 1

	}
	bitString = reverseString(bitString)
	//fmt.Println("lp_off", bitString)
	//fmt.Println("lp_len", linp.lp_len)
	//fmt.Println("lp_flags", linp.lp_flags)
}

func reverseString(s string) string {
	runes := []rune(s)
	for from, to := 0, len(runes)-1; from < to; from, to = from+1, to-1 {
		runes[from], runes[to] = runes[to], runes[from]
	}
	return string(runes)
}
