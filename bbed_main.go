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
	SHOW_LINPS
	SHOW_LINP
	SHOW_TUPLE
	SHOW_TABLE
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

type linp_t struct {
	lp_off   uint
	lp_flags uint
	lp_len   uint
}

type blockIdData struct {
	bi_hi uint16
	bi_lo uint16
}

type itemPointerData struct {
	ip_blkid blockIdData
	ip_posid uint16
}

type HeapTupleHeaderData struct {
	t_xmin      uint32
	t_xmax      uint32
	t_cid       uint32
	t_ctid      itemPointerData
	t_infomask2 uint16
	t_infomask  uint16
	t_hoff      uint8 /* sizeof header incl. bitmap, padding */
	// 23 byte header here
	t_bits [0]uint8 //bit map of header
}

type tuple struct {
	attrVal []string
}

func main() {
	//test relName2Path ok
	//fmt.Println(unsafe.Sizeof(HeapTupleHeaderData{}))
	//path, attNum, attName, attLen := relName2Path("remotepg")
	//fmt.Println(path, attNum, attName[2], len(attLen))
	//test the env
	//fmt.Println("env:", getPgdataEnv())
	//
	ser := server{}
	ser.Run()
	//
	//fmt.Println(unsafe.Sizeof(pageHeader{}))
	//file, err := os.Open("/Users/macbook/postgres/data/base/12558/17544")
	//if err != nil {
	//	panic(err)
	//}
	//file.Close()
}

type server struct {
	file *os.File
	// file path
	PGDATA  string
	relPath string

	//show tuple needed
	relOid  int
	attNum  int
	attName []string
	attLen  []int

	// manual set
	blockNum int
	offset   int

	//count linp nums needed
	lower   uint16 //where linps will encounter
	upper   uint16 //where tuples will start
	linpNum uint16

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
	case SHOW_LINP:
		s.doShow(cmd)
	case SHOW_LINPS:
		s.doShow(cmd)
	case SHOW_TUPLE:
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
			case "linps":
				return command{cmd: SHOW_LINPS}
			case "linp":
				if len(tokens) != 3 {
					return command{cmd: INVAILD}
				}
				id, err := strconv.Atoi(tokens[2])
				if err != nil {
					return command{cmd: INVAILD}
				}
				return command{cmd: SHOW_LINP, args: id}
			case "tuple":
				if len(tokens) != 3 {
					return command{cmd: INVAILD}
				}
				id, err := strconv.Atoi(tokens[2])
				if err != nil {
					return command{cmd: INVAILD}
				}
				return command{cmd: SHOW_TUPLE, args: id}
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
func relName2Path(relName string) (path string, relOid int, attName []string, attLen []int) {
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

	//get the table oid here
	sql = "select oid from pg_class where relname='" + relName + "';"
	res, err = db.Query(sql)
	if err != nil {
		panic(err)
	}
	relOid = 0
	for res.Next() {
		err := res.Scan(temp...) //this params  only receive the interface
		if err != nil {
			panic(err)
		} else {
			//get the row
			relOid, err = strconv.Atoi(row[0])
			if err != nil {
				panic(err)
			}
			break
		}
	}
	//fmt.Println(string(relOid))

	//get the schema info
	sql = "select attname, attlen from pg_attribute where attrelid=" + strconv.Itoa(relOid) + " order by attnum offset 6;"
	res, err = db.Query(sql)
	if err != nil {
		panic(err)
	}
	//最长设置20个属性好了，这里可以调
	temp = make([]interface{}, 2)
	row = make([]string, 2)
	for i := 0; i < 2; i++ {
		temp[i] = &row[i]
	}
	attName = make([]string, 0)
	attLen = make([]int, 0)
	for res.Next() {
		err := res.Scan(temp...) //this params  only receive the interface
		if err != nil {
			panic(err)
		} else {
			//get the row
			attName = append(attName, row[0])
			aLen, err := strconv.Atoi(row[1])
			if err != nil {
				panic(err)
			}
			attLen = append(attLen, aLen)
		}
	}
	return
}

// do command functions
func (s *server) doHelp() {
	fmt.Print("BBED_GO: RELEASE 1.1.0 -BETA\n" +
		"Copyright (c) 2019/1/11, arenatlx and/or its affiliates.  All rights reserved.\n" +
		"+-------------------Test for internal detection of PostgreSQL-------------------+\n" +
		"| help                         : ask for help                                   |\n" +
		"| set table [table name]       : set the table name (PGDATA env needed)         |\n" +
		"| set block [block number]     : set current pointer to specified block         |\n" +
		"| set offset [offset in block] : set current pointer to specified pos in block  |\n" +
		"| show block                   : show current block number                      |\n" +
		"| show offset                  : show current offset in block                   |\n" +
		"| show phd                     : show page_header_data structure in block       |\n" +
		"| show linp [linp id]          : show specified id start from 1                 |\n" +
		"| show linps                   : show all linp structure in this block          |\n" +
		"| show tuple [tuple id]        : show tuple header and data by linp id(not safe)|\n" +
		"| exit                         : quit the system                                |\n" +
		"+-------------------------------------------------------------------------------+\n")
}

func (s *server) doShow(cmd command) {
	switch cmd.cmd {
	case SHOW_TABLE:

	case SHOW_OFFSET:
		fmt.Println("BLOCK OFFSET : ", s.offset)
	case SHOW_BLOCK:
		fmt.Println("BLOCK NUMBER : ", s.blockNum)
	case SHOW_PAGE_HEADER_DATA:
		//fmt.Println("reading block ... ", s.blockNum)
		//fmt.Println(s.data)
		s.mapPageHeaderData()
	case SHOW_LINPS:
		s.getAllLinps()
	case SHOW_LINP:
		s.getSpecifiedLinp(cmd.args.(int), true)
	case SHOW_TUPLE:
		s.getTupleMeta(cmd.args.(int))
	}
}

func (s *server) doSet(cmd command) {
	if cmd.cmd == SET_BLOCK {
		s.blockNum = cmd.args.(int)
		err := s.doReadAtOffset() //refresh data
		if err != nil {
			fmt.Println("SET BLOCK ERROR (cann't load data): ", cmd.args)
		} else {
			fmt.Println("SET BLOCK: ", cmd.args)
		}
	}
	if cmd.cmd == SET_OFFSET {
		s.offset = cmd.args.(int) //now we do nothing for the offset,you can use linp id to check
		fmt.Println("SET OFFSET : ", cmd.args)
	}
	if cmd.cmd == SET_TABLE {
		//s.relPath = getPgdataEnv() + "/" + relName2Path(cmd.args.(string)) //interface和其他类型的转化
		envPrefix := getPgdataEnv()
		path, relOid, attName, attLen := relName2Path(cmd.args.(string))
		s.relPath = envPrefix + "/" + path
		s.relOid = relOid
		s.attNum = len(attLen)
		s.attName = attName
		s.attLen = attLen
		fmt.Println("SET TABLE : ", cmd.args)
		s.doReadAtOffset()
	}
}

func (s *server) doReadAtOffset() error {
	//fmt.Println("path ", s.relPath)
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
	//fmt.Println(offset)
	byteBuf := make([]byte, PAGESIZE)
	n, err := s.file.ReadAt(byteBuf, int64(offset))
	if err != nil || n != PAGESIZE {
		return errors.New("read fail")
	}
	//fmt.Println(byteBuf)
	//数组和切片转化,而不是s.data = byteBuf.
	for i := 0; i < 8192; i++ {
		s.data[i] = byteBuf[i]
	}
	return nil
}

func (s *server) mapPageHeaderData() {
	header := (*pageHeader)(unsafe.Pointer(&s.data))
	fmt.Printf("block : "+strconv.Itoa(s.blockNum)+"\n"+
		"+----------PageHeaderData----------+\n"+
		"| xlogid        :   %10d     |\n"+
		"| xrecoff       :   %10d     |\n"+
		"| pd_checksum   :   %10d     |\n"+
		"| pd_flags      :   %10d     |\n"+
		"| pd_lower      :   %10d     |\n"+
		"| pd_upper      :   %10d     |\n"+
		"| pd_special    :   %10d     |\n"+
		"| pd_psize_ver  :   %10d     |\n"+
		"| pd_prune_xid  :   %10d     |\n"+
		"+----------------------------------+\n", header.xlogid, header.xrecoff, header.pd_checksum,
		header.pd_flags, header.pd_lower, header.pd_upper, header.pd_special,
		header.pd_pagesize_version, header.pd_prune_xid)

	//store the upper and lower
	s.upper = header.pd_upper
	s.lower = header.pd_lower
}

func (s *server) getSpecifiedLinp(id int, show bool) (linp_t, bool) {
	if s.linpNum == 0 {
		if s.lower == 0 {
			header := (*pageHeader)(unsafe.Pointer(&s.data))
			s.upper = header.pd_upper
			s.lower = header.pd_lower
		}
		// count linp space
		s.linpNum = (s.lower - 24) / 4
	}
	//fmt.Println(s.lower)
	if s.linpNum >= uint16(id) && id >= 0 {
		return s.mapLinpHeaderData(uint16(id), (uint16(id)-1)*4, show), true
	}
	return linp_t{}, false
}

func (s *server) getAllLinps() {
	//count the linps / per for 4 byte
	if s.lower == 0 {
		header := (*pageHeader)(unsafe.Pointer(&s.data))
		s.upper = header.pd_upper
		s.lower = header.pd_lower
	}
	// count linp space
	s.linpNum = (s.lower - 24) / 4
	//fmt.Println("linpNum:", s.linpNum)
	for i := uint16(0); i < s.linpNum; i++ {
		s.mapLinpHeaderData(i+1, i*4, true)
	}
}

func (s *server) mapLinpHeaderData(id, offset uint16, show bool) linp_t {
	//这个指针转化真的骚
	linp := (*linp)(unsafe.Pointer(uintptr(unsafe.Pointer(&s.data)) + uintptr(uintptr(offset)+HEADERSIZE*unsafe.Sizeof(byte(0)))))
	linp_v := linp.linp_val
	//fmt.Println("linp_v", linp_v)
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
	bitString = reverseString(bitString) //这个存储应该是用的小端存储，所以解析顺序要换一下
	//fmt.Println("bitString", bitString)
	lp_len := string2UnsignInt(bitString[0:15])
	//fmt.Println("lp_len", lp_len)
	//fmt.Println("after", reverse32BitStore(bitString))
	lp_flags := string2UnsignInt(bitString[15:17])
	//fmt.Println("lp_flags",lp_flags)
	lp_off := string2UnsignInt(bitString[17:32])
	//fmt.Println("lp_off",lp_off)
	if show {
		fmt.Printf("linp id : "+strconv.Itoa(int(id))+"\n"+
			"+------------linp---------+\n"+
			"| lp_off    : %10d  |\n"+
			"| lp_flag   : %10d  |\n"+
			"| lp_len    : %10d  |\n"+
			"+-------------------------+\n", lp_off, lp_flags, lp_len)
	}
	return linp_t{lp_len: lp_len, lp_flags: lp_flags, lp_off: lp_off}
}

func reverseString(s string) string {
	runes := []rune(s)
	for from, to := 0, len(runes)-1; from < to; from, to = from+1, to-1 {
		runes[from], runes[to] = runes[to], runes[from]
	}
	return string(runes)
}

func reverse32BitStore(s string) string {
	res := ""
	if len(s)%8 != 0 {
		panic(errors.New("字节存储异常"))
	}
	for i := len(s)/8 - 1; i >= 0; i-- {
		start := i * 8
		for j := start; j < start+8; j++ {
			res += string(s[j])
		}
	}
	return res
}

func string2UnsignInt(s string) uint {
	var res uint
	res = 0
	for i := 0; i < len(s); i++ {
		if s[i] == '1' {
			res = res*2 + 1
		} else {
			res = res*2 + 0
		}
	}
	return res
}

func (s *server) getTupleMeta(id int) {
	linp_t, ok := s.getSpecifiedLinp(id, false)
	if !ok {
		fmt.Println("invalid linp id")
		return
	}
	s.mapTupleData(id, linp_t.lp_off, linp_t.lp_len, true)
}

func (s *server) mapTupleData(id int, offset, length uint, show bool) {
	//想要解析tuple，需要表的模式信息
	heapTupleHeaderData := (*HeapTupleHeaderData)(unsafe.Pointer(uintptr(unsafe.Pointer(&s.data)) + uintptr(uintptr(offset))))
	//show heapTupleHeaderData
	fmt.Printf("tuple(linp) id : "+strconv.Itoa(int(id))+"\n"+
		"+----heapTupleHeaderData----+\n"+
		"| t_xmin      : %10d  |\n"+
		"| t_xmax      : %10d  |\n"+
		"| t_cid       : %10d  |\n"+
		"| t_ctid:hi   : %10d  |\n"+
		"| t_ctid:lo   : %10d  |\n"+
		"| t_ctid:posid: %10d  |\n"+
		"| t_infomask2 : %10d  |\n"+
		"| t_infomask  : %10d  |\n"+
		"| t_hoff      : %10d  |\n"+
		"+---------------------------+\n", heapTupleHeaderData.t_xmin, heapTupleHeaderData.t_xmax,
		heapTupleHeaderData.t_cid, heapTupleHeaderData.t_ctid.ip_blkid.bi_hi, heapTupleHeaderData.t_ctid.ip_blkid.bi_lo,
		heapTupleHeaderData.t_ctid.ip_posid, heapTupleHeaderData.t_infomask2,
		heapTupleHeaderData.t_infomask, heapTupleHeaderData.t_hoff)

	offset += uint(heapTupleHeaderData.t_hoff) //data offset
	//fmt.Println("attnum:" , s.attNum)
	tempVal := make([]string, 0)
	for i := 0; i < s.attNum; i++ {
		if s.attLen[i] == 4 {
			val := s.getInt32(offset)
			//fmt.Println(val)
			tempVal = append(tempVal, strconv.Itoa(int(val)))
			offset += 4
		}
		if s.attLen[i] == -1 {
			str, length := s.getVarlena(offset)
			//fmt.Println(str)
			tempVal = append(tempVal, str)
			offset += length
		}
	}

	//format the tuple show
	if show {
		// format the schema
		schema := ""
		tuple := ""
		for i := 0; i < s.attNum; i++ {
			nameLen := len(s.attName[i])
			varLen := len(tempVal[i])
			maxLen := findIntMax(nameLen, varLen) + 4
			leftPadding := (maxLen - len(s.attName[i])) / 2
			rightPadding := maxLen - len(s.attName[i]) - leftPadding

			//construct the schema string
			schema += "|"
			for j := 0; j < leftPadding; j++ {
				schema += " "
			}
			schema += s.attName[i]
			for j := 0; j < rightPadding; j++ {
				schema += " "
			}

			leftPadding = (maxLen - len(tempVal[i])) / 2
			rightPadding = maxLen - len(tempVal[i]) - leftPadding

			//construct the val string
			tuple += "|"
			for j := 0; j < leftPadding; j++ {
				tuple += " "
			}
			tuple += tempVal[i]
			for j := 0; j < rightPadding; j++ {
				tuple += " "
			}
		}
		schema += "|\n"
		tuple += "|\n"
		//fmt.Println(schema)
		//fmt.Println(tuple)
		stringLen := len(schema) - 1 - 5 - 1 - len(strconv.Itoa(int(id)))
		leftPadding := stringLen / 2
		rightPadding := stringLen - leftPadding
		upperString := ""
		for j := 0; j < leftPadding; j++ {
			if j == 0 {
				upperString += "+"
			} else {
				upperString += "-"
			}
		}
		upperString += "tuple:"
		upperString += strconv.Itoa(id)
		for j := 0; j < rightPadding; j++ {
			if j == leftPadding-1 {
				upperString += "+"
			} else {
				upperString += "-"
			}
		}
		upperString += "\n"

		downString := ""
		for j := 0; j < len(schema)-1; j++ {
			if j == 0 {
				downString += "+"
			} else if j == len(schema)-2 {
				downString += "+"
			} else {
				downString += "-"
			}
		}
		downString += "\n"

		fmt.Printf("tuple(linp) id : " + strconv.Itoa(int(id)) + "\n" +
			upperString + schema + tuple + downString)
	}

}

func findIntMax(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (s *server) getInt32(offset uint) int32 {
	int_val := (*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&s.data)) + uintptr(uintptr(offset))))
	//fmt.Println(*int_val)
	return *int_val
}

//todo:  adding more type analysis...
func (s *server) getInt16(offset uint) int16 {
	return -1
}

func (s *server) getVarlena(offset uint) (string, uint) {
	flag_val := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&s.data)) + uintptr(uintptr(offset))))
	// fmt.Println(*flag_val)
	// this is just a one kind of case used for varchar(10)
	// this varlena will store the len info in one byte in the data header
	// the rule is like this:   xxxxxxx1
	// x's represent the len of the varlena(include the flag byte), 1 represent is flag of this store type
	// also the store of varlena and flag will align the certain bytes, it will be specified in pg_attribute
	// so we need to analyse it's len first！！！
	flag := (*flag_val)
	len := flag >> 1
	//fmt.Println("len", len)
	offset += 1
	str := ""
	for i := uint(0); i < uint(len-1); i++ {
		ascii_val := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&s.data)) + uintptr(uintptr(offset))))
		//fmt.Print(string(*ascii_val))
		str += string(*ascii_val)
		offset += 1
	}

	//align the len
	i := uint(1)
	for !(((i-1)*4) < uint(len) && uint(len) <= (i*4)) {
		i++
	}
	return str, i * 4
}

//获取表列信息，需要进行
/*
select attname, attlen from pg_attribute where attrelid=table.oid orderby attnum offset 6;
这样是可以拿到所有的列，但是变长度类型，也就是attlen=-1的列，我们应该怎么解析呢？
remotepg中存储的时候，跟你定义的varchar的模式有关，我定义的varchar(10)，所以这个变长的data，用1个byte来
表示这部分data的长度。其中最后一位固定为1，表示用一个字节表示variable的长度。
*/
