package main

import (
	"fmt"
	"os"
	basicpaxos "paxos/pkg/basic_paxos"
	"regexp"
	"strconv"

	"github.com/zeromicro/go-zero/core/logx"
)

func setupLogx(memberID string) {
	logx.MustSetup(logx.LogConf{
		ServiceName: fmt.Sprintf("paxos.%v", memberID),
		Mode:        "file",
		Encoding:    "plain",
		Path:        fmt.Sprintf("paxos/paxos.%v/%v.log", memberID, os.Getpid()),
	})
	logx.AddWriter(logx.NewWriter(os.Stdout)) // Add console output
}

type PaxosArgs struct {
	MemberID   int
	Parliament []string
}

func parseArgs() *PaxosArgs {
	if len(os.Args) < 3 {
		fmt.Printf("usage: %v <member-id> <parliament-1> <parliament-2>...\n", os.Args[0])
		os.Exit(1)
	}
	memberID, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		fmt.Printf("fail to parse member-id %v", os.Args[1])
		os.Exit(1)
	}
	parliaments := make([]string, 0, len(os.Args)-2)
	memberReg := regexp.MustCompile(`(.*):(\d{1,5})`)
	for i := 2; i < len(os.Args); i++ {
		if !memberReg.MatchString(os.Args[i]) {
			fmt.Printf("fail to parse member %v", os.Args[i])
			os.Exit(1)
		}
		parliaments = append(parliaments, os.Args[i])
	}
	if int(memberID) > len(parliaments) {
		fmt.Println("member-id > len(parliaments)")
		os.Exit(1)
	}
	return &PaxosArgs{
		MemberID:   int(memberID),
		Parliament: parliaments,
	}
}

func main() {

	paxosArgs := parseArgs()
	fmt.Printf("Args: %+v\n", paxosArgs)

	setupLogx(fmt.Sprint(paxosArgs.MemberID))
	p1 := basicpaxos.Init(paxosArgs.MemberID, paxosArgs.Parliament)
	if p1 == nil {
		panic("cannot init")
	}
	p1.Start()

	p1.Wait()
	// TODO: DESTRUCT
}
