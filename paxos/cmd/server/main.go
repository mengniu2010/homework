package main

import (
	"fmt"
	"os"
	basicpaxos "paxos/pkg/basic_paxos"

	"github.com/zeromicro/go-zero/core/logx"
)

func setupLogx(procIdent string) {
	logx.MustSetup(logx.LogConf{
		ServiceName: fmt.Sprintf("paxos.%v", procIdent),
		Mode:        "file",
		Encoding:    "plain",
		Path:        fmt.Sprintf("%v-log", procIdent),
	})
	// logx.AddWriter(logx.NewWriter(os.Stdout)) // Add console output

}

func main() {
	// gspt.SetProcTitle("paxos")

	PROC_IDENT := "PROC_IDENT"
	procIdent := os.Getenv(PROC_IDENT)
	if procIdent == "" {
		procIdent = "paxos"
	}
	setupLogx(procIdent)
	p1 := basicpaxos.Init(0, []string{"localhost:22222"})
	if p1 == nil {
		panic("cannot init")
	}
	p1.Start()


	p1.Wait()
}
