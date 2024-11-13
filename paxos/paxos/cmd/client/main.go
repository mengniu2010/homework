package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"paxos/pkg/client"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %v server:port\n", os.Args[0])
		os.Exit(1)
	}
	cli, err := client.Init(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to connect server, %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	cmd_func := map[string]func(*client.Client) error{
		"echo":   cli_echo,
		"get":    cli_get,
		"set":    cli_set,
		"setnet": cli_setnet,
		"getnet": cli_getnet,
	}

	for {
		var cmd string

		fmt.Print("> ")

		_, err := fmt.Scan(&cmd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Fprintf(os.Stderr, "fail to get input, %v\n", err)
			continue
		}
		fn, ok := cmd_func[strings.ToLower(cmd)]
		if !ok {
			fmt.Fprintf(os.Stderr, "cmd not support\n")
			continue
		}
		if err := fn(cli); err != nil {
			fmt.Fprintf(os.Stderr, "failed, %v\n", err)

		}
	}
}

func cli_get(cli *client.Client) error {
	rep, err := cli.Get()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, *rep)
	return nil
}

func cli_set(cli *client.Client) error {
	var num int

	_, err := fmt.Scan(&num)
	if err != nil {
		return err
	}
	err = cli.Set(num)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "ok")
	return nil
}

func cli_echo(cli *client.Client) error {
	var str string
	_, err := fmt.Scan(&str)
	if err != nil {
		return err
	}
	rep, err := cli.Echo((str))
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, rep)
	return nil
}

func cli_setnet(cli *client.Client) error {
	loss := -1
	delaySec := -1
	_, err := fmt.Scan(&loss)
	if err != nil {
		return err
	}
	if loss > 100 {
		return fmt.Errorf("invalid loss, loss <= 100")
	}

	_, err = fmt.Scan(&delaySec)
	if err != nil {
		return err
	}
	rep, err := cli.SetMockNet(loss, delaySec)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "%+v\n", rep)
	return nil
}

func cli_getnet(cli *client.Client) error {
	rep, err := cli.GetMockNet()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "%+v\n", rep)
	return nil
}
