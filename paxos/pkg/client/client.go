package client

import (
	"net/rpc"
	mycommon "paxos/pkg/common"
)

type Client struct {
	Server string
	client *rpc.Client
}

const (
	RPCSetVal     string = "Paxos.SetVal"
	RPCGetVal     string = "Paxos.GetVal"
	RPCEcho       = "Paxos.Echo"
)

func Init(serverAddr string) (*Client, error) {
	cli, err := rpc.DialHTTP("tcp", serverAddr)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Server: serverAddr,
		client: cli,
	}
	return client, nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) Set(x int) error {
	arg := mycommon.ClientArg{X: x}
	return c.client.Call(RPCSetVal, &arg, nil)
}

func (c *Client) Get() (*mycommon.ClientRep, error) {
	// arg:= 
	rep := mycommon.ClientRep{
		Proposal: mycommon.Proposal{
			NodeName: "",
			Number:   0,
			Value:    0,
		},
		State:    "",
	}
	err := c.client.Call(RPCGetVal, &mycommon.ClientArg{}, &rep)
	return &rep, err
}

func (c *Client) Echo(s string) (string, error) {
	var arg, rep string

	arg = s
	err := c.client.Call(RPCEcho, &arg, &rep)
	return rep, err
}
