package common

type ClientArg struct {
	X int
}

type ClientRep struct {
	Proposal
	State string
}

// -1 means not set
type NetConfig struct {
	Loss     int
	DelaySec int
}

type ProposalNumber int

type Proposal struct {
	NodeName string
	Number   ProposalNumber
	Value    int
}
