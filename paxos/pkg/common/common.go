package common

type ClientArg struct {
	X int
}

type ClientRep struct {
	Proposal
	State string
}

type ProposalNumber int

type Proposal struct {
	NodeName string
	Number   ProposalNumber
	Value    int
}
