package basicpaxos

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/core/logx"
)

type RPCName string

const (
	PREPARE RPCName = "Paxos.Prepare"
	ACCEPT  RPCName = "Paxos.Accept"
	LEARN   RPCName = "Paxos.Learn"
)

type RPCArg struct {
	NodeName  string
	RequestID string
}

type Proposal struct {
	NodeName string
	Number   ProposalNumber
	Value    int
}

var NilProposal = Proposal{
	NodeName: "",
	Number:   0,
	Value:    0,
}

type PrepareArg struct {
	*RPCArg
	Proposal Proposal
}
type PrepareRep struct {
	*RPCArg
	Voted Proposal
}

type AcceptArg struct {
	*RPCArg
	Accepted Proposal
}
type AcceptRep struct{ *RPCArg }

type LearnArg struct{}
type LearnRep struct{}

type ProposalNumber int

type Paxos struct {
	MemberID   int
	Parliament []string
	Chosen     Proposal
	Voted      Proposal
	muVoted    sync.Mutex
	PVersion   ProposalNumber
	muPVersion sync.Mutex
}

func Init(memberID int, parliament []string) *Paxos {
	return &Paxos{
		MemberID:   memberID,
		Parliament: parliament,
		Chosen:     NilProposal,
		Voted:      NilProposal,
		muVoted:    sync.Mutex{},
	}
}

func (p *Paxos) Get() (int, error) {
	panic(errors.New("not implemented"))
}

func (p *Paxos) newProposal(val int) *Proposal {
	proposal := Proposal{
		NodeName: p.Parliament[p.MemberID],
		Number:   p.PVersion,
		Value:    val,
	}
	p.PVersion += ProposalNumber(len(p.Parliament))
	return &proposal
}

func (p *Paxos) Set(val int) error {
	requestID := uuid.NewString()
	logx.Infof("requestID:%s, setting val to %v", requestID, val)

	proposal := p.newProposal(val)
	err := p.propose(requestID, proposal)
	if err != nil {
		logx.Info("fail to propose %v, err %v", proposal, err)
		return err
	}
	err = p.petition(requestID, proposal)
	if err != nil {
		return err
	}
	// err = p.spread(n, val)
	// if err != nil {
	// 	return err
	// }
	return nil
}

func (p *Paxos) setVoted(proposal Proposal) (Proposal, bool) {
	p.muVoted.Lock()
	defer p.muVoted.Unlock()
	accept := false
	if p.Voted == NilProposal || proposal == p.Voted || p.Voted.Number < proposal.Number {
		p.Voted = proposal
		accept = true
	}
	return p.Voted, accept
}

func (p *Paxos) propose(requestID string, proposal *Proposal) error {
	logx.Infof("requestID:%s, proposing with number %v", requestID, *proposal)
	args := make([]PrepareArg, len(p.Parliament))
	replies := make([]PrepareRep, len(p.Parliament))
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		args[i].RequestID = requestID
		args[i].NodeName = p.Parliament[p.MemberID]
		args[i].Proposal = *proposal

		replies[i] = PrepareRep{}
	}
	ch := make(chan int, len(p.Parliament))
	defer close(ch)

	wg := sync.WaitGroup{}
	wg.Add(len(p.Parliament))
	stop := false
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		go func(memberID int) {
			var err error
			for err != nil && !stop {
				err = p.Call(p.Parliament[memberID], PREPARE, &args[memberID], &replies[memberID])
				if err == nil {
					ch <- memberID
					break
				}
				logx.Errorf("fail to call %v with arg %v, error %v, sleep.", PREPARE, args[memberID], err)
				time.Sleep(time.Second * 3)
			}
			wg.Done()
		}(i)
	}
	err := errors.New("not enough acceptor")
	go func() {
		cnt := 0
		for i := range p.Parliament {
			if i == p.MemberID {
				panic(errors.New("reply of prepare phase from self"))
			}
			rep := &replies[i]
			if rep.Voted.Number > proposal.Number {
				stop = true
				err = errors.New("stale proposal")

			} else if rep.Voted == *proposal {
				logx.Infof("reply %v from %v when prepare %v", *rep, p.MemberID, *proposal)
				cnt += 1
				if cnt >= ((len(p.Parliament) + 1) / 2) {
					stop = true
					err = nil
					break
				}
			}
		}
	}()
	wg.Wait()
	return err
}

func (p *Paxos) Prepare(arg *PrepareArg, rep *PrepareRep) error {
	voted, _ := p.setVoted(arg.Proposal)
	logx.Infof("call prepare arg: %v, rep: %v", *arg, voted)
	rep.NodeName = p.Parliament[p.MemberID]
	rep.Voted = voted
	return nil
}

func (p *Paxos) petition(requestID string, proposal *Proposal) error {
	logx.Infof("petition for %v", proposal)
	args := make([]AcceptArg, len(p.Parliament))
	replies := make([]AcceptRep, len(p.Parliament))
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		args[i] = AcceptArg{RPCArg: &RPCArg{
			NodeName:  proposal.NodeName,
			RequestID: requestID,
		}, Accepted: Proposal{
			NodeName: proposal.NodeName,
			Number:   proposal.Number,
			Value:    proposal.Value,
		}}
		replies[i] = AcceptRep{}
	}
	wg := sync.WaitGroup{}
	MAX_RETRY := 10
	ch := make(chan int, len(p.Parliament)-1)
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		func(memberID int) {
			cnt := 0
			var err error
			for cnt < MAX_RETRY && err != nil {
				err = p.Call(p.Parliament[i], ACCEPT, &args[i], &replies[i])
				if err == nil {
					ch <- memberID
					break
				}
				cnt += 1
				logx.Errorf("fail to call %v with arg %v, error %v, sleep.", ACCEPT, args[memberID], err)
				time.Sleep(3 * time.Second)
			}
			if cnt < MAX_RETRY {
				logx.Infof("petition proposal %v succed", args[memberID])
			}else{
				logx.Infof("petition proposal %v falled", args[memberID])
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return nil
}
func (p *Paxos) spread(n ProposalNumber, v int) error { return nil }

func (p *Paxos) Accept(arg *AcceptArg, rep *AcceptRep) error {
	return nil
}

func (p *Paxos) Learn(arg *LearnArg, rep *LearnRep) error {
	return nil
}

func (p *Paxos) Call(node string, methodName RPCName, arg interface{}, rep interface{}) error {
	return nil
}

func (p *Paxos) Broadcast(methodName string, arg interface{}, rep *interface{}) error {
	return nil
}
