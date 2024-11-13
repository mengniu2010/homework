package basicpaxos

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	mycommon "paxos/pkg/common"

	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/core/logx"
)

var MAX_RETRY = 10
var RETRY_BACKOFF = 1 * time.Second
var TIMEOUT = 5 * time.Second

var NilProposal = mycommon.Proposal{
	NodeName: "",
	Number:   0,
	Value:    0,
}

type RPCName string

const (
	PREPARE RPCName = "Paxos.Prepare"
	ACCEPT  RPCName = "Paxos.Accept"
	LEARN   RPCName = "Paxos.Learn"
)
const (
	STATE_LEARNT    string = "LEARNT"
	STATE_PROPOSING string = "PROPOSING"
	STATE_ACCEPTING string = "ACCEPTED"
)

type State struct {
	*sync.RWMutex
	State string
}

func (s *State) Set(target string) {
	s.Lock()
	defer s.Unlock()
	s.State = target
}

func (s *State) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.State
}

type RPCArg struct {
	NodeName  string
	RequestID string
}

type PrepareArg struct {
	RPCArg
	Proposal mycommon.Proposal
}
type PrepareRep struct {
	RPCArg
	Voted mycommon.Proposal
}

type AcceptArg struct {
	RPCArg
	Accepted mycommon.Proposal
}
type AcceptRep struct{ RPCArg }

type LearnArg struct{}
type LearnRep struct{}

type Paxos struct {
	MemberID    int
	Parliament  []string
	Chosen      mycommon.Proposal
	muChosen    sync.Mutex
	Voted       mycommon.Proposal
	muVoted     sync.Mutex
	PVersion    mycommon.ProposalNumber
	muPVersion  sync.Mutex
	rpcListener *net.Listener
	wgServer    sync.WaitGroup
	State       *State
	MockNet     *mycommon.NetConfig
}

func Init(memberID int, parliament []string) *Paxos {
	return &Paxos{
		MemberID:    memberID,
		Parliament:  parliament,
		Chosen:      NilProposal,
		muChosen:    sync.Mutex{},
		Voted:       NilProposal,
		muVoted:     sync.Mutex{},
		PVersion:    mycommon.ProposalNumber(memberID),
		muPVersion:  sync.Mutex{},
		rpcListener: nil,
		wgServer:    sync.WaitGroup{},
		State:       &State{RWMutex: &sync.RWMutex{}, State: STATE_LEARNT},
		MockNet: &mycommon.NetConfig{
			Loss:     0,
			DelaySec: 0,
		},
	}
}

func (p *Paxos) newProposal(val int) *mycommon.Proposal {
	p.muPVersion.Lock()
	defer p.muPVersion.Unlock()
	proposal := mycommon.Proposal{
		NodeName: p.Parliament[p.MemberID],
		Number:   p.PVersion,
		Value:    val,
	}
	p.PVersion += mycommon.ProposalNumber(len(p.Parliament))
	return &proposal
}

func (p *Paxos) Get() (mycommon.Proposal, string) {
	requestID := uuid.NewString()
	p.muChosen.Lock()
	defer p.muChosen.Unlock()
	state := p.State.Get()
	logx.Infof("requestID:%s, getting val %v, state %v ", requestID, p.Chosen, state)

	return mycommon.Proposal{
		NodeName: p.Chosen.NodeName,
		Number:   p.Chosen.Number,
		Value:    p.Chosen.Value,
	}, state
}

func (p *Paxos) Set(val int) error {
	requestID := uuid.NewString()
	logx.Infof("requestID:%s, setting val to %v", requestID, val)
	p.State.Set(STATE_PROPOSING)
	proposal := p.newProposal(val)
	err := p.propose(requestID, proposal)
	if err != nil {
		p.State.Set(STATE_LEARNT)
		logx.Infof("fail to propose %v, err %v", proposal, err)
		return err
	}
	p.State.Set(STATE_ACCEPTING)
	err = p.petition(requestID, proposal)
	if err != nil {
		p.State.Set(STATE_LEARNT)
		return err
	}
	p.State.Set(STATE_LEARNT)

	return nil
}

func (p *Paxos) setVoted(proposal mycommon.Proposal) (mycommon.Proposal, bool) {
	p.muVoted.Lock()
	defer p.muVoted.Unlock()
	accept := false
	if p.Voted == NilProposal || proposal == p.Voted || p.Voted.Number < proposal.Number {
		p.Voted = proposal
		accept = true
	}
	return p.Voted, accept
}

func (p *Paxos) propose(requestID string, proposal *mycommon.Proposal) error {
	logx.Infof("requestID:%s, proposing with number %v", requestID, *proposal)
	args := make([]PrepareArg, len(p.Parliament))
	replies := make([]PrepareRep, len(p.Parliament))
	for i := range p.Parliament {
		args[i] = PrepareArg{
			RPCArg:   RPCArg{},
			Proposal: NilProposal,
		}
		args[i].RequestID = requestID
		args[i].NodeName = p.Parliament[p.MemberID]
		args[i].Proposal = *proposal

		replies[i] = PrepareRep{
			RPCArg: RPCArg{},
			Voted:  mycommon.Proposal{},
		}
	}
	nCalls := len(p.Parliament) - 1
	ch := make(chan int, nCalls)
	defer close(ch)
	voted, accept := p.setVoted(args[p.MemberID].Proposal)
	if !accept {
		logx.Infof("fail to vote self proposal %v, abort proposal", voted)
		return ErrStaleProposal
	}
	wg := sync.WaitGroup{}
	wg.Add(nCalls)
	stop := false
	defer func() { stop = true }()
	// func foo()<-error{
	// }
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		go func(memberID int) {
			logx.Infof("requestID:%s, proposing with number %v to %v",
				requestID, *proposal, p.Parliament[memberID])
			defer func() {
				if r := recover(); r != nil && r.(error) != nil {
					logx.Errorf("error %+v in proposing %+v, %+v", r.(error), memberID, &args[memberID])
				}
			}()
			defer func() {
				ch <- memberID
				wg.Done()
			}()

			err := ErrTimeout
			cnt := 0
			for err != nil && !stop && cnt < MAX_RETRY {
				err = p.Call(memberID, PREPARE, &args[memberID], &replies[memberID])
				if err == nil {
					return
				}
				cnt += 1
				logx.Errorf("fail to call %v with arg %v, error %v, sleep.", PREPARE, args[memberID], err)
				time.Sleep(RETRY_BACKOFF)
			}
			logx.Errorf("fail to call %v with arg %v, error %v, stop retry.", PREPARE, args[memberID], err)
		}(i)
	}
	cnt := 1
	collected := false
	err := ErrNotAccepted
	for !collected || !stop {
		collected = true
		if cnt >= ((len(p.Parliament) + 1) / 2) {
			stop = true
			err = nil
			break
		}
		select {
		case <-time.After(time.Second * 60):
			stop = true
			err = ErrTimeout
		case i := <-ch:

			if i == p.MemberID {
				panic(errors.New("reply of prepare phase from self"))
			}

			rep := &replies[i]
			if rep.Voted.Number > proposal.Number {
				stop = true
				err = ErrStaleProposal
			} else if rep.Voted == *proposal {
				logx.Infof("reply %v from %v when prepare %v", *rep, p.MemberID, *proposal)
				cnt += 1
			}
		}
	}
	wg.Wait()
	return err
}

func (p *Paxos) Prepare(arg *PrepareArg, rep *PrepareRep) error {
	logx.Infof("call prepare arg: %v", *arg)

	if err := mockNetworkProblem(mockNetConfig); err != nil {
		logx.Infof("mock net problem occurred, config: %+v\n", mockNetConfig)
		return err
	}
	voted, _ := p.setVoted(arg.Proposal)
	logx.Infof("prepare arg: %v, rep: %v", *arg, voted)
	rep.NodeName = p.Parliament[p.MemberID]
	rep.RequestID = arg.RequestID
	rep.Voted = voted
	return nil
}

func (p *Paxos) petition(requestID string, proposal *mycommon.Proposal) error {
	logx.Infof("petition for %+v", proposal)
	p.setChosen(proposal)
	args := make([]AcceptArg, len(p.Parliament))
	replies := make([]AcceptRep, len(p.Parliament))
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		args[i] = AcceptArg{RPCArg: RPCArg{
			NodeName:  proposal.NodeName,
			RequestID: requestID,
		}, Accepted: mycommon.Proposal{
			NodeName: proposal.NodeName,
			Number:   proposal.Number,
			Value:    proposal.Value,
		}}
		replies[i] = AcceptRep{
			RPCArg: RPCArg{},
		}
	}
	wg := sync.WaitGroup{}
	nCalls := len(p.Parliament) - 1
	wg.Add(nCalls)
	ch := make(chan int, nCalls)
	for i := range p.Parliament {
		if i == p.MemberID {
			continue
		}
		stop := false
		func(memberID int) {
			cnt := 0
			err := ErrTimeout
			for cnt < MAX_RETRY && err != nil && !stop {
				err = p.Call(memberID, ACCEPT, &args[i], &replies[i])
				if err == nil {
					ch <- memberID
					logx.Infof("call accept succeed, arg: %v", args[i])
					break
				}
				if errors.Is(err, ErrStaleProposal) {
					stop = true
					logx.Errorf("stale proposal %v", args[i])
				}
				cnt += 1
				logx.Errorf("fail to call %v with arg %v, error %v, sleep.", ACCEPT, args[memberID], err)
				time.Sleep(RETRY_BACKOFF)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	return nil
}

func (p *Paxos) Accept(arg *AcceptArg, rep *AcceptRep) error {
	logx.Infof("accepting proposal %+v", *arg)
	if err := mockNetworkProblem(mockNetConfig); err != nil {
		logx.Infof("mock net problem occurred, config: %+v\n", mockNetConfig)
		return err
	}
	p.setChosen(&arg.Accepted)
	rep.NodeName = p.Parliament[p.MemberID]
	return nil
}

func (p *Paxos) setChosen(proposal *mycommon.Proposal) error {
	p.muChosen.Lock()
	defer p.muChosen.Unlock()
	if proposal.Number < p.Chosen.Number {
		logx.Errorf("setting chosen with stale proposal, chosen: %v, stale proposal: %v",
			p.Chosen, proposal)
		panic(ErrStaleProposal)
	}
	p.Chosen = *proposal
	p.muPVersion.Lock()
	defer p.muPVersion.Unlock()
	p.PVersion = mycommon.ProposalNumber((int(p.Chosen.Number)/len(p.Parliament) + 1) * (len(p.Parliament)))
	return nil
}

func (p *Paxos) RpcServe() error {
	logx.Infof("start serving http rpc at %v", p.Parliament[p.MemberID])
	rpc.Register(p)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", p.Parliament[p.MemberID])
	if err != nil {
		logx.Errorf("fail to listen %v, err %v", p.Parliament[p.MemberID], err)
		return err
	}
	p.rpcListener = &listener
	return http.Serve(listener, nil)
}

func (p *Paxos) RpcServeStop() {
	logx.Infof("stopping serving http rpc")
	(*(p.rpcListener)).Close()
	p.wgServer.Done()
}

func (p *Paxos) Call(memberID int, methodName RPCName, argI interface{}, repI interface{}) error {
	if err := mockNetworkProblem(mockNetConfig); err != nil {
		logx.Infof("mock net problem occurred, config: %+v\n", mockNetConfig)
		return err
	}
	client, err := rpc.DialHTTP("tcp", p.Parliament[memberID])
	if err != nil {
		logx.Errorf("fail to new rpc client for member %v, err %v", p.Parliament[memberID], err)
		return fmt.Errorf("fail to new rpc client %w", err)
	}
	defer client.Close()
	if methodName == PREPARE {
		p.callPrepare(client, PREPARE, argI, repI)
	} else if methodName == ACCEPT {
		p.callAccept(client, ACCEPT, argI, repI)
	}
	return nil
}

func (p *Paxos) callPrepare(client *rpc.Client, methodName RPCName, argI interface{}, repI interface{}) error {
	arg, ok := argI.(*PrepareArg)
	if !ok {
		panic(fmt.Errorf("invalid %v arg", ACCEPT))
	}
	rep, ok := repI.(*PrepareRep)
	if !ok {
		panic(fmt.Errorf("invalid %v rep", ACCEPT))

	}
	return client.Call(string(methodName), arg, rep)
}

func (p *Paxos) callAccept(client *rpc.Client, methodName RPCName, argI interface{}, repI interface{}) error {
	arg, ok := argI.(*AcceptArg)
	if !ok {
		panic(fmt.Errorf("invalid %v arg", ACCEPT))
	}
	rep, ok := repI.(*AcceptRep)
	if !ok {
		panic(fmt.Errorf("invalid %v rep", ACCEPT))

	}
	return client.Call(string(methodName), arg, rep)
}

func (p *Paxos) Start() {
	p.wgServer.Add(1)
	go func() {
		if err := p.RpcServe(); err != nil {
			panic(err)
		}
	}()
}

func (p *Paxos) Stop() {
	p.RpcServeStop()

}

func (p *Paxos) Wait() {
	p.wgServer.Wait()
}
func (p *Paxos) Echo(arg, rep *string) error {
	*rep = *arg
	return nil
}

func (p *Paxos) SprintGet() string {
	proposal, s := p.Get()
	return fmt.Sprintf("p: %v, chosen: %+v, state %v", p.Parliament[p.MemberID], proposal, s)
}

func (p *Paxos) SetVal(arg *mycommon.ClientArg, rep *mycommon.ClientArg) error {
	print("set val ", arg.X, "\n")
	return p.Set(arg.X)
}

func (p *Paxos) GetVal(arg *mycommon.ClientArg, rep *mycommon.ClientRep) error {
	proposal, state := p.Get()
	rep.State = state
	rep.NodeName = proposal.NodeName
	rep.Number = proposal.Number
	rep.Value = proposal.Value
	return nil
}

func (p *Paxos) SetNet(arg *mycommon.NetConfig, rep *mycommon.NetConfig) error {
	if arg.Loss >= 0 {
		mockNetConfig.Loss = arg.Loss
	}
	if arg.DelaySec >= 0 {
		mockNetConfig.Delay = time.Duration(arg.DelaySec) * time.Second
	}

	rep.Loss = mockNetConfig.Loss
	rep.DelaySec = int(mockNetConfig.Delay / time.Second)
	return nil
}
