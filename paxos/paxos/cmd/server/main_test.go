package main

import (
	"fmt"
	"math/rand"
	basicpaxos "paxos/pkg/basic_paxos"
	"paxos/pkg/client"
	"paxos/pkg/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsensusWithoutNetProblem(t *testing.T) {
	numNodes := 5
	randGen := rand.New(rand.NewSource(33))
	memberAddrs := make([]string, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		memberAddrs = append(memberAddrs, fmt.Sprintf("2222%d", i))
	}

	numNum := 100
	inputs := make([]int, 0, numNum)
	for i := 0; i < numNum; i++ {
		inputs = append(inputs, randGen.Int())
	}
	members := make([]*basicpaxos.Paxos, 0, numNodes)
	clients := make([]*client.Client, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		p := basicpaxos.Init(i, memberAddrs)
		assert.NotNil(t, p)
		members = append(members, p)
		go func(x int) {
			members[x].Start()
			members[x].Wait()
		}(i)
	}

	defer func() {
		for i := 0; i < numNodes; i++ {
			p := members[i]
			p.Stop()
		}
	}()

	// wait for all server start
	time.Sleep(time.Second * 3)

	for i := 0; i < numNodes; i++ {
		c, err := client.Init(memberAddrs[i])
		assert.Nil(t, err)
		assert.NotNil(t, c)
		clients = append(clients, c)
	}

	defer func() {
		for i := 0; i < numNodes; i++ {
			c := clients[i]
			c.Close()
		}
	}()

	checkpoint := func() {
		// proposals := make([]common.Proposal, 0, numNodes)
		var firstProposal common.Proposal
		for i := 0; i < numNodes; i++ {
			p := members[i]
			proposal, state := p.Get()
			assert.Equal(t, basicpaxos.STATE_LEARNT, state)
			if i == 0 {
				firstProposal = proposal
			} else {
				assert.Equal(t, firstProposal, proposal)
			}
		}
	}

	for _, x := range inputs {
		i := randGen.Int() % numNodes
		clients[i].Set(x)
		time.Sleep(time.Second)
		checkpoint()
	}

}
