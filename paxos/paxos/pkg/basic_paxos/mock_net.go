package basicpaxos

import (
	"errors"
	"math/rand"
	"time"
)

var ErrMockTimeout = errors.New("mock timeout")
var LossTotal = 100

type NetworkConfig struct {
	//loss: percent of loss
	Loss  int
	Delay time.Duration
}

var mockNetConfig = NetworkConfig{
	Loss:  0,
	Delay: 0,
}
var randGen = rand.New(rand.NewSource(100))

func mockNetworkProblem(mockConfig NetworkConfig) error {
	prob := (randGen.Int() % (LossTotal)) + 1
	if prob <= mockConfig.Loss {
		return ErrMockTimeout
	}
	randSec := randGen.Int() % (int(mockConfig.Delay) + 1)
	delay := time.Duration(randSec)
	time.Sleep(delay)
	return nil
}
