package basicpaxos

import "errors"
 
 
var ErrNotAccepted = errors.New("not enough acceptor")
var ErrStaleProposal = errors.New("stale proposal")
var ErrTimeout= errors.New("timeout")
