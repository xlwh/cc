package command

import (
	"fmt"
	"strings"

	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/log"
	"github.com/ksarch-saas/cc/meta"
	"github.com/ksarch-saas/cc/redis"
	"github.com/ksarch-saas/cc/topo"
	"github.com/ksarch-saas/cc/state"
)

type ForgetAndResetNodeCommand struct {
	NodeId string
}

// 似乎，只有同时进行Forget和Reset才有意义，否则都是一个不一致的状态
func (self *ForgetAndResetNodeCommand) Execute(c *cc.Controller) (cc.Result, error) {
	cs := c.ClusterState
	target := cs.FindNode(self.NodeId)
	if target == nil {
		return nil, ErrNodeNotExist
	}
	if !target.Free == false {
		return nil, ErrNodeIsFree
	}
	if len(target.Ranges) > 0 {
		return nil, ErrNodeNotEmpty
	}
	var err error
	forgetCount := 0
	allForgetDone := true
	resChan := make(chan int, 2048)
	// 1. 所有节点发送Forget
	for _, ns := range cs.AllNodeStates() {
		if ns.Id() == target.Id {
			continue
		}

		go func(nodeState *state.NodeState, target *topo.Node) {
			targetId := target.Id
			node := ns.Node()
			ret := 1
			_, err = redis.ClusterForget(nodeState.Addr(), targetId)

			if !node.Fail && err != nil && !strings.HasPrefix(err.Error(), "ERR Unknown node") {
				allForgetDone = false
				log.Warningf(target.Addr(), "Forget node %s(%s) failed, %v", ns.Addr(), ns.Id(), err)
				ret = 0
			} else if !node.Fail && err != nil {
				//try again
				for try := redis.NUM_RETRY; try >= 0; try-- {
					_, err = redis.ClusterForget(nodeState.Addr(), target.Id)
					if err == nil {
						break
					}
				}
				//execute failed after retry
				if err != nil {
					allForgetDone = false
					log.Warningf(target.Addr(), "Forget node %s(%s) failed after retry, %v", ns.Addr(), ns.Id(), err)
					ret = 0
				}
			}
			
			if ret == 1 {
				log.Eventf(target.Addr(), "Forget by %s(%s).", nodeState.Addr(), nodeState.Id())
			}
			
			resChan <- ret
		}(ns, target)

	}

	for i := 0; i < len(cs.AllNodeStates())-1; i++ {
		res := <-resChan
		if res == 0 {
			allForgetDone = false
		} else {
			forgetCount++
		}
	}

	if !allForgetDone {
		return nil, fmt.Errorf("Not all forget done, only (%d/%d) success",
			forgetCount, len(cs.AllNodeStates())-1)
	}
	// 2. 重置
	if !target.Fail {
		_, err = redis.ClusterReset(target.Addr(), false)
		if err != nil {
			return nil, fmt.Errorf("Reset node %s(%s) failed, %v", target.Id, target.Addr(), err)
		}
		log.Eventf(target.Addr(), "Reset.")
	}

	// remove seed in leader contrller
	meta.RemoveSeed(target.Addr())
	return nil, nil
}
