//
// naming_test.go
// Copyright (C) 2017 yanming02 <yanming02@baidu.com>
//
// Distributed under terms of the MIT license.
//

package utils

import (
	"fmt"
	"testing"
)

func TestResolvZkFromNaming(t *testing.T) {
	resp, err := ResolvZkFromNaming("group.ksarch-r3-zookeeperback.osp.cn")
	fmt.Println(resp, err)
}
