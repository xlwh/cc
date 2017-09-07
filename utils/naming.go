//
// naming.go
// Copyright (C) 2017 yanming02 <yanming02@baidu.com>
//
// Distributed under terms of the MIT license.
//

package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

const namingUri = "http://bns.noah.baidu.com"

type NamingResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

//调用BNS的接口，解析BNS
func ResolvZkFromNaming(service string) (string, error) {
	resp, err := http.Get(namingUri + "/webfoot/index.php?r=webfoot/ApiInstanceInfo&serviceName=" + service)
	if resp.StatusCode != 200 {
		return "", err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	var namingResp NamingResponse

	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	d.Decode(&namingResp)
	if !namingResp.Success {
		return namingResp.Message, fmt.Errorf("resolve zk service failed")
	}

	zkAddrs := []string{}

	for _, v := range namingResp.Data.([]interface{}) {
		if v.(map[string]interface{})["status"] != "0" {
			continue
		}
		zkAddr := fmt.Sprintf("%s:%s", v.(map[string]interface{})["hostName"], v.(map[string]interface{})["port"])
		zkAddrs = append(zkAddrs, zkAddr)
	}
	return strings.Join(zkAddrs, ","), nil
}
