package command

import (
	cc "github.com/ksarch-saas/cc/controller"
	"github.com/ksarch-saas/cc/meta"
)

type AppInfoCommand struct{}

//命令执行结果
type AppInfoResult struct {
	AppConfig *meta.AppConfig        //app信息
	Leader    *meta.ControllerConfig //leader信息
}

//从meta中读取信息返回
func (self *AppInfoCommand) Execute(c *cc.Controller) (cc.Result, error) {
	result := &AppInfoResult{
		AppConfig: meta.GetAppConfig(),
		Leader:    meta.ClusterLeaderConfig(),
	}
	return result, nil
}
