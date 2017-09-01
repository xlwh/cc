package main

import (
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/GeertJohan/go.linenoise"
	"github.com/codegangsta/cli"

	c "github.com/ksarch-saas/cc/cli/command"
	"github.com/ksarch-saas/cc/cli/command/initialize"
	"github.com/ksarch-saas/cc/cli/context"
)

//可以支持执行cli命令
var cmds = []cli.Command{
	c.ChmodCommand,
	c.FailoverCommand,
	c.TakeoverCommand,
	c.MigrateCommand,
	c.MigrateRecoverCommand,
	c.ReplicateCommand,
	c.RebalanceCommand,
	c.MeetCommand,
	c.ForgetAndResetCommand,
	c.AppInfoCommand,
	c.ShowCommand,
	c.LogCommand,
	c.AppDelCommand,
	c.AppModCommand,
	c.WebCommand,
	c.TaskCommand,
	c.RedisCliCommand,
	c.Slot2NodeCommand,
	c.AlterRegionCommand,
	c.UpgradeServerCommand,
	c.FixHandshakeCommand,
	initialize.MakeReplicaCommand,
	initialize.AddRegionCommand,
	c.CheckReplicaCommand,
	c.FixClusterCommand,
	c.PdoCommand,
	c.DisableduplicatedCommand,
	c.GenerateRdbPathCommand,
}

const (
	DEFAULT_HISTORY_FILE = "/.cc_cli_history"
)

var cmdmap = map[string]cli.Command{}

//初始化
func init() {
	for _, cmd := range cmds {
		cmdmap[cmd.Name] = cmd
	}
}

func showHelp() {
	fmt.Println("List of commands:")
	for _, cmd := range cmds {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
}

func main() {
	//load config
	//执行命令的用户
	user, err := user.Current()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	conf, err := context.LoadConfig(user.HomeDir + context.DEFAULT_CONFIG_FILE)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	context.SetConfigContext(conf)

	if len(os.Args) > 1 {
		app := cli.NewApp()
		app.Name = "cli"
		app.Usage = "cluster manage tool"

		//cli工具支持的命令
		app.Commands = []cli.Command{
			initialize.Command,      //初始化服务
			initialize.Command_node, //初始化node
			c.AppAddCommand,         //新建app
			c.AppDelCommand,         //删除app
			c.AppModCommand,         //读写修改
			c.AppListCommand,        //查询所有的app
			c.ConfigCommand,
			c.UserAddCommand,            //新增用户
			c.UserDelCommand,            //删除用户
			c.UserGetCommand,            //查询用户
			c.ListFailoverRecordCommand, //查询故障迁移list
			c.GetFailoverRecordCommand,
			c.TopoCommand,            //查询拓扑信息
			c.TakeoverClusterCommand, //执行集群故障迁移
		}
		arg := append(os.Args)
		for _, cmd := range app.Commands {
			if os.Args[1] == cmd.Name {
				app.Run(arg)
				os.Exit(0)
			}
		}
	}
	if (len(os.Args) == 2 && (string(os.Args[1]) == "-h" || string(os.Args[1]) == "--help")) || (len(os.Args) == 1) {
		help := `Usage:
        cli init [options], -h for more details
        cli assign slot_range, -h for more details
        cli config -k <key> -v <value>, -h for more details
        cli appadd [options], -h for more details
        cli appdel [options], -h for more details
        cli appmod [options], -h for more details
        cli applist, -h for more details
        cli useradd [options], -h for more details
        cli userdel -u <username>, -h  for more details
        cli listfailover, -h for more details
        cli getfailover, -h for more details
        cli topo, -h for more details
        cli takeoverCluster, -h for more details
        cli <AppName> [<Command>] [options], -h for more details
        `
		fmt.Println(help)
		os.Exit(1)
	}

	// Set context
	appName := os.Args[1]
	err = context.SetApp(appName, conf.Zkhosts)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if conf.HistoryFile == "" {
		conf.HistoryFile = user.HomeDir + DEFAULT_HISTORY_FILE
	}
	_, err = os.Stat(conf.HistoryFile)
	if err != nil && os.IsNotExist(err) {
		_, err = os.Create(conf.HistoryFile)
		if err != nil {
			fmt.Println(conf.HistoryFile + "create failed")
		}
	}

	// REPL
	if len(os.Args) == 2 {
		err = linenoise.LoadHistory(conf.HistoryFile)
		if err != nil {
			fmt.Println(err)
		}
		for {
			str, err := linenoise.Line(appName + "> ")
			if err != nil {
				if err == linenoise.KillSignalError {
					os.Exit(1)
				}
				fmt.Printf("Unexpected error: %s\n", err)
				os.Exit(1)
			}
			fields := strings.Fields(str)

			linenoise.AddHistory(str)
			err = linenoise.SaveHistory(conf.HistoryFile)
			if err != nil {
				fmt.Println(err)
			}

			if len(fields) == 0 {
				continue
			}

			switch fields[0] {
			case "help":
				showHelp()
				continue
			case "quit":
				os.Exit(0)
			}

			cmd, ok := cmdmap[fields[0]]
			if !ok {
				fmt.Println("Error: unknown command.")
			}
			app := cli.NewApp()
			app.Name = cmd.Name
			app.Commands = []cli.Command{cmd}
			app.Run(append(os.Args[:1], fields...))
		}
	}

	// Command line
	if len(os.Args) > 2 {
		app := cli.NewApp()
		app.Name = "cli"
		app.Usage = "redis cluster cli"
		app.Commands = cmds
		app.Run(append(os.Args[:1], os.Args[2:]...))
	}
}
