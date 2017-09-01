package api

const (
	AppInfoPath             = "/app/info"
	AppStatusPath           = "/app/status"
	RegionSnapshotPath      = "/region/snapshot" //接收regin controller发送过来的seeds和failInfo
	MergeSeedsPath          = "/region/mergeseeds"
	MigrateCreatePath       = "/migrate/create"
	MigratePausePath        = "/migrate/pause"
	MigrateResumePath       = "/migrate/resume"
	MigrateCancelPath       = "/migrate/cancel"
	MigrateRecoverPath      = "/migrate/recover"
	FetchMigrationTasksPath = "/migrate/tasks"
	RebalancePath           = "/migrate/rebalance"
	NodePermPath            = "/node/perm"
	NodeMeetPath            = "/node/meet"
	NodeForgetAndResetPath  = "/node/forgetAndReset"
	NodeReplicatePath       = "/node/replicate"
	NodeResetPath           = "/node/reset"
	NodeSetAsMasterPath     = "/node/setAsMaster"
	FetchReplicaSetsPath    = "/replicasets"
	MakeReplicaSetPath      = "/replicaset/make"
	FailoverTakeoverPath    = "/failover/takeover"
	FixClusterPath          = "/cluster/fix"
	LogSlicePath            = "/log/slice"
	UpdateTokenId           = "/update/tokenid"
)
