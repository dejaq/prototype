package protocol

//
//Err_Cluster_offline ErrorCode = 10
////Err_Cluster_minority ErrorCode = 11
//Err_Cluster_wrong        ErrorCode = 12
//Err_Cluster_minority     ErrorCode = 13
//Err_Cluster_noconnection ErrorCode = 14
//
//Err_Storage_default_error ErrorCode = 100
//Err_Storage_offline       ErrorCode = 101
//Err_Storage_noconnection  ErrorCode = 102
//Err_Storage_timeout       ErrorCode = 103
//Err_Storage_notsupported  ErrorCode = 104
//
//Err_Broker_offline             ErrorCode = 200
//Err_broker_offline_degradation ErrorCode = 201
//Err_broker_offline_minority    ErrorCode = 202
//Err_broker_timeout             ErrorCode = 203
//
//Err_Protocol_default_err ErrorCode = 300
//Err_Protocol_unknwon_cmd ErrorCode = 301
//
//Err_topic_default_err     = 400
//Err_topic_doesnotexists   = 401
//Err_topic_is_provisioning = 402
//var (
//	ErrTexts = map[ErrorCode]string{
//		Err_PRIME:                "Shutdown signal, everything must close",
//		Err_Cluster_offline:      "The cluster is offline",
//		Err_Cluster_wrong:        "Joined a different cluster cannot do the handshake",
//		Err_Storage_notsupported: " Unknown, not implemented or not supported command.",
//		Err_Protocol_unknwon_cmd: " Unknown, not implemented or not supported command.",
//
//		Err_topic_doesnotexists: "The topic does not exists",
//	}
//)

type Error interface {
	Error() string
	String() string
	Code() uint16
	Message() string
	Details() map[string]string
	ThrottledMs() uint16
	ShouldRetry() bool
	ShouldSync() bool
}
