package protocol

type WriteConsistency uint8
const (
	WriteConsistency_Master = iota
	WriteConsistency_Quorum = 1
	WriteConsistency_AllReplicas = 2
	WriteConsistency_FireForget = 3
)

type TopicProvisioningStatus uint8
const (
	TopicProvisioningStatus_Creating = iota
	TopicProvisioningStatus_Live = 2
	TopicProvisioningStatus_Deleting= 3
)
