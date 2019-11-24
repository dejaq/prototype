package protocol

type WriteConsistency uint8

const (
	WriteConsistency_Master     WriteConsistency = iota
	WriteConsistency_Quorum
	WriteConsistency_AllReplicas
	WriteConsistency_FireForget
)

type HydrationStatus uint8

const (
	Hydration_None HydrationStatus = iota
	Hydration_Requested
	Hydration_InProgress
	Hydration_Done
)

type TopicProvisioningStatus uint8

const (
	TopicProvisioningStatus_Creating TopicProvisioningStatus = iota
	TopicProvisioningStatus_Live
	TopicProvisioningStatus_Deleting
)
