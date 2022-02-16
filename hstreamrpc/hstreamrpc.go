package hstreamrpc

type ReqType uint16

const (
	CreateStream ReqType = 1 + iota
	DeleteStream
	ListStreams
	LookupStream
	Append

	CreateSubscription ReqType = 256 + iota
	ListSubscriptions
	CheckSubscriptionExist
	DeleteSubscription
	LookupSubscription
	LookupSubscriptionWithOrderingKey
	WatchSubscription
	StreamingFetch
)

func (t ReqType) String() string {
	switch t {
	case CreateStream:
		return "CreateStream"
	case DeleteStream:
		return "DeleteStream"
	case ListStreams:
		return "ListStreams"
	case LookupStream:
		return "LookupStream"
	case Append:
		return "Append"
	case CreateSubscription:
		return "CreateSubscription"
	case ListSubscriptions:
		return "ListSubscriptions"
	case CheckSubscriptionExist:
		return "CheckSubscriptionExist"
	case DeleteSubscription:
		return "DeleteSubscription"
	case LookupSubscription:
		return "LookupSubscription"
	case LookupSubscriptionWithOrderingKey:
		return "LookupSubscriptionWithOrderingKey"
	case WatchSubscription:
		return "WatchSubscription"
	case StreamingFetch:
		return "StreamingFetch"
	}
	return "Unknown"
}

type Request struct {
	Type ReqType
	Req  interface{}
}

type Response struct {
	Resp interface{}
}
