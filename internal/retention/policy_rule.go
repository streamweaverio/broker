package retention

type RetentionPolicyRule interface {
	Enforce() error
}
