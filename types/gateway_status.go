package types

type GatewayStatus struct {
	GtwId       string           `json:"gtw_id" db:"gtw_id"`
	Description string           `json:"description"`
	Location    LocationMetadata `json:"location"`
	LastSeen    JSONTime         `json:"last_seen"`
	Source      string           `json:"status_source"` //NOC or WEB
}
