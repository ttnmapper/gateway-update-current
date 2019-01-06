package types

import "time"

type MysqlGatewayStatus struct {
	Id          int       `db:"id"`
	GtwId       string    `db:"gtw_id"`
	Description string    `db:"description"`
	LastSeen    time.Time `db:"last_seen"`

	Latitude  float32 `db:"latitude"`
	Longitude float32 `db:"longitude"`
	Altitude  int32   `db:"altitude"`
	Accuracy  int32   `db:"location_accuracy"`
	Source    string  `db:"location_source"`
}
