package datatypes

type PeerDBInterval struct {
	Hours   int     `json:"hours,omitempty"`
	Minutes int     `json:"minutes,omitempty"`
	Seconds float64 `json:"seconds,omitempty"`
	Days    int     `json:"days,omitempty"`
	Months  int     `json:"months,omitempty"`
	Years   int     `json:"years,omitempty"`
	Valid   bool    `json:"valid"`
}
