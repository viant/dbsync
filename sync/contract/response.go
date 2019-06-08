package contract

//Response return response
type Response struct {
	JobID       string `json:",ommitempty"`
	Status      string
	Transferred int
	Error       string `json:",ommitempty"`
}


