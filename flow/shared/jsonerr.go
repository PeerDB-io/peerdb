package shared

import "encoding/json"

type ErrString struct {
	Message string
}

func (e ErrString) Error() string {
	return e.Message
}

type JSONErr struct {
	E error
}

func (je JSONErr) Error() string {
	return je.E.Error()
}

func (je JSONErr) MarshalJSON() ([]byte, error) {
	if jm, ok := je.E.(json.Marshaler); ok {
		return jm.MarshalJSON()
	} else {
		return json.Marshal(je.E.Error())
	}
}

func (je *JSONErr) UnmarshalJSON(data []byte) error {
	var res error
	err := json.Unmarshal(data, &res)
	if err == nil {
		*je = JSONErr{E: res}
		return nil
	}
	var msg string
	err = json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	*je = JSONErr{ErrString{Message: msg}}
	return nil
}
