package shared

import "encoding/json"
import "testing"

type jsonSafeErr struct {
	Err string
}

func (jse jsonSafeErr) Error() string {
	return jse.Err
}

type jsonUnsafeErr struct {
	Err string
	Fn  func()
}

func (jue jsonUnsafeErr) Error() string {
	return jue.Err
}

func TestJsonSafeError(t *testing.T) {
	e := jsonSafeErr{Err: "test"}
	je := JSONErr{E: e}
	j, err := json.Marshal(je)
	if err != nil {
		t.Error(err)
	}
	t.Log(j)

	var newje JSONErr
	err = json.Unmarshal(j, &newje)
	if err != nil {
		t.Error(err)
	}
	errmsg := newje.Error()
	if errmsg != "test" {
		t.Error("Expected 'test'", errmsg)
	}
}

func TestJsonUnsafeError(t *testing.T) {
	e := jsonUnsafeErr{Err: "test", Fn: func() {}}
	je := JSONErr{E: e}
	j, err := json.Marshal(je)
	if err != nil {
		t.Error(err)
	}
	t.Log(j)

	var newje JSONErr
	err = json.Unmarshal(j, &newje)
	if err != nil {
		t.Error(err)
	}
	errmsg := newje.Error()
	if errmsg != "test" {
		t.Error("Expected 'test'", errmsg)
	}
}
