package master

import (
	"testing"
)

var app *MasterApplication

func TestCreate(t *testing.T) {
	app = NewMasterApplication(true, "/Users/tt")
	if app == nil {
		t.Errorf("assert: app == nil")
	}
}
