package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProd_Defaults(t *testing.T) {
	got := Spec{Stage: StageProd}.FQDN()
	assert.Equal(t, MainDomain, *got)
}

func TestDev_MustPrefix(t *testing.T) {
	// Panic if no DevPrefix for dev
	assert.Panics(t, func() { _ = Spec{Stage: StageDev}.FQDN() })
	// OK when DevPrefix provided
	got := Spec{Stage: StageDev, DevPrefix: "dev1"}.FQDN()
	assert.Equal(t, "dev1.infra.truf.network", *got)
}

func TestSubdomainCombos(t *testing.T) {
	// Sub before prefix
	got := Spec{Stage: StageDev, DevPrefix: "qa", Sub: "api"}.FQDN()
	assert.Equal(t, "api.qa.infra.truf.network", *got)
}
