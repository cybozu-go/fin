package e2e

import (
	"fmt"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Run e2e tests using the Ginkgo runner.
func TestE2E(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)
	_, err := fmt.Fprintf(GinkgoWriter, "Starting fin suite\n")
	if err != nil {
		panic(err)
	}
	RunSpecs(t, "e2e suite")
}
