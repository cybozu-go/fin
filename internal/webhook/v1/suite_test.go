package v1

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWebhookV1(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Webhook v1 Suite")
}
