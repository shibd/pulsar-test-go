package main

import (
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"pulsar-test/tools"
	"testing"
)

func TestListTenants(t *testing.T) {
	admin := tools.CreateAdminWithOauth2()
	tenants, _ := admin.Tenants().List()
	t.Logf("All tenants: %v", tenants)
}

func TestListNamespaces(t *testing.T) {
	admin := tools.CreateAdminWithOauth2()
	namespaces, _ := admin.Namespaces().GetNamespaces("public")
	t.Logf("All namespaces: %v", namespaces)
}

func TestCreateNamespaces(t *testing.T) {
	admin := tools.CreateAdminWithOauth2()
	admin.Namespaces().CreateNamespace("public/dev")
}

func TestCreateTopic(t *testing.T) {
	admin := tools.CreateAdminWithOauth2()
	topic, _ := utils.GetTopicName("public/dev/topic")
	admin.Topics().Create(*topic, 3)
}
