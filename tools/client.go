// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tools

import (
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin"
)

func CreateAdminWithOauth2() pulsaradmin.Client {
	cfg := &pulsaradmin.Config{}
	cfg.AuthPlugin = "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2"
	cfg.IssuerEndpoint = "https://auth.streamnative.cloud/"
	cfg.ClientID = "client_credentials"
	cfg.Audience = "urn:sn:pulsar:o-5bm99:xc-poc"
	cfg.KeyFile = "file:///Users/shibaodi/GolandProjects/pulsar-test-go/tools/admin.json"
	cfg.WebServiceURL = "https://pc-77fa251c.aws-apse2-koala-snc.streamnative.aws.snio.cloud"

	admin, err := pulsaradmin.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	return admin
}

func CreateClientWithOauth2() pulsar.Client {
	return createClientWithOauth2Config(false)
}

func CreateClientWithOauth2WithTransaction() pulsar.Client {
	return createClientWithOauth2Config(true)
}

func createClientWithOauth2Config(enableTransaction bool) pulsar.Client {
	oauth := pulsar.NewAuthenticationOAuth2(map[string]string{
		"type":       "client_credentials",
		"issuerUrl":  "https://auth.streamnative.cloud/",
		"audience":   "urn:sn:pulsar:o-5bm99:xc-poc",
		"privateKey": "file:///Users/shibaodi/GolandProjects/pulsar-test-go/tools/admin.json",
	})

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar+ssl://pc-77fa251c.aws-apse2-koala-snc.streamnative.aws.snio.cloud:6651",
		Authentication:    oauth,
		EnableTransaction: enableTransaction,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func CreateClientWithLocal(serviceURL string) pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               serviceURL,
		EnableTransaction: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}
