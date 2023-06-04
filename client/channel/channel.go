//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package channel

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

// Reserved header parameters that must not be injected as variables.
var reservedParams = []string{"user_id", "token", "use_ssl"}

// The ChannelBuilder is used to parse the different parameters of the connection
// string according to the specification documented here:
//
//	https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md
type ChannelBuilder struct {
	Host    string
	Port    int
	Token   string
	User    string
	Headers map[string]string
}

// Finalizes the creation of the gprc.ClientConn by creating a GRPC channel
// with the necessary options extracted from the connection string. For
// TLS connections, this function will load the system certificates.
func (cb *ChannelBuilder) Build() (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithAuthority(cb.Host))
	if cb.Token == "" {
		opts = append(opts, grpc.WithInsecure())
	} else {
		// Note: On the Windows platform, use of x509.SystemCertPool() requires
		// go version 1.18 or higher.
		systemRoots, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		cred := credentials.NewTLS(&tls.Config{
			RootCAs: systemRoots,
		})
		opts = append(opts, grpc.WithTransportCredentials(cred))

		t := oauth2.Token{
			AccessToken: cb.Token,
			TokenType:   "bearer",
		}
		opts = append(opts, grpc.WithPerRPCCredentials(oauth.NewOauthAccess(&t)))
	}

	remote := fmt.Sprintf("%v:%v", cb.Host, cb.Port)
	conn, err := grpc.Dial(remote, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote %s: %w", remote, err)
	}
	return conn, nil
}

// Creates a new instance of the ChannelBuilder. This constructor effectively
// parses the connection string and extracts the relevant parameters directly.
func NewBuilder(connection string) (*ChannelBuilder, error) {

	u, err := url.Parse(connection)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "sc" {
		return nil, errors.New("URL schema must be set to `sc`.")
	}

	var port = 15002
	var host = u.Host
	// Check if the host part of the URL contains a port and extract.
	if strings.Contains(u.Host, ":") {
		hostStr, portStr, err := net.SplitHostPort(u.Host)
		if err != nil {
			return nil, err
		}
		host = hostStr
		if len(portStr) != 0 {
			port, err = strconv.Atoi(portStr)
			if err != nil {
				return nil, err
			}
		}
	}

	// Validate that the URL path is empty or follows the right format.
	if u.Path != "" && !strings.HasPrefix(u.Path, "/;") {
		return nil, fmt.Errorf("The URL path (%v) must be empty or have a proper parameter syntax.", u.Path)
	}

	cb := &ChannelBuilder{
		Host:    host,
		Port:    port,
		Headers: map[string]string{},
	}

	elements := strings.Split(u.Path, ";")
	for _, e := range elements {
		props := strings.Split(e, "=")
		if len(props) == 2 {
			if props[0] == "token" {
				cb.Token = props[1]
			} else if props[0] == "user_id" {
				cb.User = props[1]
			} else {
				cb.Headers[props[0]] = props[1]
			}
		}
	}
	return cb, nil
}
