#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FIRST_GOPATH              := $(firstword $(subst :, ,$(GOPATH)))
PKGS                      := $(shell go list ./... | grep -v /tests | grep -v /xcpb | grep -v /gpb | grep -v /generated)
GOFILES_NOVENDOR          := $(shell find . -name vendor -prune -o -type f -name '*.go' -not -name '*.pb.go' -print)
GOFILES_BUILD             := $(shell find . -type f -name '*.go' -not -name '*_test.go')
PROTOFILES                := $(shell find . -name vendor -prune -o -type f -name '*.proto' -print)

ALLGOFILES				  			:= $(shell find . -type f -name '*.go' -not -name '*.pb.go')
DATE                      := $(shell date -u -d "@$(SOURCE_DATE_EPOCH)" '+%FT%T%z' 2>/dev/null || date -u '+%FT%T%z')

BUILDFLAGS_NOPIE		  :=
BUILDFLAGS                ?= $(BUILDFLAGS_NOPIE) -buildmode=pie
TESTFLAGS                 ?=
PWD                       := $(shell pwd)
PREFIX                    ?= $(GOPATH)
BINDIR                    ?= $(PREFIX)/bin
GO                        := go
GOOS                      ?= $(shell go version | cut -d' ' -f4 | cut -d'/' -f1)
GOARCH                    ?= $(shell go version | cut -d' ' -f4 | cut -d'/' -f2)
TAGS                      ?= netgo
SHELL = bash
GOFUMPT_SPLIT_LONG_LINES  := on

## Build tools
BUF                       := $(GO) run github.com/bufbuild/buf/cmd/buf@v1.26.1

BINARIES				  := cmd/spark-connect-example-spark-session cmd/spark-connect-example-raw-grpc-client

# Define the location of SPARK_HOME because we need that to depend on the build paths
MAKEFILE_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

PROTO_SRC = $(shell find internal/generated -type f -name *.proto )


OK := $(shell tput setaf 6; echo ' [OK]'; tput sgr0;)

all: build

build: $(BUILD_OUTPUT) $(BINARIES) internal/generated.out

cmd/spark-connect-example-raw-grpc-client: $(GOFILES_BUILD)
	@echo ">> BUILD, output = $@"
	@cd $@ && $(GO) build -o $(notdir $@) $(BUILDFLAGS)
	@printf '%s\n' '$(OK)'

cmd/spark-connect-example-spark-session: $(GOFILES_BUILD)
	@echo ">> BUILD, output = $@"
	@cd $@ && $(GO) build -o $(notdir $@) $(BUILDFLAGS)
	@printf '%s\n' '$(OK)'

internal/generated.out:
	@echo -n ">> BUILD, output = $@"
	$(BUF) generate --debug -vvv
	@touch internal/generated.out
	@printf '%s\n' '$(OK)'

gen: internal/generated.out

$(GOFILES_BUILD): gen

$(BUILD_OUTPUT): $(GOFILES_BUILD)
	@echo -n ">> BUILD, output = $@"
	@$(GO) build -o $@ $(BUILDFLAGS)
	@printf '%s\n' '$(OK)'

lint: $(BUILD_OUTPUT)
	@golangci-lint run

fmt:
	@echo -n ">> glongci-lint: fix"
	env GOFUMPT_SPLIT_LONG_LINES=$(GOFUMPT_SPLIT_LONG_LINES) golangci-lint run --fix

test: $(BUILD_OUTPUT)
	@echo ">> TEST, \"verbose\""
	@$(foreach pkg, $(PKGS),\
	    @echo -n "     ";\
		$(GO) test -v -run '(Test|Example)' $(BUILDFLAGS) $(TESTFLAGS) $(pkg) || exit 1)

coverage: $(BUILD_OUTPUT)
	@echo ">> TEST, \"coverage\""
	@$(GO) test -cover -coverprofile=coverage.out -covermode=atomic -coverpkg=./spark/...,./internal/tests/... ./spark/... ./internal/tests/...
	@$(GO) tool cover -html=coverage.out -o coverage.html

integration: $(BUILD_OUTPUT)
	@echo ">> TEST, \"integration\""
	@$(GO) test ./internal/tests/...

check:
	@echo -n ">> CHECK"
	./dev/check-license
	@echo -n ">> glongci-lint: "
	env GOFUMPT_SPLIT_LONG_LINES=$(GOFUMPT_SPLIT_LONG_LINES) golangci-lint run

clean:
	@echo -n ">> CLEAN"
	@$(GO) clean -i ./...
	@rm -rf ./internal/generated
	@rm  -f ./internal/generated.out
	@rm -f ./coverage-all.html
	@rm -f ./coverage-all.out
	@rm -f ./coverage.out
	@find . -type f -name "coverage.out" -delete
	@printf '%s\n' '$(OK)'
