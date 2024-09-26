// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import proto "github.com/apache/spark-connect-go/v35/internal/generated"

type ExplainMode int

const (
	ExplainModeSimple    ExplainMode = iota
	ExplainModeExtended  ExplainMode = iota
	ExplainModeCodegen   ExplainMode = iota
	ExplainModeCost      ExplainMode = iota
	ExplainModeFormatted ExplainMode = iota
)

type StorageLevel int

const (
	StorageLevelDiskOnly          StorageLevel = iota
	StorageLevelDiskOnly2         StorageLevel = iota
	StorageLevelDiskOnly3         StorageLevel = iota
	StorageLevelMemoryAndDisk     StorageLevel = iota
	StorageLevelMemoryAndDisk2    StorageLevel = iota
	StorageLevelMemoryOnly        StorageLevel = iota
	StorageLevelMemoryOnly2       StorageLevel = iota
	StorageLevelMemoyAndDiskDeser StorageLevel = iota
	StorageLevelNone              StorageLevel = iota
	StorageLevelOffHeap           StorageLevel = iota
)

func ToProtoStorageLevel(level StorageLevel) *proto.StorageLevel {
	switch level {
	case StorageLevelDiskOnly:
		return &proto.StorageLevel{UseDisk: true, UseMemory: false, Replication: 1}
	case StorageLevelDiskOnly2:
		return &proto.StorageLevel{UseDisk: true, UseMemory: false, Replication: 2}
	case StorageLevelDiskOnly3:
		return &proto.StorageLevel{UseDisk: true, UseMemory: false, Replication: 3}
	case StorageLevelMemoryAndDisk:
		return &proto.StorageLevel{UseDisk: true, UseMemory: true, Replication: 1}
	case StorageLevelMemoryAndDisk2:
		return &proto.StorageLevel{UseDisk: true, UseMemory: true, Replication: 2}
	case StorageLevelMemoryOnly:
		return &proto.StorageLevel{UseDisk: false, UseMemory: true, Replication: 1}
	case StorageLevelMemoryOnly2:
		return &proto.StorageLevel{UseDisk: false, UseMemory: true, Replication: 2}
	case StorageLevelMemoyAndDiskDeser:
		return &proto.StorageLevel{UseDisk: true, UseMemory: true, Replication: 1, Deserialized: true}
	case StorageLevelOffHeap:
		return &proto.StorageLevel{UseDisk: true, UseMemory: true, UseOffHeap: true, Replication: 1}
	default:
		return &proto.StorageLevel{UseDisk: false, UseMemory: false, UseOffHeap: false, Replication: 1}
	}
}

func FromProtoStorageLevel(level *proto.StorageLevel) StorageLevel {
	if level.UseDisk && level.UseMemory && level.Replication <= 1 && !level.Deserialized && !level.UseOffHeap {
		return StorageLevelMemoryAndDisk
	} else if level.UseDisk && level.UseMemory && level.Replication == 2 && !level.Deserialized && !level.UseOffHeap {
		return StorageLevelMemoryAndDisk2
	} else if level.UseDisk && !level.UseMemory && level.Replication == 3 &&
		!level.Deserialized && !level.UseOffHeap {
		return StorageLevelDiskOnly3
	} else if level.UseDisk && !level.UseMemory && level.Replication == 2 &&
		!level.Deserialized && !level.UseOffHeap {
		return StorageLevelDiskOnly2
	} else if level.UseDisk && !level.UseMemory && level.Replication <= 1 &&
		!level.Deserialized && !level.UseOffHeap {
		return StorageLevelDiskOnly
	} else if !level.UseDisk && level.UseMemory && level.Replication <= 1 &&
		!level.Deserialized && !level.UseOffHeap {
		return StorageLevelMemoryOnly
	} else if !level.UseDisk && level.UseMemory && level.Replication == 2 &&
		!level.Deserialized && !level.UseOffHeap {
		return StorageLevelMemoryOnly2
	} else if level.UseDisk && level.UseMemory && level.Replication <= 1 && level.Deserialized && !level.UseOffHeap {
		return StorageLevelMemoyAndDiskDeser
	} else if !level.UseDisk && !level.UseMemory && !level.Deserialized && !level.UseOffHeap {
		return StorageLevelNone
	} else if level.UseOffHeap && !level.Deserialized {
		return StorageLevelOffHeap
	}
	return StorageLevelNone
}
