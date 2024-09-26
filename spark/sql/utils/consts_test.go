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

import "testing"

func TestStorageLevelConversion(t *testing.T) {
	// Given a list of all storage levels, convert them to and from proto and
	// check with the original value:
	for _, level := range []StorageLevel{
		StorageLevelDiskOnly,
		StorageLevelDiskOnly2,
		StorageLevelDiskOnly3,
		StorageLevelMemoryAndDisk,
		StorageLevelMemoryAndDisk2,
		StorageLevelMemoryOnly,
		StorageLevelMemoryOnly2,
		StorageLevelMemoyAndDiskDeser,
		StorageLevelNone,
		StorageLevelOffHeap,
	} {
		protoLevel := ToProtoStorageLevel(level)
		convertedLevel := FromProtoStorageLevel(protoLevel)
		if level != convertedLevel {
			t.Errorf("Expected %v, got %v", level, convertedLevel)
		}
	}
}
