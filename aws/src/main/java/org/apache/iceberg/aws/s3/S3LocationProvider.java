/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.s3;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.relocated.com.google.common.hash.HashCode;
import org.apache.iceberg.relocated.com.google.common.hash.HashFunction;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;

/**
 * This custom location provider for S3 provides a hashing scheme that's optimized for S3
 * autoscaling. The current implementation uses a 24-bit base2 (aka binary) scheme.
 */
public class S3LocationProvider extends LocationProviders.ObjectStoreLocationProvider {
  private static final HashFunction HASH_FUNC = Hashing.murmur3_32_fixed();

  public S3LocationProvider(String tableLocation, Map<String, String> properties) {
    super(tableLocation, properties);
  }

  @Override
  protected String computeHash(String fileName) {
    HashCode hashCode = HASH_FUNC.hashString(fileName, StandardCharsets.UTF_8);
    int hash = hashCode.asInt();

    // {@link Integer#toBinaryString} excludes leading zeros, which we want to preserve.
    // force the first bit to be set to get around that.
    String hashAsBinaryString = Integer.toBinaryString(hash | Integer.MIN_VALUE);

    // just the lower 24 bits
    return hashAsBinaryString.substring(8);
  }
}
