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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.junit.jupiter.api.Test;

public class TestS3LocationProvider extends TestBase {
  @Test
  public void testHashInjection() {
    table
        .updateProperties()
        .set(
            TableProperties.WRITE_LOCATION_PROVIDER_IMPL,
            "org.apache.iceberg.aws.s3.S3LocationProvider")
        .commit();
    assertThat(table.locationProvider().newDataLocation("a"))
        .contains("data/001001010110100110110010/a");
    assertThat(table.locationProvider().newDataLocation("b"))
        .contains("data/110111100111111000000011/b");
    assertThat(table.locationProvider().newDataLocation("c"))
        .contains("data/001100101101011001011111/c");
    assertThat(table.locationProvider().newDataLocation("d"))
        .contains("data/000110010001010001110011/d");
  }
}
