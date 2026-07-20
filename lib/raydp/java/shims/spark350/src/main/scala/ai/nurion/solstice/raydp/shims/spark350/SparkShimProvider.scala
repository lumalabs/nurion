/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.nurion.solstice.raydp.shims.spark350

import ai.nurion.solstice.raydp.shims.{Spark350Shims, SparkShimDescriptor, SparkShims}

object SparkShimProvider {
  val DESCRIPTOR: SparkShimDescriptor = SparkShimDescriptor(3, 5, 0)
  // Any 3.5.x patch matches this shim. Keeping the match a prefix check means
  // newly-released patches (e.g. 3.5.8, 3.5.9 ...) work without a shim rebuild.
  val SUPPORTED_PREFIX = "3.5."
}

class SparkShimProvider extends ai.nurion.solstice.raydp.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark350Shims()
  }

  def matches(version: String): Boolean = {
    version.startsWith(SparkShimProvider.SUPPORTED_PREFIX)
  }
}
