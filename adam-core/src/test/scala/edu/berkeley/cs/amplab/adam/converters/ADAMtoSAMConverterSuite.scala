/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.converters

import org.broadinstitute.variant.variantcontext.{Allele, VariantContextBuilder, GenotypeBuilder}
import edu.berkeley.cs.amplab.adam.avro.VariantType
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._  

import org.scalatest.FunSuite

class ADAMtoSAMConverterSuite extends FunSuite {

  test("Basic test comparing two sam files to see if correct fields outputted") {
    val convertedRead = RDD(SAMRecord)
    val origFile = sc.newAPIHadoopFile(path, classOf(SAMInputFormat), classOf(
      Writable), classOf(SAMRecordWritable))
    convertedRead.zip(origFile) <- RDD((SAMRead, SAMRecord))
  }


}
