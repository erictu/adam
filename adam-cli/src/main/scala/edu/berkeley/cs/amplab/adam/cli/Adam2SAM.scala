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

package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.avro.ADAMGenotype
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.util.ADAMSAMOutputFormat
import net.sf.samtools.{SAMReadGroupRecord, SAMRecord}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import org.kohsuke.args4j.Argument
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.converters.ADAMRecordConverter

object Adam2SAM extends AdamCommandCompanion {

  val commandName = "adam2sam"
  val commandDescription = "Convert an ADAM variant to the SAM/BAM format"

  def apply(cmdLine: Array[String]) = {
    new Adam2SAM(Args4j[Adam2SAMArgs](cmdLine))
  }
}

class Adam2SAMArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM variant files to convert", index = 0)
  var adamFile: String = _
  @Argument(required = true, metaVar = "SAM", usage = "Location to write SAM data", index = 1)
  var outputPath: String = null
}

class Adam2SAM(val args: Adam2SAMArgs) extends AdamSparkCommand[Adam2SAMArgs] with Logging {
  val companion = Adam2SAM // point of this? 

// def convert(adamRecord: ADAMRecord, dict: SequenceDictionary, readGroups: RecordGroupDictionary): SAMRecord = { 

  def run(sc: SparkContext, job: Job) {
    val adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.adamFile)
    sc.adamSAMSave(args.outputPath, adamRecords)  
  }

}
