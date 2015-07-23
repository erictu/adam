/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.io

import htsjdk.tribble.index.Block
import htsjdk.tribble.index.tabix.TabixIndex
import org.seqdoop.hadoop_bam.VCFInputFormat
import org.seqdoop.hadoop_bam.FileVirtualSplit
import org.seqdoop.hadoop_bam.VariantContextWritable
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import java.io.File
import scala.collection.mutable

object IndexedVcfInputFormat {

  var optFilePath: Option[Path] = None
  var optIndexFilePath: Option[Path] = None
  var optViewRegion: Option[ReferenceRegion] = None

  def apply(filePath: Path, indexFilePath: Path, viewRegion: ReferenceRegion) {
    optFilePath = Some(filePath)
    optIndexFilePath = Some(indexFilePath)
    optViewRegion = Some(viewRegion)
  }

}

class IndexedVcfInputFormat extends VCFInputFormat {

  override def createRecordReader(split: InputSplit, ctx: TaskAttemptContext): RecordReader[LongWritable, VariantContextWritable] = {
    val rr: RecordReader[LongWritable, VariantContextWritable] = new VCFFilteredRecordReader()
    assert(VCFFilteredRecordReader.optViewRegion.isDefined)
    VCFFilteredRecordReader(IndexedVcfInputFormat.optViewRegion.get)
    rr.initialize(split, ctx)
    rr
  }

  override def getSplits(job: JobContext): java.util.List[InputSplit] = {
    assert(IndexedVcfInputFormat.optIndexFilePath.isDefined &&
      IndexedVcfInputFormat.optFilePath.isDefined &&
      IndexedVcfInputFormat.optViewRegion.isDefined)
    val indexFilePath = IndexedVcfInputFormat.optIndexFilePath.get

    val idxFile: File = new File(indexFilePath.toString)
    if (!idxFile.exists()) {
      super.getSplits(job)
    } else {
      val filePath = IndexedVcfInputFormat.optFilePath.get
      val viewRegion = IndexedVcfInputFormat.optViewRegion.get
      val refName = viewRegion.referenceName
      val start = viewRegion.start.toInt
      val end = viewRegion.end.toInt
      val tabix: TabixIndex = new TabixIndex(idxFile)
      var regions: List[Block] = tabix.getBlocks(refName, start, end)

      var splits = new mutable.ListBuffer[FileVirtualSplit]()
      for (block <- regions) {
        val start: Long = block.getStartPosition()
        val end: Long = block.getEndPosition
        val locs = Array[String]()
        val newSplit = new FileVirtualSplit(filePath, start, end, locs)
        splits += newSplit
      }
      splits.toList
    }
  }

}
