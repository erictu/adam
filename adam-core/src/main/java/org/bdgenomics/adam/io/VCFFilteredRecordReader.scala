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

import org.apache.hadoop.fs.{ FileSystem, Path, FSDataInputStream }

import org.apache.hadoop.io.LongWritable

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.seqdoop.hadoop_bam.VariantContextWritable
import org.seqdoop.hadoop_bam.VCFRecordReader
import htsjdk.tribble.FeatureCodecHeader
import htsjdk.variant.vcf.VCFCodec
import htsjdk.tribble.readers.AsciiLineReader
import htsjdk.tribble.readers.AsciiLineReaderIterator
import htsjdk.variant.vcf.VCFHeader
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._

import hbparquet.hadoop.util.ContextUtil

import java.util.Map
import java.util.HashMap
import java.io.IOException

object VCFFilteredRecordReader {
  var optViewRegion: Option[ReferenceRegion] = None

  def apply(viewRegion: ReferenceRegion) {
    optViewRegion = Some(viewRegion)
  }
}

class VCFFilteredRecordReader extends VCFRecordReader {
  val key: LongWritable = new LongWritable()
  val vc: VariantContextWritable = new VariantContextWritable()

  var codec: VCFCodec = new VCFCodec()
  var it: AsciiLineReaderIterator = _
  var reader: AsciiLineReader = _
  var header: VCFHeader = _
  var length: Long = _
  var contigDict: java.util.Map[String, Integer] = new HashMap[String, Integer]()

  override def initialize(spl: InputSplit, ctx: TaskAttemptContext) = {
    val split: FileSplit = spl.asInstanceOf[FileSplit]
    this.length = split.getLength()

    val file: Path = split.getPath()
    val fs: FileSystem = file.getFileSystem(ContextUtil.getConfiguration(ctx))
    val ins: FSDataInputStream = fs.open(file)
    reader = new AsciiLineReader(ins)
    it = new AsciiLineReaderIterator(reader)

    val h = codec.readHeader(it)
    h match {
      case f: FeatureCodecHeader => throw new IOException("No VCF header found in " + file)

    }
    val v = h.asInstanceOf[FeatureCodecHeader].getHeaderValue()
    v match {
      case a: VCFHeader => throw new IOException("No VCF header found in " + file)
    }
    header = (h.asInstanceOf[FeatureCodecHeader]).getHeaderValue().asInstanceOf[VCFHeader]

    contigDict.clear()
    var i: Int = 0
    for (contig <- header.getContigLines()) {
      i = i + 1
      contigDict.put(contig.getID(), i)
    }
    val start: Long = split.getStart()
    if (start != 0) {
      ins.seek(start - 1)
      reader = new AsciiLineReader(ins)
      reader.readLine()
      it = new AsciiLineReaderIterator(reader)
    } else {
      val current_pos: Long = it.getPosition()
      ins.seek(0)
      reader = new AsciiLineReader(ins)
      it = new AsciiLineReaderIterator(reader)
      while (it.hasNext() && it.getPosition() <= current_pos && it.peek().startsWith("#")) {
        it.next()
      }
      if (!it.hasNext() || it.getPosition() > current_pos) {
        throw new IOException("Empty VCF file " + file)
      }
    }

  }

  override def close() {
  	reader.close()
  }

  override def getProgress(): 

}

