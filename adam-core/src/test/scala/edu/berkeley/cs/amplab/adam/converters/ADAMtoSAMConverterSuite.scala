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
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.AdamRDDFunctions
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}
import net.sf.samtools.SAMFileHeader

class ADAMtoSAMConverterSuite extends SparkFunSuite {
  /*
  * start with SAM file, convert to adam with SAMRecordConverter
  * then convert back to SAM with ADAMRecordConverter
  * Then Compare
  */
  test("Basic test comparing two sam files to see if correct fields outputted") {
    val startingSAM = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val filePath = ClassLoader.getSystemClassLoader.getResource("small.sam")

    //trying to get original Sam parts

    //ERROR:
    // overloaded method constructor Path with alternatives:
    // (x$1: java.net.URI)org.apache.hadoop.fs.Path <and>
    // (x$1: String)org.apache.hadoop.fs.Path
    // cannot be applied to (java.net.URL)
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration) 
    val seqDict = adamBamDictionaryLoad(samHeader)     
    val readGroups =  adamBamLoadReadGroups(samHeader)

    val convertedToADAM = sc.adamLoad(startingSAM)    

    //ERROR: Class[T] does not take parameters
    val origFile = sc.newAPIHadoopFile(filePath.toString, classOf(SAMInputFormat), classOf(    //reads them in from disk before you convert them
      Writable), classOf(SAMRecordWritable))
    val ADAMRecordConverter = new ADAMRecordConverter
    val backToSAM = origFile.map(r => ADAMRecordConverter.convert(adamRecord, seqdict, readGroups))   //in adamBamLoad
    // val backToSAM = ADAMRecordConverter.convert(convertedToSAM, seqDict, readGroups)     //or this

    //assertion statements
    val backToSAMHeader = backToSAM.getHeader
    assert(samHeader == backToSAMHeader)   

    //ERROR:
    // overloaded method value assert with alternatives:
    // (o: Option[String])Unit <and>
    // (o: Option[String],clue: Any)Unit <and>
    // (condition: Boolean,clue: Any)Unit <and>
    // (condition: Boolean)Unit
    // cannot be applied to (readGroups: edu.berkeley.cs.amplab.adam.models.RecordGroupDictionary)
    assert(seqDict == adamBamDictionaryLoad(backToSAMHeader))
    assert(readGroups = adamBamLoadReadGroups(backToSAMHeader))


  }

  def adamBamDictionaryLoad(samHeader : SAMFileHeader): SequenceDictionary = {
    SequenceDictionary.fromSAMHeader(samHeader)

  }

  def adamBamLoadReadGroups(samHeader : SAMFileHeader) : RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }
  
  //adam-core/src/test/scala/edu/berkeley/cs/amplab/adam/algorithms/realignmenttarget/IndelRealignmentTargetSuite.scala
  def make_read(start : Long, cigar : String, mdtag : String, length : Int, id : Int = 0) : ADAMRecord = {
    val sequence : String = "A" * length
    ADAMRecord.newBuilder()
      .setReadName("read" + id.toString)
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setQual(sequence) // no typo, we just don't care
      .setMismatchingPositions(mdtag)
      .build()
  }
}




