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
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary, SequenceRecord}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.AdamRDDFunctions
import fi.tkk.ics.hadoop.bam.util.SAMHeaderReader
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import java.io.File
import org.apache.hadoop.fs.{FileSystem, Path}
import net.sf.samtools.SAMFileHeader
import fi.tkk.ics.hadoop.bam.{SAMRecordWritable, AnySAMInputFormat}
import org.apache.hadoop.io.LongWritable
import parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.converters.{SAMRecordConverter, ADAMRecordConverter}

class ADAMtoSAMConverterSuite extends SparkFunSuite {
  /*
  * start with SAM file, convert to adam with SAMRecordConverter
  * then convert back to SAM with ADAMRecordConverter
  * Then Compare
  */
  // sparkTest("Basic test comparing two sam files to see if correct fields outputted") {
  //   val startingSAM = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
  //   val filePath = ClassLoader.getSystemClassLoader.getResource("small.sam")

  //   //trying to get original Sam parts

  //   //ERROR:
  //   // overloaded method constructor Path with alternatives:
  //   // (x$1: java.net.URI)org.apache.hadoop.fs.Path <and>
  //   // (x$1: String)org.apache.hadoop.fs.Path
  //   // cannot be applied to (java.net.URL)
  //   val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(filePath), sc.hadoopConfiguration) 
  //   val seqDict = adamBamDictionaryLoad(samHeader)     
  //   val readGroups =  adamBamLoadReadGroups(samHeader)

  //   val adamRecord: RDD[ADAMRecord] = sc.adamLoad(startingSAM)    //have to specify the type

  //   //this is giving me an RDD of ADAMRecords, I need an ADAMRecord
  //   val origFile = sc.newAPIHadoopFile(filePath.toString, classOf[AnySAMInputFormat], classOf[LongWritable],
  //     classOf[SAMRecordWritable])
  //   val ADAMRecordConverter = new ADAMRecordConverter
  //   //this gets a samfile with r._2.get?
  //   val backToSAM = origFile.map(r => ADAMRecordConverter.convert(r._2.get, seqdict, readGroups))   //in adamBamLoad

  //   //assertion statements
  //   val backToSAMHeader = backToSAM.getHeader
  //   assert(samHeader == backToSAMHeader)   

  
  //   assert(seqDict == adamBamDictionaryLoad(backToSAMHeader))
  //   assert(readGroups == adamBamLoadReadGroups(backToSAMHeader))


  // }

  // def adamBamDictionaryLoad(samHeader : SAMFileHeader): SequenceDictionary = {
  //   SequenceDictionary.fromSAMHeader(samHeader)

  // }

  // def adamBamLoadReadGroups(samHeader : SAMFileHeader) : RecordGroupDictionary = {
  //   RecordGroupDictionary.fromSAMHeader(samHeader)
  // }

  // //adam-core/src/test/scala/edu/berkeley/cs/amplab/adam/algorithms/realignmenttarget/IndelRealignmentTargetSuite.scala
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

  //converts back and forth. any other way to do this?
  // sparkTest("creating simple adam read converting it back and forth") {
  //   val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
  //   val adamRecordConverter = new ADAMRecordConverter
  //   val samRecordConverter = new SAMRecordConverter
  //   val dict = SequenceDictionary(SequenceRecord(1, "1", 5, "test://chrom1"))
  //   val readGroups = new RecordGroupDictionary(Seq("testing"))

  //   //look into what the RecordGroupDictionary has to do with the record itself, why do I have 
  //   val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
  //   val backToADAM = samRecordConverter.convert(toSAM, dict, readGroups)
  //   assert(adamRead.getReadName == backToADAM.getReadName)
  //   assert(adamRead.getSequence == backToADAM.getSequence)
  //   assert(adamRead.getQual == backToADAM.getQual)
  //   assert(adamRead.getStart == backToADAM.getStart)
  //   assert(adamRead.getMateReferenceId == backToADAM.getMateReferenceId)
  //   assert(adamRead.getReferenceId == backToADAM.getReferenceId)    
  // }

  sparkTest("creating simple adam read converting it back and forth") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("test")
    adamRead.setReferenceId(0)
    // adamRead.setRecordGroupSequencingCenter("rgsc")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(1, "1", 5, "test://chrom1"))
    val readGroups = new RecordGroupDictionary(Seq("testing"))

    //look into what the RecordGroupDictionary has to do with the record itself, why do I have 
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    assert(toSAM.getCigarString == "2M3D2M")      
  }


  // - creating simple adam read *** FAILED ***
  // java.lang.NullPointerException:
  // at edu.berkeley.cs.amplab.adam.rdd.AdamContext$.charSequenceToString(AdamContext.scala:88)
  // at edu.berkeley.cs.amplab.adam.converters.ADAMRecordConverter.convert(ADAMRecordConverter.scala:31)
  // at edu.berkeley.cs.amplab.adam.converters.ADAMtoSAMConverterSuite$$anonfun$1.apply$mcV$sp(ADAMtoSAMConverterSuite.scala:110)
  // at edu.berkeley.cs.amplab.adam.util.SparkFunSuite$$anonfun$sparkTest$1.apply$mcV$sp(SparkFunSuite.scala:102)
  // at edu.berkeley.cs.amplab.adam.util.SparkFunSuite$$anonfun$sparkTest$1.apply(SparkFunSuite.scala:98)
  // at edu.berkeley.cs.amplab.adam.util.SparkFunSuite$$anonfun$sparkTest$1.apply(SparkFunSuite.scala:98)
  // at org.scalatest.FunSuiteLike$$anon$1.apply(FunSuiteLike.scala:129)
  // at org.scalatest.Suite$class.withFixture(Suite.scala:1974)
  // at org.scalatest.FunSuite.withFixture(FunSuite.scala:1182)
  // at org.scalatest.FunSuiteLike$class.invokeWithFixture$1(FunSuiteLike.scala:126)
  // ...


}



