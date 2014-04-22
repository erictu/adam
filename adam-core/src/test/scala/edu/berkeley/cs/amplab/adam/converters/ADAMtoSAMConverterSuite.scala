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
// import edu.berkeley.cs.amplab.adam.avro.VariantType
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
      .setReadMapped(false)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setQual(sequence) // no typo, we just don't care
      .setMismatchingPositions(mdtag)
      .build()
  }

  sparkTest("testing the fields in a converted ADAM Read") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      //does this need to match with the seq record 9?
    adamRead.setReferenceName("referencetest")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"))
    // println(dict)
    val readGroups = new RecordGroupDictionary(Seq("testing"))
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    val sequence = "A" * 4
    assert(toSAM.getReadName == ("read" + 0.toString))
    assert(toSAM.getAlignmentStart == 4) //requires referenceId to be set, equiv to getStart
    assert(toSAM.getReadUnmappedFlag == true)
    assert(toSAM.getCigarString == "2M3D2M")     
    assert(toSAM.getReadString == sequence)
    assert(toSAM.getReadNegativeStrandFlag == false)
    assert(toSAM.getMappingQuality == 60)
    assert(toSAM.getBaseQualityString == sequence)
    assert(toSAM.getReferenceIndex == 0) //why must this be zero?
    System.out.println("adamRead mdtag is: " + adamRead.getMismatchingPositions)
    System.out.println("mdtag is:" + toSAM.getAttribute("MD"))
    assert(toSAM.getAttribute("MD") == "2^AAA2")

  }

  sparkTest("testing the fields in a converted ADAM Read with larger SequenceDictionary") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      //does this need to match with the seq record 9?
    adamRead.setReferenceName("referencetest")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"),
      SequenceRecord(1, "referencetest1", 4, "test://chrom2"))
    // println(dict)
    val readGroups = new RecordGroupDictionary(Seq("testing"))
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    val sequence = "A" * 4
    assert(toSAM.getReadName == ("read" + 0.toString))
    assert(toSAM.getAlignmentStart == 4) //requires referenceId to be set, equiv to getStart
    assert(toSAM.getReadUnmappedFlag == true)
    assert(toSAM.getCigarString == "2M3D2M")     
    assert(toSAM.getReadString == sequence)
    assert(toSAM.getReadNegativeStrandFlag == false)
    assert(toSAM.getMappingQuality == 60)
    assert(toSAM.getBaseQualityString == sequence)
    println("getReferenceIndex is: " + toSAM.getReferenceIndex);
    // assert(toSAM.getReferenceIndex == 0) //why must this be zero?
    System.out.println("adamRead mdtag is: " + adamRead.getMismatchingPositions)
    System.out.println("mdtag is:" + toSAM.getAttribute("MD"))
    assert(toSAM.getAttribute("MD") == "2^AAA2")

  }

  sparkTest("creating simple adam read converting it back and forth") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      //what's the point in this?
    adamRead.setReferenceName("referencetest")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"))
    // println(dict)
    val readGroups = new RecordGroupDictionary(Seq("testing"))
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    val backToADAM = samRecordConverter.convert(toSAM, dict, readGroups)
    assert(adamRead.getRecordGroupSequencingCenter == backToADAM.getRecordGroupSequencingCenter)
    assert(adamRead.getRecordGroupRunDateEpoch == backToADAM.getRecordGroupRunDateEpoch)
    assert(adamRead.getRecordGroupDescription == backToADAM.getRecordGroupDescription)
    assert(adamRead.getRecordGroupFlowOrder == backToADAM.getRecordGroupFlowOrder)
    assert(adamRead.getRecordGroupKeySequence == backToADAM.getRecordGroupKeySequence)
    assert(adamRead.getRecordGroupLibrary == backToADAM.getRecordGroupLibrary)
    assert(adamRead.getRecordGroupPredictedMedianInsertSize == backToADAM.getRecordGroupPredictedMedianInsertSize)
    assert(adamRead.getRecordGroupPlatform == backToADAM.getRecordGroupPlatform)
    assert(adamRead.getRecordGroupPlatformUnit == backToADAM.getRecordGroupPlatformUnit)
    assert(adamRead.getRecordGroupSample == backToADAM.getRecordGroupSample)
    assert(adamRead.getReadName == backToADAM.getReadName)
    assert(adamRead.getCigar == backToADAM.getCigar)
    assert(adamRead.getSequence == backToADAM.getSequence)
    assert(adamRead.getQual == backToADAM.getQual)
    println("toSAM reference index is: " + toSAM.getReferenceIndex)
    assert(adamRead.getReferenceId == backToADAM.getReferenceId) //referenceid/seqdict issue
    assert(adamRead.getReferenceName == backToADAM.getReferenceName)
    println("initial start is: " + adamRead.getStart)
    println("intermediate start is: " + toSAM.getAlignmentStart)
    println("end start is: " + backToADAM.getStart)
    assert(adamRead.getStart == backToADAM.getStart)      //also has to do with seqdict issue

    println("initial mapq is : " + adamRead.getMapq)
    println("intermediate mapq is: " + toSAM.getMappingQuality)
    println("end mapq is : " + backToADAM.getMapq)
    assert(adamRead.getMapq == backToADAM.getMapq)        //also has to do with seqdict issue

    assert(adamRead.getMateReferenceId == backToADAM.getMateReferenceId)
    assert(adamRead.getMateReference == backToADAM.getMateReference)
    assert(adamRead.getReadPaired == backToADAM.getReadPaired)
    assert(adamRead.getMateNegativeStrand == backToADAM.getMateNegativeStrand)
    assert(adamRead.getMateMapped == backToADAM.getMateMapped)
    assert(adamRead.getProperPair == backToADAM.getProperPair)
    assert(adamRead.getFirstOfPair == backToADAM.getFirstOfPair)
    assert(adamRead.getSecondOfPair == backToADAM.getSecondOfPair)
    assert(adamRead.getDuplicateRead == backToADAM.getDuplicateRead)
    assert(adamRead.getReadNegativeStrand == backToADAM.getReadNegativeStrand)
    assert(adamRead.getPrimaryAlignment == backToADAM.getPrimaryAlignment)
    assert(adamRead.getFailedVendorQualityChecks == backToADAM.getFailedVendorQualityChecks)
    assert(adamRead.getReadMapped == backToADAM.getReadMapped)

    // System.out.println("initial mdtag is: " + adamRead.getMismatchingPositions)
    // System.out.println("intermediate mdtag is:" + toSAM.getAttribute("MD"))
    // System.out.println("end mdtag is: " + backToADAM.getMismatchingPositions)
    assert(adamRead.getMismatchingPositions == backToADAM.getMismatchingPositions)
    // println("adamRead attributes are: " + adamRead.getAttributes)   //null
    // println("backToADAM attributes are: " + backToADAM.getAttributes) //nothing
    // assert(adamRead.getAttributes == backToADAM.getAttributes) //null equiv to nothing? Should be ok



 
  }
    sparkTest("creating simple adam read converting it back and forth with larger seqdict") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      //what's the point in this?
    adamRead.setReferenceName("referencetest")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"),
      SequenceRecord(1, "referencetest1", 4, "test://chrom2"))
    val readGroups = new RecordGroupDictionary(Seq("testing"))
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    val backToADAM = samRecordConverter.convert(toSAM, dict, readGroups)
    assert(adamRead.getRecordGroupSequencingCenter == backToADAM.getRecordGroupSequencingCenter)
    assert(adamRead.getRecordGroupRunDateEpoch == backToADAM.getRecordGroupRunDateEpoch)
    assert(adamRead.getRecordGroupDescription == backToADAM.getRecordGroupDescription)
    assert(adamRead.getRecordGroupFlowOrder == backToADAM.getRecordGroupFlowOrder)
    assert(adamRead.getRecordGroupKeySequence == backToADAM.getRecordGroupKeySequence)
    assert(adamRead.getRecordGroupLibrary == backToADAM.getRecordGroupLibrary)
    assert(adamRead.getRecordGroupPredictedMedianInsertSize == backToADAM.getRecordGroupPredictedMedianInsertSize)
    assert(adamRead.getRecordGroupPlatform == backToADAM.getRecordGroupPlatform)
    assert(adamRead.getRecordGroupPlatformUnit == backToADAM.getRecordGroupPlatformUnit)
    assert(adamRead.getRecordGroupSample == backToADAM.getRecordGroupSample)
    assert(adamRead.getReadName == backToADAM.getReadName)
    assert(adamRead.getCigar == backToADAM.getCigar)
    assert(adamRead.getSequence == backToADAM.getSequence)
    assert(adamRead.getQual == backToADAM.getQual)
    println("toSAM reference index is: " + toSAM.getReferenceIndex)
    assert(adamRead.getReferenceId == backToADAM.getReferenceId) //referenceid/seqdict issue
    assert(adamRead.getReferenceName == backToADAM.getReferenceName)
    println("initial start is: " + adamRead.getStart)
    println("intermediate start is: " + toSAM.getAlignmentStart)
    println("end start is: " + backToADAM.getStart)
    assert(adamRead.getStart == backToADAM.getStart)      //also has to do with seqdict issue

    println("initial mapq is : " + adamRead.getMapq)
    println("intermediate mapq is: " + toSAM.getMappingQuality)
    println("end mapq is : " + backToADAM.getMapq)
    assert(adamRead.getMapq == backToADAM.getMapq)        //also has to do with seqdict issue

    assert(adamRead.getMateReferenceId == backToADAM.getMateReferenceId)
    assert(adamRead.getMateReference == backToADAM.getMateReference)
    assert(adamRead.getReadPaired == backToADAM.getReadPaired)
    assert(adamRead.getMateNegativeStrand == backToADAM.getMateNegativeStrand)
    assert(adamRead.getMateMapped == backToADAM.getMateMapped)
    assert(adamRead.getProperPair == backToADAM.getProperPair)
    assert(adamRead.getFirstOfPair == backToADAM.getFirstOfPair)
    assert(adamRead.getSecondOfPair == backToADAM.getSecondOfPair)
    assert(adamRead.getDuplicateRead == backToADAM.getDuplicateRead)
    assert(adamRead.getReadNegativeStrand == backToADAM.getReadNegativeStrand)
    assert(adamRead.getPrimaryAlignment == backToADAM.getPrimaryAlignment)
    assert(adamRead.getFailedVendorQualityChecks == backToADAM.getFailedVendorQualityChecks)
    assert(adamRead.getReadMapped == backToADAM.getReadMapped)

    // System.out.println("initial mdtag is: " + adamRead.getMismatchingPositions)
    // System.out.println("intermediate mdtag is:" + toSAM.getAttribute("MD"))
    // System.out.println("end mdtag is: " + backToADAM.getMismatchingPositions)
    assert(adamRead.getMismatchingPositions == backToADAM.getMismatchingPositions)
    // println("adamRead attributes are: " + adamRead.getAttributes)   //null
    // println("backToADAM attributes are: " + backToADAM.getAttributes) //nothing
    // assert(adamRead.getAttributes == backToADAM.getAttributes) //null equiv to nothing? Should be ok



 
  }

}



