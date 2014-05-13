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



  sparkTest("testing the fields in a converted ADAM Read with larger SequenceDictionary") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      
    adamRead.setReferenceName("referencetest")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"),
      SequenceRecord(1, "referencetest1", 4, "test://chrom2"))
    val readGroups = new RecordGroupDictionary(Seq("testing"))
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    val sequence = "A" * 4
    assert(toSAM.getReadName == ("read" + 0.toString))
    assert(toSAM.getAlignmentStart == 4) 
    assert(toSAM.getReadUnmappedFlag == true)
    assert(toSAM.getCigarString == "2M3D2M")     
    assert(toSAM.getReadString == sequence)
    assert(toSAM.getReadNegativeStrandFlag == false)
    assert(toSAM.getMappingQuality == 60)
    assert(toSAM.getBaseQualityString == sequence)
    assert(toSAM.getReferenceIndex == 0) 
    assert(toSAM.getAttribute("MD") == "2^AAA2")
  }

  sparkTest("creating simple adam read converting it back and forth with seqdict of 1") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      
    adamRead.setReferenceName("referencetest")
    adamRead.setReferenceLength(5)
    adamRead.setReferenceUrl("test://chrom1")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"))
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
    assert(adamRead.getReferenceId == backToADAM.getReferenceId) 
    assert(adamRead.getReferenceName == backToADAM.getReferenceName)
    assert(adamRead.getReferenceLength == backToADAM.getReferenceLength)
    assert(adamRead.getReferenceUrl == backToADAM.getReferenceUrl)
    assert(adamRead.getStart == backToADAM.getStart)      
    assert(adamRead.getMapq == backToADAM.getMapq)        
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
    assert(adamRead.getMismatchingPositions == backToADAM.getMismatchingPositions)
  }

    sparkTest("creating simple adam read converting it back and forth with seqdict of 2") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(1)      
    adamRead.setReferenceName("referencetest1")
    adamRead.setReferenceLength(4)
    adamRead.setReferenceUrl("test://chrom2")
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
    assert(adamRead.getReferenceId == backToADAM.getReferenceId) 
    assert(adamRead.getReferenceName == backToADAM.getReferenceName)
    assert(adamRead.getReferenceLength == backToADAM.getReferenceLength)
    assert(adamRead.getReferenceUrl == backToADAM.getReferenceUrl)
    assert(adamRead.getStart == backToADAM.getStart)      
    assert(adamRead.getMapq == backToADAM.getMapq)        
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
    assert(adamRead.getMismatchingPositions == backToADAM.getMismatchingPositions)
  }

    sparkTest("creating simple adam read converting it back and forth with seqdict of 3") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    //the four fields below must match the fields in the specified SequenceRecord
    adamRead.setReferenceId(2)      
    adamRead.setReferenceName("referencetest2")
    adamRead.setReferenceLength(7)
    adamRead.setReferenceUrl("test://chrom3")
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    val dict = SequenceDictionary(SequenceRecord(0, "referencetest", 5, "test://chrom1"),
      SequenceRecord(1, "referencetest1", 4, "test://chrom2"), 
      SequenceRecord(2, "referencetest2", 7, "test://chrom3"))
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
    assert(adamRead.getReferenceId == backToADAM.getReferenceId) 
    assert(adamRead.getReferenceName == backToADAM.getReferenceName)
    assert(adamRead.getReferenceLength == backToADAM.getReferenceLength)
    assert(adamRead.getReferenceUrl == backToADAM.getReferenceUrl)
    assert(adamRead.getStart == backToADAM.getStart)      
    assert(adamRead.getMapq == backToADAM.getMapq)        
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
    assert(adamRead.getMismatchingPositions == backToADAM.getMismatchingPositions)
  }
    sparkTest("testing the fields in a converted ADAM Read") {
    val adamRead = make_read(3L, "2M3D2M", "2^AAA2", 4)
    adamRead.setRecordGroupName("testname")
    adamRead.setReferenceId(0)      
    adamRead.setReferenceName("referencetest")
    //mate stuff
    adamRead.setMateReferenceId(0)
    adamRead.setMateReference("matereferencetest")
    adamRead.setMateAlignmentStart(6L)
    val fakeMd5 = "1b22b98cdeb4a9304cb5d48026a85128"
    val mateRefId = 0
    val mateRefName = "matereferencetest" 
    val mateRefLength = 6L
    val mateRefUrl = "test://chrom1"
    val mateRefSeqRecord = SequenceRecord(mateRefId, mateRefName, mateRefLength, mateRefUrl, fakeMd5)

    // val mateRefSeqRecord = new SequenceRecord(mateRefId, mateRefName, mateRefLength, mateRefUrl)
    println("the tested mateRefSeqRecord is : " + mateRefSeqRecord)
    val seqRecForDict = SequenceRecord(0, "referencetest", 5, "test://chrom1")   //works if I don't do new? 
    println("seqRecForDict : " + seqRecForDict)
    //mate stuff
    val adamRecordConverter = new ADAMRecordConverter
    val samRecordConverter = new SAMRecordConverter
    // val seqRecForDict = SequenceRecord(0, "referencetest", 5, "test://chrom1")   //works if I don't do new? 
    // pritnln("seqRecForDict : " + seqRecForDict)
    val dict = SequenceDictionary(seqRecForDict)
    val readGroups = new RecordGroupDictionary(Seq("testing"))
    val toSAM = adamRecordConverter.convert(adamRead, dict, readGroups)
    val sequence = "A" * 4
    assert(toSAM.getReadName == ("read" + 0.toString))
    assert(toSAM.getAlignmentStart == 4) 
    assert(toSAM.getReadUnmappedFlag == true)
    assert(toSAM.getCigarString == "2M3D2M")     
    assert(toSAM.getReadString == sequence)
    assert(toSAM.getReadNegativeStrandFlag == false)
    assert(toSAM.getMappingQuality == 60)
    assert(toSAM.getBaseQualityString == sequence)
    assert(toSAM.getReferenceIndex == 0) 
    assert(toSAM.getAttribute("MD") == "2^AAA2")
    println("Mate Reference Index is : " + toSAM.getMateReferenceIndex)
    println("Mate Reference Name is : " + toSAM.getMateReferenceName)
    println("Mate Alignment Start is : " + toSAM.getMateAlignmentStart)
    assert(toSAM.getMateReferenceIndex == 0)
    assert(toSAM.getMateReferenceName == "matereferencetest")
    assert(toSAM.getMateAlignmentStart == 7)
  }

}



