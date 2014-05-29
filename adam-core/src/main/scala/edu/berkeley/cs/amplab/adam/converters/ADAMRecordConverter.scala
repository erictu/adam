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

import net.sf.samtools.{SAMReadGroupRecord, SAMRecord}

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary, SequenceRecord}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._ 	
import net.sf.samtools.SAMFileHeader
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.models.Attribute 	
import java.util.Date 

class ADAMRecordConverter extends Serializable {
	def convert(adamRecord: ADAMRecord, dict: SequenceDictionary, readGroups: RecordGroupDictionary): SAMRecord = {	
		assert(adamRecord.getRecordGroupName != null, "can't get record group name if not set")
		println("-----------------START INSIDE ADAMRECORDCONVERTER-----------------")
		val readGroupFromADAM: SAMReadGroupRecord = new SAMReadGroupRecord(adamRecord.getRecordGroupName)
		Option(adamRecord.getRecordGroupSequencingCenter).foreach(v => readGroupFromADAM.setSequencingCenter(v.toString)) 	
		Option(adamRecord.getRecordGroupRunDateEpoch).foreach(v => readGroupFromADAM.setRunDate(new Date(v)))
		Option(adamRecord.getRecordGroupDescription).foreach(v => readGroupFromADAM.setDescription(v))
		Option(adamRecord.getRecordGroupFlowOrder).foreach(v => readGroupFromADAM.setFlowOrder(v))
		Option(adamRecord.getRecordGroupKeySequence).foreach(v => readGroupFromADAM.setKeySequence(v))
		Option(adamRecord.getRecordGroupLibrary).foreach(v => readGroupFromADAM.setLibrary(v))
		Option(adamRecord.getRecordGroupPredictedMedianInsertSize).foreach(v => readGroupFromADAM.setPredictedMedianInsertSize(v))
		Option(adamRecord.getRecordGroupPlatform).foreach(v => readGroupFromADAM.setPlatform(v))
		Option(adamRecord.getRecordGroupPlatformUnit).foreach(v => readGroupFromADAM.setPlatformUnit(v))
		Option(adamRecord.getRecordGroupSample).foreach(v => readGroupFromADAM.setSample(v))

		val mateRefName = adamRecord.getMateReference
		val mateRefId = if(adamRecord.getMateReferenceId == 0)
			0
		else
			adamRecord.getMateReferenceId-1
		val mateRefLength = adamRecord.getMateReferenceLength

		val mateRefUrl = adamRecord.getMateReferenceUrl


		println("mateRefName is: " + mateRefName)
		println("mateRefId is: " + mateRefId)
		println("mateRefLength is: " +mateRefLength)
		println("mateRefUrl is: " + mateRefUrl)
		//fake md5 for now
		val mateRefSeqRecord = new SequenceRecord(mateRefId, mateRefName, mateRefLength, mateRefUrl, "1b22b98cdeb4a9304cb5d48026a85128") //class v object?

		println("materefseqrecord is: " + mateRefSeqRecord)
		println("adding into the following dict: " + dict)
		val contained = dict.recordsIn.contains(mateRefSeqRecord)
		println("already contained is: " + contained)
		// val refDict = if (!dict.recordsIn.contains(mateRefSeqRecord)) 
		// 	dict
			
		// else 
		// 	dict+(mateRefSeqRecord)

		val refDict = dict+(mateRefSeqRecord)
		// val refDict = dict+(mateRefSeqRecord)
		println("after adding into dict: " + refDict)
		val header: SAMFileHeader = createSAMHeader(refDict, readGroups, readGroupFromADAM)

		val builder: SAMRecord = new SAMRecord(header)

		builder.setReadName(adamRecord.getReadName.toString) 
		builder.setReadString(adamRecord.getSequence)	
		builder.setCigarString(adamRecord.getCigar) 		
		builder.setBaseQualityString(adamRecord.getQual)	
				
		if (adamRecord.getReferenceId != null) {
			//this also sets the index when it's setting the name
			Option(adamRecord.getReferenceName).foreach(v => builder.setReferenceName(v)) 

			if (adamRecord.getStart != null) {
				val start: Int = adamRecord.getStart.toInt		
				if (start!= 0) {
					builder.setAlignmentStart(start + 1) 			
				}
			}
			Option(adamRecord.getMapq).foreach(v => builder.setMappingQuality(v))
		}
	
		if (adamRecord.getMateReferenceId != null) {
			println("sequence is (by id-1) : " + header.getSequence(adamRecord.getMateReferenceId-1))
			println("sequence is (by id): " + header.getSequence(adamRecord.getMateReferenceId))
			println("sequence is (-1) : " + header.getSequence(-1))

			println("sequence is (by id): " + header.getSequence(adamRecord.getMateReferenceId))
			println("sequence is (by name): " + header.getSequence("matereferencetest"))

			println("header seq index is: " + header.getSequenceIndex("matereferencetest"))
			println("header dict is: " + header.getSequenceDictionary.getSequences)
			builder.setMateReferenceName(adamRecord.getMateReference)		//id supposedly set here

			val mateStart: Int = adamRecord.getMateAlignmentStart.toInt		
			if (mateStart > 0) {
				builder.setMateAlignmentStart(mateStart + 1)			
			}

		}
				
		if (adamRecord.getReadPaired) {
			builder.setReadPairedFlag(true)										
			if (adamRecord.getMateNegativeStrand) {
				builder.setMateNegativeStrandFlag(true) 
			}
			if (!adamRecord.getMateMapped) {
				builder.setMateUnmappedFlag(true)				
			}
			if (adamRecord.getProperPair) {
				builder.setProperPairFlag(true)	
			}
			if (adamRecord.getFirstOfPair) {
				builder.setFirstOfPairFlag(true)		
			}
			if (adamRecord.getSecondOfPair) {
				builder.setSecondOfPairFlag(true)	
			}
		}
		if (adamRecord.getDuplicateRead) {
			builder.setDuplicateReadFlag(true)
		}
		if (adamRecord.getReadNegativeStrand) {
			builder.setReadNegativeStrandFlag(true)
		}
		if (!adamRecord.getPrimaryAlignment) {
			builder.setNotPrimaryAlignmentFlag(true)		
		}
		if (adamRecord.getFailedVendorQualityChecks) {
			builder.setReadFailsVendorQualityCheckFlag(true)
		}
		if (!adamRecord.getReadMapped) {
			builder.setReadUnmappedFlag(true)				
		}
		if (adamRecord.getMismatchingPositions != null) {		
			builder.setAttribute("MD", adamRecord.getMismatchingPositions)
		}
		if (adamRecord.getAttributes != null) {				
			val mp = RichADAMRecord(adamRecord).tags		
			mp.foreach(a => { 		
					builder.setAttribute(a.tag, a.value)
			})
		}
		println("-----------------END INSIDE ADAMRECORDCONVERTER-----------------")
		builder
	}

	def createSAMHeader(sd: SequenceDictionary, rgd: RecordGroupDictionary, rgfa: SAMReadGroupRecord): SAMFileHeader = {    
    	val samSequenceDictionary = sd.toSAMSequenceDictionary  
      	val samHeader = new SAMFileHeader
      	samHeader.setSequenceDictionary(samSequenceDictionary)         
    	rgd.readGroups.foreach(kv=> {     
      		val(_, name) = kv
      		val nextMember = new SAMReadGroupRecord(name.toString)
      		if (!nextMember.equivalent(rgfa)) {
      			samHeader.addReadGroup(nextMember) 
      		}
  		})
  		samHeader.addReadGroup(rgfa)		

  		samHeader
    }
}
