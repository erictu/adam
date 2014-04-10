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
import edu.berkeley.cs.amplab.adam.models.{RecordGroupDictionary, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._ 	//added this in
import net.sf.samtools.SAMFileHeader
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.models.Attribute 	//added this in
import java.util.Date 

class ADAMRecordConverter extends Serializable {
	def convert(adamRecord: ADAMRecord, dict: SequenceDictionary, readGroups: RecordGroupDictionary): SAMRecord = {	
		assert(adamRecord.getRecordGroupName != null, "can't get record group name if not set")
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

		val header: SAMFileHeader = createSAMHeader(dict, readGroups, readGroupFromADAM)
		val builder: SAMRecord = new SAMRecord(header)

		builder.setReadName(adamRecord.getReadName.toString) 
		builder.setReadString(adamRecord.getSequence)	
		builder.setCigarString(adamRecord.getCigar) 		//should I be setting the cigar?
		builder.setBaseQualityString(adamRecord.getQual)	

		// val readReference: Int = adamRecord.getReferenceId				
		if (adamRecord.getReferenceId != null) {
			builder.setReferenceIndex(adamRecord.getReferenceId)	 		//reference index must be found in seqdict
			Option(adamRecord.getReferenceName).foreach(v => builder.setReferenceName(v))
			// builder.setReferenceName(adamRecord.getReferenceName)

			if (adamRecord.getStart != null) {
				val start: Int = adamRecord.getStart.toInt		
				if (start!= 0) {
					builder.setAlignmentStart(start + 1) 					
				}
			}

			Option(adamRecord.getMapq).foreach(v => builder.setMappingQuality(v))
		}

		// val mateReference: Int = adamRecord.getMateReferenceId		
		if (adamRecord.getMateReferenceId != null) {
			builder.setMateReferenceIndex(adamRecord.getMateReferenceId)
			builder.setMateReferenceName(adamRecord.getMateReference)		

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


		if (adamRecord.getAttributes != null) {				
			val mp = RichADAMRecord(adamRecord).tags		
			mp.foreach(a => { 		
				if (adamRecord.getMismatchingPositions != null) {		
					builder.setAttribute("MD", adamRecord.getMismatchingPositions)
				}
				builder.setAttribute(a.tag, a.value) 
			})
		}

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
  		samHeader.addReadGroup(rgfa)		//adds in SAMReadGroupRecord from ADAM RecordGroup

  		samHeader
    }
}