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

		// println("ARC.CONVERT: before header dict is: " + dict.toSAMSequenceDictionary.getSequence(adamRecord.getReferenceName))

		val header: SAMFileHeader = createSAMHeader(dict, readGroups, readGroupFromADAM)
		val builder: SAMRecord = new SAMRecord(header)

		builder.setReadName(adamRecord.getReadName.toString) 
		builder.setReadString(adamRecord.getSequence)	
		builder.setCigarString(adamRecord.getCigar) 		//should I be setting the cigar?
		builder.setBaseQualityString(adamRecord.getQual)	

		// val readReference: Int = adamRecord.getReferenceId				
		if (adamRecord.getReferenceId != null) {
			// println("ARC.CONVERT: direct get referenceid is :" + adamRecord.getReferenceId)
			// builder.setReferenceIndex(adamRecord.getReferenceId)	 		//  java.lang.IllegalArgumentException: Reference index 1 not found in sequence dictionary.
			

			//what happens is that the reference index 1 can't be found, which means that the reference
			//index is incorrect.
			// println("ARC.CONVERT: before setting reference name header dict is : " + header.getSequenceDictionary)
			// println("ARC.CONVERT: before setting reference name header read is : "  + header.getSequenceDictionary.getSequence(adamRecord.getReferenceName))
			// println("ARC.CONVERT: before setting reference name builder dict is : " + builder.getHeader().getSequenceDictionary)
			// println("ARC.CONVERT: before setting reference name builder read is : "  + builder.getHeader().getSequenceDictionary.getSequence(adamRecord.getReferenceName))
			//should I put this option stuff in an if statement?
			println("ARC.CONVERT: header sequence index is: " + header.getSequenceIndex(adamRecord.getReferenceName)) //is 9
			println("ARC.CONVERT: header sequence from name is: " + header.getSequence(adamRecord.getReferenceName)) //works
			println("ARC.CONVERT: dict is: " + header.getSequenceDictionary)
			println("ARC.CONVERT: dict sequences are : " + header.getSequenceDictionary.getSequences)
			println("ARC.CONVERT: header sequence from index is: " + header.getSequenceDictionary.getSequence(0))
			println("ARC.CONVERT: header sequence from index is: " + header.getSequence(0)) //header sequence is null
			println("ARC.CONVERT: header sequence name is: "+ header.getSequence(0).getSequenceName) //error because it's null
			// println("ARC.CONVERT: header sequence name is: "+ header.getSequence(1)) //error because it's null
			// println("ARC.CONVERT: header sequence name is: "+ header.getSequence(1).getSequenceName) //error because it's null

			Option(adamRecord.getReferenceName).foreach(v => builder.setReferenceName(v)) //error here

			val name: String = adamRecord.getReferenceName		
			println("ARC.CONVERT: about to set reference name to : " + name)
			println("ARC.CONVERT: about to set reference index to : "  + header.getSequenceDictionary.getSequenceIndex(name)) //The index for the given sequence name, or -1 if the name is not found.
			builder.setReferenceIndex(header.getSequenceDictionary.getSequenceIndex(name))
			builder.setReferenceName(adamRecord.getReferenceName)

			// System.out.println("before getStart")
			if (adamRecord.getStart != null) {
				val start: Int = adamRecord.getStart.toInt		
				// println("start is: " + start)
				if (start!= 0) {
					// println("setting start: " + start)
					builder.setAlignmentStart(start + 1) 	
					// println("got: " + builder.getAlignmentStart)				
				}
			}

			// println("mapq is: " + adamRecord.getMapq)
			Option(adamRecord.getMapq).foreach(v => builder.setMappingQuality(v))
			// println("got: " + builder.getMappingQuality)
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

		// println("I'm right before adamRecord.getAttributes")
		// println("adamRecord.getAttributes is: " + adamRecord.getAttributes)
		if (adamRecord.getMismatchingPositions != null) {		
			// println("about to set MD tag")
			builder.setAttribute("MD", adamRecord.getMismatchingPositions)
		}
		if (adamRecord.getAttributes != null) {				
			val mp = RichADAMRecord(adamRecord).tags		
			// println("Now I'm here")
			mp.foreach(a => { 		
					builder.setAttribute(a.tag, a.value)
			})
		}

		builder
	}

	def createSAMHeader(sd: SequenceDictionary, rgd: RecordGroupDictionary, rgfa: SAMReadGroupRecord): SAMFileHeader = {    
    	val samSequenceDictionary = sd.toSAMSequenceDictionary     //not doing it correct?  
    	// println("length of samSequenceDictionary is: " + samSequenceDictionary.getReferenceLength)
    	// println("ARC.HEADER: sequence of samSequenceDictionary is " + samSequenceDictionary.getSequence("referencetest"))
     //  	println("ARC.HEADER: sd is : " + sd)
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
