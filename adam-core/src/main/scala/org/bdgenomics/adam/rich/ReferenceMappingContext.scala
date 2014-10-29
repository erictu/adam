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
package org.bdgenomics.adam.rich

import org.bdgenomics.adam.models.{ ReferenceRegion, ReferenceMapping }
import org.bdgenomics.formats.avro.{ AlignmentRecord, FlatGenotype, Genotype }

/**
 * A common location in which to drop some ReferenceMapping implementations.
 */
object ReferenceMappingContext {

  implicit object FlatGenotypeReferenceMapping extends ReferenceMapping[FlatGenotype] with Serializable {
    override def getReferenceName(value: FlatGenotype): String = value.getReferenceName.toString
    override def getReferenceRegion(value: FlatGenotype): ReferenceRegion =
      ReferenceRegion(value.getReferenceName.toString, value.getPosition, value.getPosition)
  }

  //ERIC:
  implicit object GenotypeReferenceMapping extends ReferenceMapping[Genotype] with Serializable {
    override def getReferenceName(value: Genotype): String = value.getSampleId.toString
    override def getReferenceRegion(value: Genotype): ReferenceRegion =
      ReferenceRegion(value.getSampleId.toString, value.getVariant.getStart, value.getVariant.getEnd)
  }

  implicit object AlignmentRecordReferenceMapping extends ReferenceMapping[AlignmentRecord] with Serializable {
    def getReferenceName(value: AlignmentRecord): String =
      value.getContig.getContigName.toString

    def getReferenceRegion(value: AlignmentRecord): ReferenceRegion =
      ReferenceRegion(value).orNull
  }

  implicit object ReferenceRegionReferenceMapping extends ReferenceMapping[ReferenceRegion] with Serializable {
    def getReferenceName(value: ReferenceRegion): String =
      value.referenceName.toString

    def getReferenceRegion(value: ReferenceRegion): ReferenceRegion = value
  }
}
