package com.krux.hyperion.objects.aws

/**
 * References to an existing aws data pipeline object
 *
 * more details:
 * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-expressions.html
 */
case class AdpRef[+T <: AdpDataPipelineAbstractObject] (objId: String)

object AdpRef {
  def apply[T <: AdpDataPipelineAbstractObject](obj: T) = new AdpRef[T](obj.id)
}
