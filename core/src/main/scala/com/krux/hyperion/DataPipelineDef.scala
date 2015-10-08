package com.krux.hyperion

import scala.language.implicitConversions

import com.amazonaws.services.datapipeline.model.{ParameterObject => AwsParameterObject}
import com.amazonaws.services.datapipeline.model.{PipelineObject => AwsPipelineObject}
import org.json4s.JsonDSL._
import org.json4s.{JValue, JArray}

import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.aws.{AdpDataPipelineAbstractObject, AdpParameterSerializer, AdpPipelineSerializer, AdpJsonSerializer}
import com.krux.hyperion.common.{S3UriHelper, S3Uri, DefaultObject, PipelineObject}
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.workflow.WorkflowExpressionImplicits

/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef extends HyperionCli with S3UriHelper with WorkflowExpressionImplicits {

  private lazy val context = new HyperionContext()

  implicit def hc: HyperionContext = context

  def schedule: Schedule

  def workflow: WorkflowExpression

  def defaultObject = DefaultObject(schedule)

  def tags: Map[String, Option[String]] = Map.empty

  def parameters: Iterable[Parameter[_]] = Seq.empty

  def objects: Iterable[PipelineObject] = workflow
    .toPipelineObjects
    .foldLeft(Map.empty[String, PipelineObject])(flattenPipelineObjects)
    .values

  private def flattenPipelineObjects(r: Map[String, PipelineObject], po: PipelineObject): Map[String, PipelineObject] =
    if (!r.contains(po.id.toString)) {
      r ++ Map(po.id.toString -> po) ++ po.objects.foldLeft(r)(flattenPipelineObjects)
    } else {
      r
    }

  def pipelineName = MainClass(this).toString

  def withNewParameterValues(moreParams: Iterable[(String, String)]): DataPipelineDef = {
    val paramMap = parameters.map(p => p.id -> p).toMap[String, Parameter[_]]

    moreParams.foldLeft(paramMap) { case (m, (pid, pvalue)) =>
      val previousParam = paramMap(pid)
      m + ((pid, previousParam.withValue(pvalue)))
    }
    ???
  }

  /**
   * Overwrites the defined parameters in this pipeline.
   */
  def overwriteParameters(moreParams: Iterable[Parameter[_]]): DataPipelineDef = {
    val bldr = Map.newBuilder[String, Parameter[_]] ++=
      parameters.map(p => p.id -> p) ++=
      moreParams.map(p => p.id -> p)

    MinimalDataPipelineDef(
      this.pipelineName,
      this.schedule,
      this.workflow,
      bldr.result.values
    )
  }

}

object DataPipelineDef {

  implicit def dataPipelineDef2Json(pd: DataPipelineDef): JValue =
    ("objects" -> JArray(
      AdpJsonSerializer(pd.defaultObject.serialize) ::
      AdpJsonSerializer(pd.schedule.serialize) ::
      pd.objects.map(_.serialize).toList.sortBy(_.id).map(o => AdpJsonSerializer(o)))) ~
    ("parameters" -> JArray(
      pd.parameters.flatMap(_.serialize).map(o => AdpJsonSerializer(o)).toList))

  implicit def dataPipelineDef2Aws(pd: DataPipelineDef): Seq[AwsPipelineObject] =
    AdpPipelineSerializer(pd.defaultObject.serialize) ::
    AdpPipelineSerializer(pd.schedule.serialize) ::
    pd.objects.map(o => AdpPipelineSerializer(o.serialize)).toList

  implicit def dataPipelineDef2AwsParameter(pd: DataPipelineDef): Seq[AwsParameterObject] =
    pd.parameters.flatMap(_.serialize).map(o => AdpParameterSerializer(o)).toList

}
