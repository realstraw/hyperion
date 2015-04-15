package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrActivity, AdpJsonSerializer, AdpRef, AdpEmrCluster,
  AdpActivity, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Defines a spark activity
 */
case class SparkActivity private (
  id: PipelineObjectId,
  runsOn: SparkCluster,
  steps: Seq[SparkStep],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends EmrActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withStepSeq(steps: Seq[SparkStep]) = this.copy(steps = steps)
  def withSteps(steps: SparkStep*) = this.copy(steps = steps)

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] =
    (runsOn +: dependsOn) ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpEmrActivity(
    id = id,
    name = Some(id),
    input = None,
    output = None,
    preStepCommand = None,
    postStepCommand = None,
    actionOnResourceFailure = None,
    actionOnTaskFailure = None,
    step = steps.map(_.toStepString),
    runsOn = AdpRef[AdpEmrCluster](runsOn.id),
    dependsOn = toOption(dependsOn)(d => AdpRef[AdpActivity](d.id)),
    precondition = toOption(preconditions)(precondition => AdpRef[AdpPrecondition](precondition.id)),
    onFail = toOption(onFailAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onSuccess = toOption(onSuccessAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onLateAction = toOption(onLateActionAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id))
  )
}

object SparkActivity extends RunnableObject {
  def apply(runsOn: SparkCluster) =
    new SparkActivity(
      id = PipelineObjectId("SparkActivity"),
      runsOn = runsOn,
      steps = Seq(),
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
