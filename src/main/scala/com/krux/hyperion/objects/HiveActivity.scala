package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpHiveActivity, AdpDataNode, AdpRef, AdpEmrCluster, AdpActivity, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

case class HiveActivity private (
  id: PipelineObjectId,
  runsOn: EmrCluster,
  hiveScript: Option[String],
  scriptUri: Option[String],
  scriptVariable: Option[String],
  input: Option[DataNode],
  output: Option[DataNode],
  stage: Option[Boolean],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
)(
  implicit val hc: HyperionContext
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withHiveScript(hiveScript: String) = this.copy(hiveScript = Some(hiveScript))
  def withScriptUri(scriptUri: String) = this.copy(scriptUri = Some(scriptUri))
  def withScriptVariable(scriptVariable: String) = this.copy(scriptVariable = Some(scriptVariable))

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpHiveActivity(
    id = id,
    name = Some(id),
    hiveScript = hiveScript,
    scriptUri = scriptUri,
    scriptVariable = scriptVariable,
    input = input.map(in => AdpRef(in.serialize)).get,
    output = output.map(out => AdpRef(out.serialize)).get,
    stage = stage.toString,
    runsOn = AdpRef(runsOn.serialize),
    dependsOn = seqToOption(dependsOn)(act => AdpRef(act.serialize)),
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef(alarm.serialize))
  )
}

object HiveActivity {
  def apply(runsOn: EmrCluster)(implicit hc: HyperionContext) =
    new HiveActivity(
      id = PipelineObjectId("HiveActivity"),
      runsOn = runsOn,
      hiveScript = None,
      scriptUri = None,
      scriptVariable = None,
      input = None,
      output = None,
      stage = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
