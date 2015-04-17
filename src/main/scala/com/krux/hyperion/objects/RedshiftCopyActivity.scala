package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpRedshiftCopyActivity, AdpRef, AdpJsonSerializer,
  AdpActivity, AdpS3DataNode, AdpRedshiftDataNode, AdpEc2Resource, AdpPrecondition}
import com.krux.hyperion.objects.aws.AdpSnsAlarm

/**
 * Redshift copy activity
 */
case class RedshiftCopyActivity private (
  id: PipelineObjectId,
  input: S3DataNode,
  output: RedshiftDataNode,
  insertMode: RedshiftCopyActivity.InsertMode,
  runsOn: Ec2Resource,
  transformSql: Option[String] = None,
  commandOptions: Seq[RedshiftCopyOption] = Seq(),
  dependsOn: Seq[PipelineActivity] = Seq(),
  preconditions: Seq[Precondition] = Seq(),
  onFailAlarms: Seq[SnsAlarm] = Seq(),
  onSuccessAlarms: Seq[SnsAlarm] = Seq(),
  onLateActionAlarms: Seq[SnsAlarm] = Seq()
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withCopyOptions(opts: RedshiftCopyOption*) = this.copy(commandOptions = opts)

  def withTransformSql(sql: String) = this.copy(transformSql = Some(sql))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] = Seq(input, runsOn, output) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  lazy val serialize = AdpRedshiftCopyActivity(
    id = id,
    name = Some(id),
    input = AdpRef(input.serialize),
    insertMode = insertMode.toString,
    output = AdpRef(output.serialize),
    transformSql = transformSql,
    commandOptions = seqToOption(commandOptions)(_.repr).map(_.flatten),
    queue = None,
    runsOn = AdpRef(runsOn.serialize),
    dependsOn = seqToOption(dependsOn)(act => AdpRef(act.serialize)),
    precondition = seqToOption(preconditions)(precondition => AdpRef(precondition.serialize)),
    onFail = seqToOption(onFailAlarms)(alarm => AdpRef(alarm.serialize)),
    onSuccess = seqToOption(onSuccessAlarms)(alarm => AdpRef(alarm.serialize)),
    onLateAction = seqToOption(onLateActionAlarms)(alarm => AdpRef(alarm.serialize))
  )

}

object RedshiftCopyActivity extends Enumeration with RunnableObject {

  type InsertMode = Value
  val KeepExisting = Value("KEEP_EXISTING")
  val OverwriteExisting = Value("OVERWRITE_EXISTING")
  val Truncate = Value("TRUNCATE")

  def apply(input: S3DataNode, output: RedshiftDataNode, insertMode: InsertMode, runsOn: Ec2Resource) =
    new RedshiftCopyActivity(
      id = PipelineObjectId("RedshiftCopyActivity"),
      input = input,
      output = output,
      insertMode = insertMode,
      runsOn = runsOn,
      transformSql = None,
      commandOptions = Seq(),
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )

}
