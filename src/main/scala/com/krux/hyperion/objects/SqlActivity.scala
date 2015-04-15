package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpSqlActivity, AdpEc2Resource, AdpRef, AdpDatabase,
  AdpActivity, AdpSnsAlarm, AdpPrecondition}

case class SqlActivity private (
  id: PipelineObjectId,
  runsOn: Ec2Resource,
  database: Database,
  script: String,
  scriptArgument: Seq[String],
  queue: Option[String],
  dependsOn: Seq[PipelineActivity],
  preconditions: Seq[Precondition],
  onFailAlarms: Seq[SnsAlarm],
  onSuccessAlarms: Seq[SnsAlarm],
  onLateActionAlarms: Seq[SnsAlarm]
) extends PipelineActivity {

  def named(name: String) = this.copy(id = PipelineObjectId.withName(name, id))

  def groupedBy(group: String) = this.copy(id = PipelineObjectId.withGroup(group, id))

  def withQueue(queue: String) = this.copy(queue = Option(queue))

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def whenMet(preconditions: Precondition*) = this.copy(preconditions = preconditions)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = alarms)

  override def objects: Iterable[PipelineObject] =
    Seq(runsOn, database) ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  def serialize = AdpSqlActivity(
    id = id,
    name = Some(id),
    database = AdpRef[AdpDatabase](database.id),
    script = script,
    scriptArgument = scriptArgument,
    queue = queue,
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = toOption(dependsOn)(a => AdpRef[AdpActivity](a.id)),
    precondition = toOption(preconditions)(precondition => AdpRef[AdpPrecondition](precondition.id)),
    onFail = toOption(onFailAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onSuccess = toOption(onSuccessAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id)),
    onLateAction = toOption(onLateActionAlarms)(alarm => AdpRef[AdpSnsAlarm](alarm.id))
  )
}

object SqlActivity {
  def apply(runsOn: Ec2Resource, database: Database, script: String) =
    new SqlActivity(
      id = PipelineObjectId("SqlActivity"),
      runsOn = runsOn,
      database = database,
      script = script,
      scriptArgument = Seq(),
      queue = None,
      dependsOn = Seq(),
      preconditions = Seq(),
      onFailAlarms = Seq(),
      onSuccessAlarms = Seq(),
      onLateActionAlarms = Seq()
    )
}
